"""
validator_analysis.py
=====================
Subnet Validator Analysis Tool — Intelligence Sovereignty Research Suite
@im_perseverance

Analyses all active validators on a given subnet and ranks them by
estimated nominator APY. Designed to support staking decisions after
subnet_staking_snapshot.py has identified a target subnet.

Net supply delta is sourced from subnet_analysis/trajectory_all.json,
which is maintained by subnet_analysis.py running daily. 7d price APY
and emission share trend are derived from the validator tool's own
per-subnet trajectory.

Usage:
    python validator_analysis.py --netuid 64 --stake 100

Arguments:
    --netuid    Subnet ID to analyse (required)
    --stake     Your intended stake in TAO (required)

Outputs:
    - Console report ranked by combined APY (30d basis)
    - validator_analysis/SN{netuid}/snapshot_YYYY-MM-DD.csv
    - validator_analysis/SN{netuid}/snapshot_YYYY-MM-DD.json
    - validator_analysis/SN{netuid}/trajectory.json  (7d price APY + emission share trend)

Notes on methodology:
    - Emission APY: your projected TAO per day, annualised as % of stake.
      Based on your share of the validator pool * validator emission share
      * (1 - take). Reflects actual position size, not a pool-level proxy.
    - Price APY (30d): annualised momentum from spot vs protocol EMA.
    - Price APY (7d): annualised return from spot vs 7d-ago snapshot price.
      Populates from the second run onwards on the same subnet.
    - Real APY: emission APY minus net supply delta sourced from
      subnet_analysis/trajectory_all.json (maintained by subnet_analysis.py).
    - Combined APY: emission APY + price APY (30d basis).
    - Concentration: top-1 and top-3 nominator share of the validator pool.
      HIGH = top-1 > 50%. MODERATE = top-1 > 25%. DISTRIBUTED otherwise.
    - Weight staleness: blocks since the validator last set weights. A stale
      validator (24h+) is not actively evaluating miners.
    - Consensus: how much other validators agree with this validator's weights.
    - Self-stake: whether the validator owner has skin in the game on this subnet.
    - Div/Inc ratio: validator dividends vs mining incentive for this UID.
      High = pure validator. Low = validator-miner (mining and validating).
    - Emission share trend: change in a validator's emission share across runs.
      Leading indicator of decay before trust or staleness catch it. Populates
      from the second run onwards on the same subnet.
"""

import argparse
import bittensor as bt
import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────
OUTPUT_DIR        = Path("validator_analysis")
SUBNET_TRAJ_ALL   = Path("subnet_analysis") / "trajectory_all.json"
BLOCKS_PER_DAY    = 7200
BLOCKS_PER_YEAR   = BLOCKS_PER_DAY * 365
MIN_TV            = 0.5
EMA_LAG_THRESHOLD = -0.15
SEPARATOR         = "=" * 160
THIN_SEP          = "-" * 160

# ── Helpers ────────────────────────────────────────────────────────────────

def safe_float(val, default=0.0):
    try:
        return float(val)
    except Exception:
        return default

def fmt_pct(val, decimals=2, signed=True):
    if val is None:
        return "  N/A  "
    return f"{val*100:+.{decimals}f}%" if signed else f"{val*100:.{decimals}f}%"

def fmt_apy(val, decimals=2):
    if val is None:
        return "   N/A   "
    return f"{val*100:+.{decimals}f}%"

def load_json(path):
    p = Path(path)
    if p.exists():
        try:
            with open(p) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_json(path, data):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)

# ── Per-subnet trajectory ──────────────────────────────────────────────────

def load_trajectory(netuid):
    """Load longitudinal trajectory for a specific subnet."""
    path = OUTPUT_DIR / f"SN{netuid}" / "trajectory.json"
    data = load_json(path)
    return data if isinstance(data, list) else []

def save_trajectory(netuid, trajectory):
    """Save longitudinal trajectory for a specific subnet."""
    path = OUTPUT_DIR / f"SN{netuid}" / "trajectory.json"
    save_json(path, trajectory)

def get_7d_price_apy(current_price, trajectory, date_str):
    """
    7d price APY from this subnet's own trajectory.

    Finds the closest snapshot within 3 days of the 7-day-ago target and
    annualises the return using the actual period length.
    """
    if len(trajectory) < 2:
        return None
    try:
        curr_dt   = datetime.strptime(date_str, "%Y-%m-%d")
        target_dt = curr_dt - timedelta(days=7)
    except Exception:
        return None
    best      = None
    best_diff = float("inf")
    for entry in trajectory:
        try:
            entry_dt = datetime.strptime(entry["date"], "%Y-%m-%d")
            diff = abs((entry_dt - target_dt).days)
            if diff < best_diff:
                best_diff = diff
                best = entry
        except Exception:
            continue
    if best is None or best_diff > 3:
        return None
    old_price = best.get("spot_price")
    if not old_price or old_price <= 0 or current_price <= 0:
        return None
    period_days   = max((curr_dt - datetime.strptime(best["date"], "%Y-%m-%d")).days, 1)
    period_return = (current_price - old_price) / old_price
    return period_return * (365 / period_days)

def get_net_supply_delta(netuid, date_str):
    """
    Net supply delta sourced from subnet_analysis/trajectory_all.json.

    Reads the most recent entry for this subnet that predates today's snapshot.
    Uses the pre-computed net_supply_delta value written by subnet_analysis.py,
    which captures actual alpha supply change after buybacks, burns, and
    unstaking outflows — identical methodology to the ecosystem tool.

    Returns None if trajectory_all.json is unavailable or has no prior entry
    for this subnet.
    """
    traj_all = load_json(SUBNET_TRAJ_ALL)
    history  = traj_all.get(str(netuid), [])
    if not history:
        return None
    # Find the most recent entry that is not today (avoid reading today's
    # in-progress snapshot if subnet_analysis.py has already run today)
    for entry in reversed(history):
        if entry.get("date") != date_str:
            return entry.get("net_supply_delta")
    return None

def get_prev_e_shares(trajectory, date_str):
    """
    Retrieve the validator emission shares dict from the most recent
    previous trajectory entry. Returns {} if unavailable.
    """
    if not trajectory:
        return {}
    prev = trajectory[-1]
    if prev.get("date") == date_str:
        if len(trajectory) < 2:
            return {}
        prev = trajectory[-2]
    return prev.get("validator_e_shares", {})

# ── Delegate analysis (concentration, self-stake, take) ────────────────────

def analyse_delegate(sub, hotkey, netuid):
    """
    Single RPC call per validator via get_delegate_by_hotkey. Extracts:
      - take (float, 0-1)
      - top-1 and top-3 nominator concentration
      - concentration flag (HIGH / MODERATE / DISTRIBUTED)
      - nominator count for this subnet
      - self-stake percentage (owner's stake as fraction of total)

    Returns a dict with all fields, or defaults with None values if
    delegate info cannot be retrieved.
    """
    defaults = {
        "take": None, "top1_pct": None, "top3_pct": None,
        "conc_flag": "NO DATA", "nominator_count": None,
        "self_stake_pct": None,
    }
    try:
        delegate = sub.get_delegate_by_hotkey(hotkey)
        if not delegate:
            return defaults

        # Take directly from delegate object — saves a separate RPC call
        take = safe_float(getattr(delegate, "take", None), default=0.0)
        take = max(0.0, min(1.0, take))

        # Owner coldkey for self-stake detection
        owner_ss58 = getattr(delegate, "owner_ss58", None)

        # Nominator analysis for this specific subnet
        stakes = []
        owner_stake = 0.0
        for coldkey, subnet_stakes in delegate.nominators.items():
            alpha = safe_float(subnet_stakes.get(netuid, 0))
            if alpha > 0:
                stakes.append((coldkey, alpha))
                if coldkey == owner_ss58:
                    owner_stake = alpha

        if not stakes:
            return {**defaults, "take": take}

        nominator_count = len(stakes)
        amounts = sorted([s[1] for s in stakes], reverse=True)
        total   = sum(amounts)

        top1_pct = amounts[0] / total if total > 0 else None
        top3_pct = sum(amounts[:3]) / total if total > 0 else None
        self_stake_pct = owner_stake / total if total > 0 else 0.0

        if top1_pct is not None and top1_pct > 0.50:
            conc_flag = "HIGH"
        elif top1_pct is not None and top1_pct > 0.25:
            conc_flag = "MODERATE"
        else:
            conc_flag = "DISTRIBUTED"

        return {
            "take": take,
            "top1_pct": top1_pct,
            "top3_pct": top3_pct,
            "conc_flag": conc_flag,
            "nominator_count": nominator_count,
            "self_stake_pct": self_stake_pct,
        }
    except Exception:
        return defaults

# ── Main ───────────────────────────────────────────────────────────────────

def run_analysis(netuid: int, my_stake: float):
    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str   = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    subnet_dir = OUTPUT_DIR / f"SN{netuid}"
    subnet_dir.mkdir(parents=True, exist_ok=True)

    # Load this subnet's own trajectory
    trajectory = load_trajectory(netuid)
    print(f"  Trajectory: {len(trajectory)} previous run(s) for SN{netuid}")

    print(SEPARATOR)
    print("  VALIDATOR ANALYSIS — Intelligence Sovereignty Research Suite")
    print("  @im_perseverance")
    print(SEPARATOR)
    print(f"\n  Connecting to Bittensor network...")

    sub           = bt.Subtensor(network="finney")
    current_block = sub.get_current_block()

    print(f"  Block     : {current_block:,}")
    print(f"  Timestamp : {ts_str}")
    print(f"  Target    : SN{netuid}")
    print(f"  Stake     : {my_stake:,.2f} TAO\n")

    # ── Subnet pool state ─────────────────────────────────────────────────
    all_subnets = sub.all_subnets()
    subnet = next((s for s in all_subnets if s.netuid == netuid), None)
    if not subnet:
        print(f"  ❌ SN{netuid} not found")
        return

    tao_per_block = safe_float(subnet.tao_in_emission)
    spot_price    = safe_float(subnet.price)
    moving_price  = safe_float(subnet.moving_price)
    tao_reserves  = safe_float(subnet.tao_in)
    alpha_out     = safe_float(subnet.alpha_out)
    volume        = safe_float(subnet.subnet_volume)
    name          = getattr(subnet, "subnet_name", f"SN{netuid}") or f"SN{netuid}"

    momentum_30d  = (spot_price - moving_price) / moving_price if moving_price > 0 else None
    price_apy_30d = momentum_30d * (365 / 30) if momentum_30d is not None else None

    # EMA band — check lag trap first (narrower threshold), then discount
    ema_lag_flag = momentum_30d is not None and momentum_30d < EMA_LAG_THRESHOLD
    if momentum_30d is None:        ema_band = "N/A"
    elif momentum_30d >  0.20:      ema_band = "PREMIUM"
    elif momentum_30d < -0.20:      ema_band = "DISCOUNT"
    elif ema_lag_flag:              ema_band = "⚠️ LAG TRAP"
    else:                           ema_band = "IN BAND"

    # Derive from trajectories
    price_apy_7d     = get_7d_price_apy(spot_price, trajectory, date_str)
    net_supply_delta = get_net_supply_delta(netuid, date_str)
    prev_e_shares    = get_prev_e_shares(trajectory, date_str)

    print(f"  {THIN_SEP}")
    print(f"  SN{netuid} — {name}")
    print(f"  {THIN_SEP}")
    print(f"  Spot price       : {spot_price:.6f} TAO")
    print(f"  Moving price     : {moving_price:.6f} TAO")
    print(f"  EMA momentum     : {fmt_pct(momentum_30d)}  [{ema_band}]")
    print(f"  TAO / block      : {tao_per_block:.6f}")
    print(f"  TAO reserves     : {tao_reserves:,.2f}")
    print(f"  Alpha supply     : {alpha_out:,.2f}")
    print(f"  Volume (24h)     : {volume:,.2f}")
    print(f"  Net supply delta : {fmt_apy(net_supply_delta)}  {'(from subnet_analysis)' if net_supply_delta is not None else '(N/A — subnet_analysis trajectory not found)'}")
    print(f"  Price APY (30d)  : {fmt_apy(price_apy_30d)}")
    print(f"  Price APY (7d)   : {fmt_apy(price_apy_7d)}  {'(from trajectory)' if price_apy_7d is not None else '(N/A — needs 7+ days of runs)'}")
    print()

    # ── Metagraph ─────────────────────────────────────────────────────────
    print(f"  Loading SN{netuid} metagraph...")
    meta   = sub.metagraph(netuid=netuid)
    n_uids = len(meta.uids)

    # ── Validator filtering ───────────────────────────────────────────────
    total_e_validators = 0.0
    candidates = []
    for uid in range(n_uids):
        if not meta.validator_permit[uid]:
            continue
        tv    = safe_float(meta.validator_trust[uid])
        div   = safe_float(meta.dividends[uid])
        stake = safe_float(meta.stake[uid])
        e_val = safe_float(meta.incentive[uid])
        cons  = safe_float(meta.consensus[uid])
        last  = int(meta.last_update[uid])

        if tv < MIN_TV or div <= 0 or stake <= 0:
            continue

        staleness_blocks = current_block - last
        staleness_hours  = staleness_blocks / (BLOCKS_PER_DAY / 24)

        total_e_validators += e_val
        candidates.append({
            "uid":              uid,
            "hotkey":           meta.hotkeys[uid],
            "stake":            stake,
            "div":              div,
            "e_val":            e_val,
            "tv":               tv,
            "consensus":        cons,
            "staleness_blocks": staleness_blocks,
            "staleness_hours":  staleness_hours,
        })

    if not candidates:
        print(f"  ❌ No qualifying validators on SN{netuid} (TV >= {MIN_TV}, dividend > 0)")
        return

    if total_e_validators <= 0:
        total_e_validators = 1.0

    print(f"  Qualifying validators : {len(candidates)}")
    print(f"  Fetching delegate info (take, concentration, self-stake)...\n")

    results = []
    # Track all e_shares for trajectory storage
    current_e_shares = {}

    for c in candidates:
        uid    = c["uid"]
        hotkey = c["hotkey"]
        stake  = c["stake"]
        div    = c["div"]
        e_val  = c["e_val"]
        tv     = c["tv"]

        # Single RPC call — extracts take, concentration, self-stake
        delegate_info = analyse_delegate(sub, hotkey, netuid)
        take = delegate_info["take"]

        # Emission share
        e_share = e_val / total_e_validators
        current_e_shares[str(uid)] = e_share

        # Emission share trend (delta from previous run)
        prev_share = prev_e_shares.get(str(uid))
        e_share_delta = (e_share - prev_share) if prev_share is not None else None

        # Dividend-to-incentive ratio
        # High = pure validator (earns from staking, not mining)
        # Low  = validator-miner (earns from both)
        div_inc_ratio = div / e_val if e_val > 0 else None

        # Projected yield
        validator_tao_block = tao_per_block * e_share
        pool_total          = stake + my_stake
        your_share          = my_stake / pool_total if pool_total > 0 else 0

        if take is not None:
            your_tao_per_day = validator_tao_block * (1 - take) * your_share * BLOCKS_PER_DAY
        else:
            your_tao_per_day = validator_tao_block * your_share * BLOCKS_PER_DAY

        emission_apy = (your_tao_per_day * 365 / my_stake) if my_stake > 0 else 0

        combined_apy_30d = emission_apy + price_apy_30d if price_apy_30d is not None else None
        combined_apy_7d  = emission_apy + price_apy_7d  if price_apy_7d  is not None else None
        real_apy         = emission_apy - net_supply_delta if net_supply_delta is not None else None

        # Combined real APY = real yield + price momentum (the canonical ranking metric)
        # Falls back to gross combined when real_apy is unavailable (first run)
        combined_real_apy_30d = real_apy + price_apy_30d if real_apy is not None and price_apy_30d is not None else None
        combined_real_apy_7d  = real_apy + price_apy_7d  if real_apy is not None and price_apy_7d  is not None else None

        # Staleness flag
        if c["staleness_hours"] > 48:
            stale_flag = "🔴 STALE"
        elif c["staleness_hours"] > 24:
            stale_flag = "🟡 AGING"
        else:
            stale_flag = "🟢 FRESH"

        results.append({
            "uid":              uid,
            "hotkey":           hotkey,
            "hotkey_short":     hotkey[:8] + "...",
            "stake":            stake,
            "div":              div,
            "e_share":          e_share,
            "e_share_delta":    e_share_delta,
            "div_inc_ratio":    div_inc_ratio,
            "take":             take,
            "take_known":       take is not None,
            "tv":               tv,
            "consensus":        c["consensus"],
            "staleness_blocks": c["staleness_blocks"],
            "staleness_hours":  c["staleness_hours"],
            "stale_flag":       stale_flag,
            "your_tao_per_day": your_tao_per_day,
            "emission_apy":     emission_apy,
            "price_apy_30d":    price_apy_30d,
            "price_apy_7d":     price_apy_7d,
            "combined_apy_30d": combined_apy_30d,
            "combined_apy_7d":  combined_apy_7d,
            "combined_real_apy_30d": combined_real_apy_30d,
            "combined_real_apy_7d":  combined_real_apy_7d,
            "real_apy":         real_apy,
            "net_supply_delta": net_supply_delta,
            "top1_pct":         delegate_info["top1_pct"],
            "top3_pct":         delegate_info["top3_pct"],
            "conc_flag":        delegate_info["conc_flag"],
            "nominator_count":  delegate_info["nominator_count"],
            "self_stake_pct":   delegate_info["self_stake_pct"],
        })

    # Cascading sort: rank by real combined APY when available (dilution-adjusted),
    # fall back to gross combined APY on first run when net_supply_delta is null.
    has_real = any(r["combined_real_apy_30d"] is not None for r in results)
    if has_real:
        sort_basis = "Combined Real APY (30d) — dilution-adjusted"
        results.sort(
            key=lambda r: r["combined_real_apy_30d"] if r["combined_real_apy_30d"] is not None else -999,
            reverse=True
        )
    else:
        sort_basis = "Combined Gross APY (30d) — real APY unavailable, subnet_analysis trajectory not found"
        results.sort(
            key=lambda r: r["combined_apy_30d"] if r["combined_apy_30d"] is not None else -999,
            reverse=True
        )

    # ── Console report ─────────────────────────────────────────────────────
    # Console: first-tier signals only. Nominator count → CSV/JSON only.
    print(SEPARATOR)
    print(f"  VALIDATOR RANKINGS — SN{netuid} {name}  |  Stake: {my_stake:,.2f} TAO")
    print(f"  Sorted by: {sort_basis}")
    print(THIN_SEP)
    print(f"  {'UID':<6} {'Stake':>14} {'E Share':>9} {'Δ Share':>8} {'Take':>7} {'TV':>7} {'Cons':>7} "
          f"{'D/I':>6} {'Emiss APY':>11} {'Real APY':>10} {'RealCmb30d':>11} {'GrossCmb30d':>12} {'TAO/day':>10} "
          f"{'Top1':>7} {'Top3':>7} {'Conc':>12} {'Self%':>7} "
          f"{'Weights':>12}  {'Hotkey'}")
    print(THIN_SEP)

    conc_labels = {
        "HIGH":        "🔴 HIGH",
        "MODERATE":    "🟡 MOD",
        "DISTRIBUTED": "🟢 DISTR",
        "NO DATA":     "  N/A",
    }

    for r in results:
        conc_sym  = conc_labels.get(r["conc_flag"], r["conc_flag"])
        top1_str  = f"{r['top1_pct']:.1%}" if r["top1_pct"] is not None else "N/A"
        top3_str  = f"{r['top3_pct']:.1%}" if r["top3_pct"] is not None else "N/A"
        take_str  = f"{r['take']:>6.1%}" if r["take"] is not None else "    ?%"
        self_str  = f"{r['self_stake_pct']:.1%}" if r["self_stake_pct"] is not None else "  N/A"
        stale_str = f"{r['staleness_hours']:.1f}h {r['stale_flag']}"
        di_str    = f"{r['div_inc_ratio']:>6.2f}" if r["div_inc_ratio"] is not None else "   N/A"
        if r["e_share_delta"] is not None:
            delta_str = f"{r['e_share_delta']:>+8.4f}"
        else:
            delta_str = "     N/A"
        if not r["take_known"]:
            take_str = f"{take_str}*"
        print(
            f"  {r['uid']:<6} {r['stake']:>14,.0f} {r['e_share']:>9.4f} {delta_str} "
            f"{take_str:>7} {r['tv']:>7.4f} {r['consensus']:>7.4f} "
            f"{di_str} "
            f"{fmt_apy(r['emission_apy']):>11} "
            f"{fmt_apy(r['real_apy']):>10} "
            f"{fmt_apy(r['combined_real_apy_30d']):>11} "
            f"{fmt_apy(r['combined_apy_30d']):>12} "
            f"{r['your_tao_per_day']:>10.6f} "
            f"{top1_str:>7} {top3_str:>7} "
            f"{conc_sym:<14}{self_str:>7} "
            f"{stale_str:>14}  {r['hotkey']}"
        )

    print(SEPARATOR)

    # ── Staleness warnings ─────────────────────────────────────────────────
    stale_validators = [r for r in results if r["staleness_hours"] > 24]
    if stale_validators:
        print(f"\n  ⚠️  WEIGHT STALENESS WARNINGS")
        print(THIN_SEP)
        for r in stale_validators:
            print(f"  UID {r['uid']} {r['hotkey_short']}  |  Last weights: {r['staleness_hours']:.1f}h ago  |  {r['stale_flag']}")

    # ── Zero self-stake warnings ───────────────────────────────────────────
    no_skin = [r for r in results if r["self_stake_pct"] is not None and r["self_stake_pct"] == 0]
    if no_skin:
        print(f"\n  ⚠️  ZERO SELF-STAKE (no skin in the game)")
        print(THIN_SEP)
        for r in no_skin:
            take_display = f"{r['take']:.1%}" if r["take"] is not None else "unknown"
            print(f"  UID {r['uid']} {r['hotkey_short']}  |  Take: {take_display}  |  Emission APY: {fmt_apy(r['emission_apy'])}")

    # ── Emission share decay warnings ──────────────────────────────────────
    decaying = [r for r in results if r["e_share_delta"] is not None and r["e_share_delta"] < -0.01]
    if decaying:
        print(f"\n  ⚠️  EMISSION SHARE DECAY (losing share since last run)")
        print(THIN_SEP)
        for r in decaying:
            print(f"  UID {r['uid']} {r['hotkey_short']}  |  E Share: {r['e_share']:.4f}  |  Delta: {r['e_share_delta']:+.4f}")

    # ── Recommendation ─────────────────────────────────────────────────────
    best = results[0]
    print(f"\n  ✅ TOP: UID {best['uid']}  {best['hotkey']}")
    print(f"     Combined Real APY (30d) : {fmt_apy(best['combined_real_apy_30d'])}")
    print(f"     Combined Gross APY (30d): {fmt_apy(best['combined_apy_30d'])}")
    print(f"     Combined Real APY (7d)  : {fmt_apy(best['combined_real_apy_7d'])}")
    print(f"     Emission APY       : {fmt_apy(best['emission_apy'])}{'  (pre-take, take unknown)' if not best['take_known'] else ''}")
    print(f"     Real APY           : {fmt_apy(best['real_apy'])}")
    if best["take"] is not None:
        print(f"     Take               : {best['take']:.1%}")
    else:
        print(f"     Take               : unknown")
    print(f"     Validator Trust    : {best['tv']:.4f}")
    print(f"     Consensus          : {best['consensus']:.4f}")
    print(f"     TAO / day          : {best['your_tao_per_day']:.6f}")
    print(f"     Weight staleness   : {best['staleness_hours']:.1f}h  {best['stale_flag']}")
    if best["div_inc_ratio"] is not None:
        label = "pure validator" if best["div_inc_ratio"] > 5.0 else "validator-miner" if best["div_inc_ratio"] < 1.0 else "mixed"
        print(f"     Div/Inc ratio      : {best['div_inc_ratio']:.2f}  ({label})")
    if best["e_share_delta"] is not None:
        print(f"     E Share trend      : {best['e_share_delta']:+.4f}")
    if best["top1_pct"] is not None:
        print(f"     Concentration      : {best['conc_flag']}  (Top1: {best['top1_pct']:.1%}  Top3: {best['top3_pct']:.1%})")
    if best["self_stake_pct"] is not None:
        print(f"     Self-stake         : {best['self_stake_pct']:.1%}")
    print(f"\n  Snapshot block : {current_block:,}")
    print(f"  Timestamp      : {ts_str}")
    print(f"\n  Note: Real APY sourced from subnet_analysis/trajectory_all.json (subnet_analysis.py).")
    print(f"        7d price APY and emission share trend populate from the second run onwards.")
    print(f"        Price APY is momentum-based, not a return guarantee.")
    if any(not r["take_known"] for r in results):
        print(f"        * = take unknown, emission APY shown pre-take.")

    # ── Update trajectory ─────────────────────────────────────────────────
    traj_entry = {
        "date":                date_str,
        "block":               current_block,
        "spot_price":          spot_price,
        "moving_price":        moving_price,
        "alpha_outstanding":   alpha_out,
        "tao_reserves":        tao_reserves,
        "tao_per_block":       tao_per_block,
        "momentum_30d":        momentum_30d,
        "price_apy_30d":       price_apy_30d,
        "net_supply_delta":    net_supply_delta,
        "validator_e_shares":  current_e_shares,
        "top_validator_uid":       best["uid"],
        "top_validator_hotkey":    best["hotkey"],
        "top_combined_apy_30d":    best["combined_apy_30d"],
        "top_combined_real_apy_30d": best["combined_real_apy_30d"],
        "top_emission_apy":        best["emission_apy"],
    }
    trajectory = [e for e in trajectory if e.get("date") != date_str]
    trajectory.append(traj_entry)
    trajectory.sort(key=lambda e: e.get("date", ""))
    save_trajectory(netuid, trajectory)
    print(f"\n  📈 Trajectory updated: {len(trajectory)} snapshot(s) for SN{netuid}")

    # ── Save outputs ──────────────────────────────────────────────────────
    csv_path  = subnet_dir / f"snapshot_{date_str}.csv"
    json_path = subnet_dir / f"snapshot_{date_str}.json"

    fieldnames = [
        "uid", "hotkey", "hotkey_short", "stake", "div", "e_share",
        "e_share_delta", "div_inc_ratio",
        "take", "take_known", "tv", "consensus",
        "staleness_blocks", "staleness_hours", "stale_flag",
        "emission_apy", "price_apy_30d", "price_apy_7d",
        "combined_apy_30d", "combined_apy_7d",
        "combined_real_apy_30d", "combined_real_apy_7d",
        "real_apy",
        "net_supply_delta", "your_tao_per_day",
        "top1_pct", "top3_pct", "conc_flag",
        "nominator_count", "self_stake_pct",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    save_json(json_path, {
        "date":              date_str,
        "timestamp":         ts_str,
        "block":             current_block,
        "netuid":            netuid,
        "name":              name,
        "my_stake":          my_stake,
        "spot_price":        spot_price,
        "moving_price":      moving_price,
        "ema_band":          ema_band,
        "ema_lag_flag":      ema_lag_flag,
        "momentum_30d":      momentum_30d,
        "price_apy_30d":     price_apy_30d,
        "price_apy_7d":      price_apy_7d,
        "net_supply_delta":  net_supply_delta,
        "tao_per_block":     tao_per_block,
        "tao_reserves":      tao_reserves,
        "alpha_outstanding": alpha_out,
        "volume":            volume,
        "trajectory_runs":   len(trajectory),
        "validators_analysed": len(results),
        "sort_basis":        sort_basis,
        "recommended_uid":   best["uid"],
        "recommended_hotkey":best["hotkey"],
        "rankings": [
            {
                "rank":              i + 1,
                "uid":               r["uid"],
                "hotkey":            r["hotkey"],
                "stake":             r["stake"],
                "take":              r["take"],
                "take_known":        r["take_known"],
                "tv":                r["tv"],
                "consensus":         r["consensus"],
                "staleness_hours":   r["staleness_hours"],
                "stale_flag":        r["stale_flag"],
                "e_share":           r["e_share"],
                "e_share_delta":     r["e_share_delta"],
                "div_inc_ratio":     r["div_inc_ratio"],
                "emission_apy":      r["emission_apy"],
                "real_apy":          r["real_apy"],
                "price_apy_30d":     r["price_apy_30d"],
                "combined_apy_30d":  r["combined_apy_30d"],
                "combined_apy_7d":   r["combined_apy_7d"],
                "combined_real_apy_30d": r["combined_real_apy_30d"],
                "combined_real_apy_7d":  r["combined_real_apy_7d"],
                "conc_flag":         r["conc_flag"],
                "top1_pct":          r["top1_pct"],
                "top3_pct":          r["top3_pct"],
                "nominator_count":   r["nominator_count"],
                "self_stake_pct":    r["self_stake_pct"],
            }
            for i, r in enumerate(results)
        ],
    })

    print(f"\n💾  Outputs saved:")
    print(f"    {csv_path}")
    print(f"    {json_path}")
    print(f"    {OUTPUT_DIR}/SN{netuid}/trajectory.json")
    print(f"\n{SEPARATOR}\n")


# ── Entry point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Subnet Validator Analysis — Intelligence Sovereignty Research Suite"
    )
    parser.add_argument("--netuid", type=int, required=True,
                        help="Subnet ID to analyse")
    parser.add_argument("--stake", type=float, required=True,
                        help="Your intended stake in TAO")
    args = parser.parse_args()
    run_analysis(netuid=args.netuid, my_stake=args.stake)

if __name__ == "__main__":
    main()
