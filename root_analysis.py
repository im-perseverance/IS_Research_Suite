"""
root_analysis.py
================
Root Validator Analysis Tool — Intelligence Sovereignty Research Suite
@im_perseverance

Analyses all root validators and ranks them by estimated nominator yield.

Root dividends are empirically driven by emission flow (r=0.98 with dividends).
The exact mechanism remains under investigation — frozen Yuma Consensus weights
from pre-dTAO are the leading hypothesis after ruling out pure stake weighting,
subnet alpha exposure, parent/child delegation, and claimable rates.

Staleness and consensus score are retained in CSV/JSON for longitudinal tracking
but removed from console output — all root validators are equally stale (400+ days)
so the metric adds no discriminating value. Ghost validators (div=0) are included
in all outputs to monitor for unexpected state changes.

Usage:
    python root_analysis.py --stake 100

Arguments:
    --stake     Your intended stake in TAO (required)

Outputs:
    - Console report ranked by estimated yield
    - root_analysis/snapshot_YYYY-MM-DD.csv
    - root_analysis/snapshot_YYYY-MM-DD.json
    - root_analysis/trajectory.json  (longitudinal history across runs)

Notes on methodology:
    - Yield estimate: (your_share * dividend * (1 - take)) as a relative score.
    - Ghost validators: UIDs with stake but zero dividends. They earn nothing
      for nominators. Discovered in the Week 3 Root Staking article.
    - Concentration: top-1 and top-3 nominator share of root stake.
    - Self-stake: owner's stake on root as fraction of total. Zero = no skin
      in the game.
    - Subnet coverage: how many subnets the validator has permits on.
    - Identity: on-chain name if registered.
    - Pool size: total root stake on the validator — larger pools dilute yield.
"""

import argparse
import bittensor as bt
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────
OUTPUT_DIR     = Path("root_analysis")
BLOCKS_PER_DAY = 7200
MIN_STAKE      = 1000.0      # minimum root stake to be considered
SEPARATOR      = "=" * 160
THIN_SEP       = "-" * 160

# ── Helpers ────────────────────────────────────────────────────────────────

def safe_float(val, default=0.0):
    try:
        return float(val)
    except Exception:
        return default

def fmt_pct(val, decimals=1):
    if val is None:
        return " N/A"
    return f"{val*100:.{decimals}f}%"

def save_json(path, data):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)

def load_json(path):
    p = Path(path)
    if p.exists():
        try:
            with open(p) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

# ── Trajectory ─────────────────────────────────────────────────────────────

def load_trajectory():
    """Load longitudinal trajectory for root analysis."""
    path = OUTPUT_DIR / "trajectory.json"
    data = load_json(path)
    return data if isinstance(data, list) else []

def save_trajectory(trajectory):
    """Save longitudinal trajectory for root analysis."""
    path = OUTPUT_DIR / "trajectory.json"
    save_json(path, trajectory)

# ── Delegate analysis ──────────────────────────────────────────────────────

def analyse_root_delegate(delegate, owner_ss58):
    """
    Extract root-specific metrics from a DelegateInfo object:
      - take
      - root stake concentration (top-1, top-3)
      - nominator count on root
      - self-stake percentage on root
      - subnet coverage (validator_permits count)

    Returns a dict with all fields.
    """
    defaults = {
        "take": None, "top1_pct": None, "top3_pct": None,
        "conc_flag": "NO DATA", "nominator_count": None,
        "self_stake_pct": None, "subnet_count": None,
    }
    if not delegate:
        return defaults

    take = safe_float(getattr(delegate, "take", None), default=0.0)
    take = max(0.0, min(1.0, take))

    # Subnet coverage
    permits = getattr(delegate, "validator_permits", []) or []
    subnet_count = len(permits)

    # Owner coldkey
    owner = owner_ss58 or getattr(delegate, "owner_ss58", None)

    # Root (netuid=0) nominator analysis
    stakes = []
    owner_stake = 0.0
    for coldkey, subnet_stakes in delegate.nominators.items():
        # Root stake is on netuid 0
        root_balance = safe_float(subnet_stakes.get(0, 0))
        if root_balance > 0:
            stakes.append((coldkey, root_balance))
            if coldkey == owner:
                owner_stake = root_balance

    if not stakes:
        return {**defaults, "take": take, "subnet_count": subnet_count}

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
        "subnet_count": subnet_count,
    }

# ── Main ───────────────────────────────────────────────────────────────────

def run_analysis(my_stake: float):
    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str   = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load trajectory
    trajectory = load_trajectory()

    print(SEPARATOR)
    print("  ROOT VALIDATOR ANALYSIS — Intelligence Sovereignty Research Suite")
    print("  @im_perseverance")
    print(SEPARATOR)
    print(f"\n  Connecting to Bittensor network...")
    print(f"  Trajectory: {len(trajectory)} previous run(s)")

    sub           = bt.Subtensor(network="finney")
    current_block = sub.get_current_block()

    print(f"  Block     : {current_block:,}")
    print(f"  Timestamp : {ts_str}")
    print(f"  Stake     : {my_stake:,.2f} TAO\n")

    # ── Root metagraph ────────────────────────────────────────────────────
    print("  Loading root metagraph (netuid=0)...")
    meta   = sub.metagraph(netuid=0)
    n_uids = len(meta.uids)

    # ── Delegate identities (batch fetch) ─────────────────────────────────
    print("  Fetching delegate identities...")
    try:
        identities = sub.get_delegate_identities()
    except Exception:
        identities = {}

    # ── Build candidate list ──────────────────────────────────────────────
    print("  Analysing validators...\n")

    active_results = []
    ghost_validators = []
    all_validators = []  # both active and ghost for CSV/JSON

    for uid in range(n_uids):
        stake = safe_float(meta.stake[uid])
        if stake < MIN_STAKE:
            continue

        hotkey   = meta.hotkeys[uid]
        coldkey  = meta.coldkeys[uid]
        div      = safe_float(meta.dividends[uid])
        inc      = safe_float(meta.incentive[uid])
        tv       = safe_float(meta.validator_trust[uid])
        cons     = safe_float(meta.consensus[uid])
        emission = safe_float(meta.emission[uid])
        last     = int(meta.last_update[uid])

        staleness_blocks = current_block - last
        staleness_hours  = staleness_blocks / (BLOCKS_PER_DAY / 24)

        # Ghost validator detection (from Week 3 article)
        is_ghost = div == 0 and stake >= MIN_STAKE

        # Fetch delegate info for all validators (active and ghost)
        try:
            delegate = sub.get_delegate_by_hotkey(hotkey)
        except Exception:
            delegate = None

        delegate_info = analyse_root_delegate(delegate, coldkey)
        take = delegate_info["take"]

        # Yield estimate
        pool_total = stake + my_stake
        your_share = my_stake / pool_total if pool_total > 0 else 0
        if take is not None:
            your_yield = your_share * div * (1 - take)
        else:
            your_yield = your_share * div

        # Identity
        identity = identities.get(hotkey)
        name = identity.name if identity and identity.name else ""

        # Staleness flag (retained for CSV/JSON only)
        if staleness_hours > 48:
            stale_flag = "STALE"
        elif staleness_hours > 24:
            stale_flag = "AGING"
        else:
            stale_flag = "FRESH"

        record = {
            "uid":              uid,
            "hotkey":           hotkey,
            "hotkey_short":     hotkey[:8] + "...",
            "coldkey":          coldkey,
            "name":             name,
            "stake":            stake,
            "div":              div,
            "incentive":        inc,
            "emission":         emission,
            "tv":               tv,
            "consensus":        cons,
            "take":             take,
            "take_known":       take is not None,
            "staleness_blocks": staleness_blocks,
            "staleness_hours":  staleness_hours,
            "stale_flag":       stale_flag,
            "your_yield":       your_yield,
            "is_ghost":         is_ghost,
            "top1_pct":         delegate_info["top1_pct"],
            "top3_pct":         delegate_info["top3_pct"],
            "conc_flag":        delegate_info["conc_flag"],
            "nominator_count":  delegate_info["nominator_count"],
            "self_stake_pct":   delegate_info["self_stake_pct"],
            "subnet_count":     delegate_info["subnet_count"],
        }

        all_validators.append(record)

        if is_ghost:
            ghost_validators.append(record)
        else:
            active_results.append(record)

    active_results.sort(key=lambda r: r["your_yield"], reverse=True)

    # ── Console report: Active validators ─────────────────────────────────
    print(SEPARATOR)
    print(f"  ROOT VALIDATOR RANKINGS  |  Stake: {my_stake:,.2f} TAO  |  Sorted by estimated yield")
    print(f"  Active (dividend > 0, stake >= {MIN_STAKE:,.0f} TAO)")
    print(THIN_SEP)
    print(f"  {'UID':<6} {'Name':<20} {'Pool Size':>14} {'Div':>10} {'Take':>7} {'TV':>7} "
          f"{'Yield':>12} {'Top1':>7} {'Top3':>7} {'Conc':>12} {'Self%':>7} {'SNs':>5} "
          f"{'Noms':>6}  {'Hotkey'}")
    print(THIN_SEP)

    conc_labels = {
        "HIGH":        "🔴 HIGH",
        "MODERATE":    "🟡 MOD",
        "DISTRIBUTED": "🟢 DISTR",
        "NO DATA":     "  N/A",
    }

    for r in active_results:
        conc_sym  = conc_labels.get(r["conc_flag"], r["conc_flag"])
        top1_str  = f"{r['top1_pct']:.1%}" if r["top1_pct"] is not None else "N/A"
        top3_str  = f"{r['top3_pct']:.1%}" if r["top3_pct"] is not None else "N/A"
        take_str  = f"{r['take']:>6.1%}" if r["take"] is not None else "    ?%"
        self_str  = fmt_pct(r["self_stake_pct"]) if r["self_stake_pct"] is not None else " N/A"
        sn_str    = f"{r['subnet_count']:>5}" if r["subnet_count"] is not None else "  N/A"
        nom_str   = f"{r['nominator_count']:>6}" if r["nominator_count"] is not None else "   N/A"
        name_str  = r["name"][:20] if r["name"] else ""
        if not r["take_known"]:
            take_str = f"{take_str}*"
        print(
            f"  {r['uid']:<6} {name_str:<20} {r['stake']:>14,.0f} {r['div']:>10.6f} "
            f"{take_str:>7} {r['tv']:>7.4f} "
            f"{r['your_yield']:>12.6f} "
            f"{top1_str:>7} {top3_str:>7} "
            f"{conc_sym:<14}{self_str:>7} {sn_str} "
            f"{nom_str}  {r['hotkey']}"
        )

    print(SEPARATOR)
    print(f"  Active validators  : {len(active_results)}")
    print(f"  Ghost validators   : {len(ghost_validators)}  (stake > 0, dividend = 0)")
    print(f"  Total monitored    : {len(all_validators)}")

    # ── Ghost validator report ─────────────────────────────────────────────
    if ghost_validators:
        ghost_sorted = sorted(ghost_validators, key=lambda x: x["stake"], reverse=True)
        print(f"\n  👻 GHOST VALIDATORS (earning zero dividends)")
        print(THIN_SEP)
        print(f"  {'UID':<6} {'Pool Size':>14} {'Emission':>12} {'SNs':>5}  {'Hotkey'}")
        print(THIN_SEP)
        for g in ghost_sorted[:15]:
            sn_str = f"{g['subnet_count']:>5}" if g["subnet_count"] is not None else "  N/A"
            print(f"  {g['uid']:<6} {g['stake']:>14,.0f} {g['emission']:>12.8f} {sn_str}  {g['hotkey']}")
        if len(ghost_validators) > 15:
            print(f"  ... and {len(ghost_validators) - 15} more")

    # ── Zero self-stake warnings ───────────────────────────────────────────
    no_skin = [r for r in active_results if r["self_stake_pct"] is not None and r["self_stake_pct"] == 0]
    if no_skin:
        print(f"\n  ⚠️  ZERO SELF-STAKE ON ROOT (no skin in the game)")
        print(THIN_SEP)
        for r in no_skin:
            take_display = f"{r['take']:.1%}" if r["take"] is not None else "unknown"
            print(f"  UID {r['uid']} {r['hotkey_short']}  |  Take: {take_display}  |  Yield: {r['your_yield']:.6f}")

    # ── Recommendation ─────────────────────────────────────────────────────
    if active_results:
        best = active_results[0]
        print(f"\n  ✅ TOP: UID {best['uid']}  {best['hotkey']}")
        if best["name"]:
            print(f"     Name               : {best['name']}")
        print(f"     Estimated yield    : {best['your_yield']:.6f}")
        print(f"     Dividend           : {best['div']:.6f}")
        if best["take"] is not None:
            print(f"     Take               : {best['take']:.1%}")
        else:
            print(f"     Take               : unknown")
        print(f"     Validator Trust    : {best['tv']:.4f}")
        print(f"     Pool size          : {best['stake']:,.0f} TAO")
        if best["top1_pct"] is not None:
            print(f"     Concentration      : {best['conc_flag']}  (Top1: {best['top1_pct']:.1%}  Top3: {best['top3_pct']:.1%})")
        if best["nominator_count"] is not None:
            print(f"     Nominators         : {best['nominator_count']}")
        if best["self_stake_pct"] is not None:
            print(f"     Self-stake         : {best['self_stake_pct']:.1%}")
        if best["subnet_count"] is not None:
            print(f"     Subnet coverage    : {best['subnet_count']} subnet(s)")

    print(f"\n  Snapshot block : {current_block:,}")
    print(f"  Timestamp      : {ts_str}")

    # ── Update trajectory ─────────────────────────────────────────────────
    traj_entry = {
        "date":              date_str,
        "block":             current_block,
        "active_validators": len(active_results),
        "ghost_validators":  len(ghost_validators),
        "top_validator_uid":    active_results[0]["uid"] if active_results else None,
        "top_validator_hotkey": active_results[0]["hotkey"] if active_results else None,
        "top_validator_name":   active_results[0]["name"] if active_results else None,
        "top_yield":            active_results[0]["your_yield"] if active_results else None,
        "top_div":              active_results[0]["div"] if active_results else None,
        "top_take":             active_results[0]["take"] if active_results else None,
    }
    trajectory = [e for e in trajectory if e.get("date") != date_str]
    trajectory.append(traj_entry)
    trajectory.sort(key=lambda e: e.get("date", ""))
    save_trajectory(trajectory)
    print(f"\n  📈 Trajectory updated: {len(trajectory)} snapshot(s)")

    # ── Save outputs (all validators — active + ghost) ────────────────────
    csv_path  = OUTPUT_DIR / f"snapshot_{date_str}.csv"
    json_path = OUTPUT_DIR / f"snapshot_{date_str}.json"

    # Sort: active first (by yield desc), then ghosts (by stake desc)
    all_sorted = sorted(active_results, key=lambda r: r["your_yield"], reverse=True) + \
                 sorted(ghost_validators, key=lambda r: r["stake"], reverse=True)

    fieldnames = [
        "uid", "hotkey", "hotkey_short", "name", "stake", "div",
        "incentive", "emission", "tv", "consensus",
        "take", "take_known", "is_ghost",
        "staleness_blocks", "staleness_hours", "stale_flag",
        "your_yield",
        "top1_pct", "top3_pct", "conc_flag",
        "nominator_count", "self_stake_pct", "subnet_count",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_sorted)

    save_json(json_path, {
        "date":               date_str,
        "timestamp":          ts_str,
        "block":              current_block,
        "my_stake":           my_stake,
        "active_validators":  len(active_results),
        "ghost_validators":   len(ghost_validators),
        "ghost_uids":         [g["uid"] for g in ghost_validators],
        "recommended_uid":    active_results[0]["uid"] if active_results else None,
        "recommended_hotkey": active_results[0]["hotkey"] if active_results else None,
        "rankings": [
            {
                "rank":             i + 1,
                "uid":              r["uid"],
                "hotkey":           r["hotkey"],
                "name":             r["name"],
                "stake":            r["stake"],
                "div":              r["div"],
                "take":             r["take"],
                "take_known":       r["take_known"],
                "tv":               r["tv"],
                "emission":         r["emission"],
                "is_ghost":         r["is_ghost"],
                "your_yield":       r["your_yield"],
                "conc_flag":        r["conc_flag"],
                "top1_pct":         r["top1_pct"],
                "top3_pct":         r["top3_pct"],
                "nominator_count":  r["nominator_count"],
                "self_stake_pct":   r["self_stake_pct"],
                "subnet_count":     r["subnet_count"],
            }
            for i, r in enumerate(all_sorted)
        ],
    })

    print(f"\n💾  Outputs saved:")
    print(f"    {csv_path}")
    print(f"    {json_path}")
    print(f"    {OUTPUT_DIR}/trajectory.json")
    print(f"\n{SEPARATOR}\n")


# ── Entry point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Root Validator Analysis — Intelligence Sovereignty Research Suite"
    )
    parser.add_argument("--stake", type=float, required=True,
                        help="Your intended stake in TAO")
    args = parser.parse_args()
    run_analysis(my_stake=args.stake)

if __name__ == "__main__":
    main()
