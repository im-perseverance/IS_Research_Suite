"""
distil_sn97_snapshot.py
========================================
SN97 Distil — Genesis Period Deep Dive Tracker
Part of the Intelligence Sovereignty research suite by @perseverance

Tracks the full embryonic lifecycle of SN97 Distil from genesis:
  - Reserve dynamics (TAO + Alpha) — pulled via all_subnets()
  - Alpha price vs. EMA momentum
  - Validator registry: Tv score, Yuma alignment, take rate, stake
  - UID-level & coldkey-level concentration (matching best_subnet.py logic)
  - Daily delta vs. previous snapshot (if available)
  - Longitudinal trajectory.json updated on every run

Owner hotkey: 5G9ZvMXQEecYs8eA4xQ6Mrb9BHPQWs2g7u2RR6UXxrWQceZW
Displaced:    SN97 FlameWire (deregistered 13 Mar 2026)
Registration: 560.77τ (~$132.62K) at block ~13 Mar 2026

Recommended cadence: Daily at 12:00 UTC (or on-demand)
Output: Console + JSON snapshot saved to snapshots/distil/

Usage:
    python distil_sn97_snapshot.py
    python distil_sn97_snapshot.py --stake 1000
    python distil_sn97_snapshot.py --no-save   (skip JSON export)
"""

import bittensor as bt
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
NETUID            = 97
SUBNET_NAME       = "distil"
OWNER_HOTKEY      = "5G9ZvMXQEecYs8eA4xQ6Mrb9BHPQWs2g7u2RR6UXxrWQceZW"
GENESIS_DATE      = "2026-03-13"
DISPLACED_SUBNET  = "FlameWire"
STAKE_AMOUNT      = 1000.0
SNAPSHOT_DIR      = Path("snapshots/distil")
PREVIOUS_FILE     = SNAPSHOT_DIR / "latest.json"
TRAJECTORY_FILE   = SNAPSHOT_DIR / "trajectory.json"
YUMA_TV_THRESHOLD = 0.80

SEPARATOR = "=" * 100
THIN_SEP  = "-" * 100


# ── Helpers ───────────────────────────────────────────────────────────────────

def classify_concentration(top1_pct: float, top5_pct: float) -> str:
    if top1_pct > 0.50 or top5_pct > 0.70:
        return "HIGH RISK"
    elif top1_pct > 0.30:
        return "MODERATE"
    return "DISTRIBUTED"


def yuma_aligned(tv: float) -> str:
    if tv >= YUMA_TV_THRESHOLD:
        return "✅ ALIGNED"
    elif tv >= 0.50:
        return "⚠️  PARTIAL"
    return "❌ MISALIGNED"


def abs_delta(current: float, previous: float, fmt: str = ".4f") -> str:
    delta = current - previous
    sign  = "+" if delta >= 0 else ""
    return f"{sign}{delta:{fmt}}"


def load_previous() -> dict | None:
    if PREVIOUS_FILE.exists():
        with open(PREVIOUS_FILE, "r") as f:
            return json.load(f)
    return None


def load_trajectory() -> dict:
    if TRAJECTORY_FILE.exists():
        with open(TRAJECTORY_FILE, "r") as f:
            return json.load(f)
    return {
        "netuid":           NETUID,
        "subnet":           SUBNET_NAME,
        "genesis_date":     GENESIS_DATE,
        "displaced_subnet": DISPLACED_SUBNET,
        "owner_hotkey":     OWNER_HOTKEY,
        "generated_by":     "Intelligence Sovereignty Research Suite | @perseverance",
        "snapshots_count":  0,
        "first_snapshot":   None,
        "latest_snapshot":  None,
        "summary":          {},
        "series":           [],
    }


def days_since_genesis() -> int:
    genesis = datetime.strptime(GENESIS_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - genesis).days


def safe_price_apy(momentum_pct: float) -> float:
    """
    Annualised price APY from 30-day momentum.
    Clamped to [-100, 1000] daily to prevent genesis overflow
    when alpha price spikes from near-zero.
    """
    try:
        daily_momentum = momentum_pct / 30
        daily_momentum = max(min(daily_momentum, 1000.0), -100.0)
        return ((1 + daily_momentum / 100) ** 365 - 1) * 100
    except (OverflowError, ZeroDivisionError):
        return 0.0


def save_snapshot(data: dict):
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    ts           = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    archive_file = SNAPSHOT_DIR / f"snapshot_{ts}.json"
    with open(archive_file, "w") as f:
        json.dump(data, f, indent=2)
    with open(PREVIOUS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\n💾  Snapshot saved → {PREVIOUS_FILE}")
    print(f"📁  Archive copy  → {archive_file}")


def _build_series_entry(data: dict, existing_series: list) -> dict:
    """
    Build a single trajectory entry with day-over-day deltas.
    Tracks all key scalar metrics + structural shift annotations.
    """
    prev = existing_series[-1] if existing_series else None

    def d(key, default=0.0):
        return data.get(key, default)

    def delta(key, default=0.0):
        if prev is None:
            return None
        curr_val = data.get(key, default)
        prev_val = prev.get(key, default)
        if prev_val == 0:
            return None
        return round(curr_val - prev_val, 6)

    def pct_change(key, default=0.0):
        if prev is None:
            return None
        curr_val = data.get(key, default)
        prev_val = prev.get(key, default)
        if prev_val == 0:
            return None
        return round((curr_val - prev_val) / abs(prev_val) * 100, 4)

    # Structural shift annotations
    annotations = []

    # Reserve milestones
    tao = d("tao_reserves")
    if prev:
        prev_tao = prev.get("tao_reserves", 0)
        for threshold in [500, 1000, 2000, 5000, 10000]:
            if prev_tao < threshold <= tao:
                annotations.append(f"TAO reserves crossed {threshold} TAO")
        if tao < prev_tao * 0.9:
            annotations.append(f"TAO reserves dropped >10% — selling pressure")

    # Validator alignment shifts
    if prev:
        prev_aligned    = prev.get("yuma_aligned", 0)
        prev_misaligned = prev.get("yuma_misaligned", 0)
        curr_aligned    = d("yuma_aligned")
        curr_misaligned = d("yuma_misaligned")
        if curr_aligned > prev_aligned:
            annotations.append(f"Yuma alignment improved: {prev_aligned} → {curr_aligned} aligned validators")
        if curr_misaligned > prev_misaligned:
            annotations.append(f"Yuma alignment degraded: {prev_misaligned} → {curr_misaligned} misaligned")

    # Concentration shifts
    if prev:
        prev_conc = prev.get("uid_concentration", "")
        curr_conc = d("uid_concentration", "")
        if prev_conc != curr_conc:
            annotations.append(f"UID concentration shifted: {prev_conc} → {curr_conc}")

    # Active UID changes
    if prev:
        uid_delta = int(d("n_active")) - int(prev.get("n_active", 0))
        if uid_delta > 0:
            annotations.append(f"+{uid_delta} new active UID(s) — miners entering")
        elif uid_delta < 0:
            annotations.append(f"{uid_delta} UID(s) exited — miner churn")

    # Price volatility
    if prev:
        p_chg = pct_change("price")
        if p_chg is not None and abs(p_chg) > 20:
            annotations.append(f"Alpha price moved {p_chg:+.1f}% — high volatility")

    # Owner stake changes
    if prev:
        prev_owner_pct = prev.get("owner_stake_pct", 0)
        curr_owner_pct = d("owner_stake_pct", 0)
        if prev_owner_pct == 0 and curr_owner_pct > 0:
            annotations.append(f"Owner hotkey became active — first stake detected")
        elif curr_owner_pct > 0 and abs(curr_owner_pct - prev_owner_pct) > 0.05:
            annotations.append(f"Owner stake changed: {prev_owner_pct*100:.1f}% → {curr_owner_pct*100:.1f}%")

    entry = {
        "date":                     data["timestamp"][:10],
        "day":                      d("days_since_genesis"),
        "timestamp":                data["timestamp"],
        "block":                    data["block"],

        # Pool
        "tao_reserves":             round(d("tao_reserves"), 4),
        "alpha_reserves":           round(d("alpha_reserves"), 2),
        "alpha_out":                round(d("alpha_out"), 2),
        "price":                    round(d("price"), 8),
        "moving_price":             round(d("moving_price"), 8),
        "momentum_pct":             round(d("momentum_pct"), 4),
        "emission_per_block":       round(d("emission_per_block"), 6),

        # Yield
        "emission_apy":             round(d("emission_apy"), 4),
        "price_apy":                round(d("price_apy"), 4),
        "combined_apy":             round(d("combined_apy"), 4),
        "genesis_momentum_clamped": data.get("genesis_momentum_clamped", False),

        # Network
        "n_uids":                   int(d("n_uids")),
        "n_active":                 int(d("n_active")),
        "n_validators":             int(d("n_validators")),
        "total_stake":              round(d("total_stake"), 2),

        # Concentration
        "uid_top1_pct":             round(d("uid_top1_pct"), 6),
        "uid_top3_pct":             round(d("uid_top3_pct"), 6),
        "uid_top5_pct":             round(d("uid_top5_pct"), 6),
        "uid_concentration":        d("uid_concentration", ""),
        "ck_top1_pct":              round(d("ck_top1_pct") or 0, 6),
        "ck_top3_pct":              round(d("ck_top3_pct") or 0, 6),
        "ck_top5_pct":              round(d("ck_top5_pct") or 0, 6),
        "ck_concentration":         d("ck_concentration", ""),

        # Yuma
        "yuma_aligned":             int(d("yuma_aligned")),
        "yuma_partial":             int(d("yuma_partial")),
        "yuma_misaligned":          int(d("yuma_misaligned")),

        # Owner
        "owner_uid":                data.get("owner_uid"),
        "owner_stake":              round(d("owner_stake"), 4),
        "owner_stake_pct":          round(d("owner_stake_pct"), 6),

        # Deltas vs previous day
        "deltas": {
            "tao_reserves":     delta("tao_reserves"),
            "alpha_out":        delta("alpha_out"),
            "price":            delta("price"),
            "momentum_pct":     delta("momentum_pct"),
            "total_stake":      delta("total_stake"),
            "n_active":         delta("n_active", 0),
            "uid_top1_pct":     delta("uid_top1_pct"),
            "emission_apy":     delta("emission_apy"),
            "combined_apy":     delta("combined_apy"),
            "owner_stake_pct":  delta("owner_stake_pct"),
        },

        # Pct changes vs previous day
        "pct_changes": {
            "tao_reserves":   pct_change("tao_reserves"),
            "price":          pct_change("price"),
            "total_stake":    pct_change("total_stake"),
            "alpha_out":      pct_change("alpha_out"),
        },

        # Structural annotations
        "annotations": annotations,
    }

    return entry


def _compute_summary(series: list) -> dict:
    """
    Compute summary statistics across all trajectory entries.
    Useful for article writing — pull these directly.
    """
    if not series:
        return {}

    prices       = [s["price"]        for s in series if s["price"] > 0]
    tao_reserves = [s["tao_reserves"] for s in series if s["tao_reserves"] > 0]
    apys         = [s["combined_apy"] for s in series if s["combined_apy"] != 0]
    stakes       = [s["total_stake"]  for s in series if s["total_stake"] > 0]
    active_uids  = [s["n_active"]     for s in series]

    # Validator alignment history
    all_annotations = []
    for s in series:
        for a in s.get("annotations", []):
            all_annotations.append({"date": s["date"], "day": s["day"], "event": a})

    first = series[0]
    last  = series[-1]

    price_change_total = None
    if first["price"] > 0 and last["price"] > 0:
        price_change_total = round((last["price"] - first["price"]) / first["price"] * 100, 2)

    tao_change_total = None
    if first["tao_reserves"] > 0 and last["tao_reserves"] > 0:
        tao_change_total = round(last["tao_reserves"] - first["tao_reserves"], 4)

    return {
        "days_tracked":           len(series),
        "first_date":             first["date"],
        "latest_date":            last["date"],
        "first_day":              first["day"],
        "latest_day":             last["day"],

        # Price
        "price_first":            round(first["price"], 8),
        "price_latest":           round(last["price"], 8),
        "price_min":              round(min(prices), 8)    if prices else None,
        "price_max":              round(max(prices), 8)    if prices else None,
        "price_change_total_pct": price_change_total,

        # Reserves
        "tao_reserves_first":     round(first["tao_reserves"], 4),
        "tao_reserves_latest":    round(last["tao_reserves"], 4),
        "tao_reserves_min":       round(min(tao_reserves), 4) if tao_reserves else None,
        "tao_reserves_max":       round(max(tao_reserves), 4) if tao_reserves else None,
        "tao_reserves_net_change":tao_change_total,

        # Stake
        "total_stake_first":      round(first["total_stake"], 2),
        "total_stake_latest":     round(last["total_stake"], 2),
        "total_stake_max":        round(max(stakes), 2)   if stakes else None,

        # Active UIDs
        "n_active_first":         first["n_active"],
        "n_active_latest":        last["n_active"],
        "n_active_max":           max(active_uids)        if active_uids else None,

        # APY range
        "combined_apy_min":       round(min(apys), 4)     if apys else None,
        "combined_apy_max":       round(max(apys), 4)     if apys else None,
        "combined_apy_latest":    round(last["combined_apy"], 4),

        # Structural events log
        "structural_events":      all_annotations,

        # Concentration trajectory
        "concentration_history": [
            {"date": s["date"], "uid_concentration": s["uid_concentration"],
             "ck_concentration": s["ck_concentration"]}
            for s in series
        ],

        # Yuma alignment trajectory
        "yuma_history": [
            {"date": s["date"], "aligned": s["yuma_aligned"],
             "partial": s["yuma_partial"], "misaligned": s["yuma_misaligned"]}
            for s in series
        ],

        # Owner stake trajectory
        "owner_stake_history": [
            {"date": s["date"], "owner_uid": s["owner_uid"],
             "owner_stake_pct": s["owner_stake_pct"]}
            for s in series
        ],
    }


def update_trajectory(data: dict):
    """Append or update today's entry in trajectory.json."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    traj     = load_trajectory()
    date_str = data["timestamp"][:10]

    existing_dates = [s["date"] for s in traj["series"]]

    if date_str in existing_dates:
        idx = existing_dates.index(date_str)
        traj["series"][idx] = _build_series_entry(data, traj["series"][:idx])
        print(f"🔄  Trajectory updated (overwrite) for {date_str}")
    else:
        traj["series"].append(_build_series_entry(data, traj["series"]))
        print(f"📈  Trajectory appended for {date_str}")

    traj["series"].sort(key=lambda x: x["date"])

    traj["snapshots_count"] = len(traj["series"])
    traj["first_snapshot"]  = traj["series"][0]["date"]  if traj["series"] else None
    traj["latest_snapshot"] = traj["series"][-1]["date"] if traj["series"] else None
    traj["summary"]         = _compute_summary(traj["series"])

    with open(TRAJECTORY_FILE, "w") as f:
        json.dump(traj, f, indent=2)
    print(f"📊  Trajectory file saved  → {TRAJECTORY_FILE}")


def get_coldkey_concentration(sub, meta, price: float) -> tuple:
    try:
        coldkey_stakes = {}
        for uid in range(len(meta.hotkeys)):
            hotkey = meta.hotkeys[uid]
            try:
                delegate = sub.get_delegate_by_hotkey(hotkey)
                if not delegate or not delegate.nominators:
                    continue
                for coldkey, subnet_stakes_dict in delegate.nominators.items():
                    alpha     = float(subnet_stakes_dict.get(NETUID, 0))
                    tao_equiv = alpha * price
                    if tao_equiv > 0:
                        coldkey_stakes[coldkey] = coldkey_stakes.get(coldkey, 0) + tao_equiv
            except Exception:
                continue

        if not coldkey_stakes:
            return None, None, None, "NO DATA"

        sorted_stakes = sorted(coldkey_stakes.values(), reverse=True)
        total = sum(sorted_stakes)
        if total == 0:
            return None, None, None, "NO DATA"

        top1_pct = sorted_stakes[0] / total
        top3_pct = sum(sorted_stakes[:3]) / total
        top5_pct = sum(sorted_stakes[:5]) / total
        flag     = classify_concentration(top1_pct, top5_pct)

        return top1_pct, top3_pct, top5_pct, flag
    except Exception:
        return None, None, None, "NO DATA"


# ── Core Logic ────────────────────────────────────────────────────────────────

def run_snapshot(stake_amount: float, save: bool):
    days_old = days_since_genesis()

    print(f"\n{SEPARATOR}")
    print(f"  SN{NETUID} {SUBNET_NAME.upper()} — GENESIS DEEP DIVE SNAPSHOT")
    print(f"  Intelligence Sovereignty Research Suite | @perseverance")
    print(f"  Genesis: {GENESIS_DATE} | Day {days_old} of tracking")
    print(f"  Displaced: {DISPLACED_SUBNET} | Owner: {OWNER_HOTKEY[:16]}...{OWNER_HOTKEY[-6:]}")
    print(SEPARATOR)

    print("\nConnecting to Bittensor network...")
    sub   = bt.Subtensor(network="finney")
    block = sub.get_current_block()
    now   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"Block: {block:,} | Timestamp: {now}")

    print(f"\nLoading SN{NETUID} pool data...")
    all_s  = sub.all_subnets()
    subnet = next((s for s in all_s if s.netuid == NETUID), None)

    if subnet is None:
        print(f"ERROR: SN{NETUID} not found in all_subnets(). Exiting.")
        return {}

    tao_in_pool        = float(getattr(subnet, "tao_in_pool", None) or getattr(subnet, "tao_in", 0))
    alpha_in_pool      = float(getattr(subnet, "alpha_in_pool", None) or getattr(subnet, "alpha_in", 0))
    alpha_out          = float(getattr(subnet, 'alpha_out', 0))
    price              = float(subnet.price)
    moving_price       = float(subnet.moving_price)
    emission_per_block = float(subnet.tao_in_emission)

    momentum_pct = ((price - moving_price) / moving_price * 100) if moving_price > 0 else 0
    momentum_str = f"+{momentum_pct:.2f}%" if momentum_pct >= 0 else f"{momentum_pct:.2f}%"

    print(f"Loading SN{NETUID} metagraph...")
    meta = sub.metagraph(NETUID)

    n_uids       = len(meta.uids)
    active_uids  = [i for i, s in enumerate(meta.stake) if float(s) > 0]
    n_active     = len(active_uids)
    total_stake  = sum(float(s) for s in meta.stake)
    vali_uids    = [i for i, vp in enumerate(meta.validator_permit) if vp]
    n_validators = len(vali_uids)

    stake_list = sorted(
        [(i, float(meta.stake[i])) for i in active_uids],
        key=lambda x: x[1], reverse=True
    )
    uid_top1 = stake_list[0][1] / total_stake if stake_list and total_stake > 0 else 0
    uid_top3 = sum(s for _, s in stake_list[:3]) / total_stake if total_stake > 0 else 0
    uid_top5 = sum(s for _, s in stake_list[:5]) / total_stake if total_stake > 0 else 0
    uid_concentration = classify_concentration(uid_top1, uid_top5)

    print("Computing coldkey concentration (this may take a moment)...")
    ck_top1, ck_top3, ck_top5, ck_concentration = get_coldkey_concentration(sub, meta, price)

    # Owner tracking
    owner_uid       = None
    owner_stake     = 0.0
    owner_stake_pct = 0.0
    for uid in range(len(meta.hotkeys)):
        if meta.hotkeys[uid] == OWNER_HOTKEY:
            owner_uid       = uid
            owner_stake     = float(meta.stake[uid])
            owner_stake_pct = owner_stake / total_stake if total_stake > 0 else 0
            break

    validators = []
    for uid in vali_uids:
        hotkey       = meta.hotkeys[uid] if hasattr(meta, 'hotkeys') else "unknown"
        hotkey_short = hotkey[:8] + "..." if len(hotkey) > 8 else hotkey
        stake_val    = float(meta.stake[uid])
        tv           = float(meta.validator_trust[uid]) if hasattr(meta, 'validator_trust') else 0.0
        dividends    = float(meta.dividends[uid]) if hasattr(meta, 'dividends') else 0.0
        incentive    = float(meta.incentive[uid])  if hasattr(meta, 'incentive')  else 0.0
        is_owner     = (hotkey == OWNER_HOTKEY)

        take_rate = 0.0
        try:
            delegate  = sub.get_delegate_by_hotkey(hotkey)
            take_rate = delegate.take if delegate else 0.0
        except Exception:
            take_rate = 0.0

        validators.append({
            "uid":          uid,
            "hotkey":       hotkey,
            "hotkey_short": hotkey_short,
            "stake":        stake_val,
            "stake_pct":    stake_val / total_stake if total_stake > 0 else 0,
            "tv":           tv,
            "alignment":    yuma_aligned(tv),
            "take":         take_rate,
            "dividends":    dividends,
            "incentive":    incentive,
            "is_owner":     is_owner,
        })

    validators.sort(key=lambda x: x["stake"], reverse=True)

    aligned_count    = sum(1 for v in validators if v["tv"] >= YUMA_TV_THRESHOLD)
    partial_count    = sum(1 for v in validators if 0.5 <= v["tv"] < YUMA_TV_THRESHOLD)
    misaligned_count = sum(1 for v in validators if v["tv"] < 0.5)

    blocks_per_year = 365 * 24 * 3600 / 12
    best_uid_data   = max(validators, key=lambda x: x["tv"] * x["stake_pct"]) if validators else None

    if total_stake > 0 and emission_per_block > 0:
        staker_share     = stake_amount / (total_stake + stake_amount)
        annual_tao_yield = staker_share * emission_per_block * blocks_per_year
        emission_apy     = (annual_tao_yield / stake_amount) * 100
    else:
        emission_apy = 0.0

    genesis_momentum_flag = abs(momentum_pct) > 100
    raw_price_apy         = safe_price_apy(momentum_pct)
    price_apy             = min(raw_price_apy, 1000.0) if raw_price_apy > 0 else max(raw_price_apy, -100.0)
    price_apy_capped      = genesis_momentum_flag and raw_price_apy > 1000.0
    combined_apy          = emission_apy + price_apy

    prev = load_previous()

    def delta_str(key, current, fmt=".2f"):
        if prev and key in prev and prev[key] != 0:
            return f"  Δ {abs_delta(current, prev[key], fmt)}"
        return ""

    # ── Print Report ──────────────────────────────────────────────────────────
    print(f"\n{SEPARATOR}")
    print(f"  POOL DYNAMICS  (Day {days_old} since genesis)")
    print(THIN_SEP)
    print(f"  TAO Reserves    : {tao_in_pool:>12,.4f} TAO{delta_str('tao_reserves', tao_in_pool)}")
    print(f"  Alpha Reserves  : {alpha_in_pool:>12,.2f} α  {delta_str('alpha_reserves', alpha_in_pool)}")
    print(f"  Alpha Circulating:{alpha_out:>12,.2f} α  {delta_str('alpha_out', alpha_out)}")
    print(f"  Alpha Price     : {price:>12.6f} TAO{delta_str('price', price, '.6f')}")
    print(f"  Moving Price    : {moving_price:>12.6f} TAO{delta_str('moving_price', moving_price, '.6f')}")
    momentum_flag_str = "  ⚠️  EMA not yet meaningful — Price APY suppressed" if genesis_momentum_flag else ""
    print(f"  Momentum        : {momentum_str:>12}{momentum_flag_str}")
    print(f"  Emission/block  : {emission_per_block:>12.6f} TAO{delta_str('emission_per_block', emission_per_block, '.6f')}")

    print(f"\n{SEPARATOR}")
    print(f"  YIELD PROFILE  (simulated stake: {stake_amount:,.1f} TAO)")
    print(THIN_SEP)
    print(f"  Emission APY    : {emission_apy:>+10.4f}%")
    if price_apy_capped:
        print(f"  Price APY       : {price_apy:>+10.4f}%  ⚠️  capped at 1000% (raw: {raw_price_apy:,.0f}% — EMA unreliable at {momentum_str})")
        print(f"  Combined APY    : {combined_apy:>+10.4f}%  (capped)")
    else:
        print(f"  Price APY       : {price_apy:>+10.4f}%")
        print(f"  Combined APY    : {combined_apy:>+10.4f}%")
    if best_uid_data:
        print(f"  Best Validator  : UID {best_uid_data['uid']} ({best_uid_data['hotkey_short']})  "
              f"Tv={best_uid_data['tv']:.4f}  Take={best_uid_data['take']:.1%}  {best_uid_data['alignment']}")

    print(f"\n{SEPARATOR}")
    print(f"  NETWORK COMPOSITION")
    print(THIN_SEP)
    print(f"  Total UIDs      : {n_uids}")
    print(f"  Active UIDs     : {n_active}{delta_str('n_active', n_active, 'd')}")
    print(f"  Validators      : {n_validators}{delta_str('n_validators', n_validators, 'd')}")
    print(f"  Total Stake     : {total_stake:>12,.2f} α  {delta_str('total_stake', total_stake)}")
    print()
    print(f"  UID-level Concentration  : {uid_concentration}")
    print(f"  Top-1: {uid_top1*100:>5.1f}%   Top-3: {uid_top3*100:.1f}%   Top-5: {uid_top5*100:.1f}%")
    print()
    if ck_top1 is not None:
        print(f"  Coldkey Concentration    : {ck_concentration}")
        print(f"  Top-1: {ck_top1*100:>5.1f}%   Top-3: {ck_top3*100:.1f}%   Top-5: {ck_top5*100:.1f}%")
    else:
        print(f"  Coldkey Concentration    : NO DATA")

    print(f"\n{SEPARATOR}")
    print(f"  OWNER HOTKEY TRACKING")
    print(THIN_SEP)
    if owner_uid is not None:
        print(f"  Owner UID       : {owner_uid}")
        print(f"  Owner Stake     : {owner_stake:>12,.2f} α  ({owner_stake_pct*100:.1f}% of total)")
        is_vali_str = "✅ Yes" if owner_uid in vali_uids else "❌ No"
        print(f"  Is Validator    : {is_vali_str}")
    else:
        print(f"  Owner hotkey not yet registered as UID in metagraph.")
    print(f"  Owner Hotkey    : {OWNER_HOTKEY}")

    print(f"\n{SEPARATOR}")
    print(f"  VALIDATOR REGISTRY — Tv Score & Yuma Alignment")
    print(THIN_SEP)
    print(f"  {'UID':>4}  {'Hotkey':10}  {'Stake (α)':>14}  {'Stake%':>7}  {'Tv':>6}  {'Take':>6}  {'Alignment':14}  {'Owner':>7}  {'Dividends':>10}  {'Incentive':>10}")
    print(THIN_SEP)
    for v in validators:
        owner_tag = "🏠 Yes" if v['is_owner'] else "     No"
        print(
            f"  {v['uid']:>4}  "
            f"{v['hotkey_short']:10}  "
            f"{v['stake']:>14,.2f}  "
            f"{v['stake_pct']*100:>6.1f}%  "
            f"{v['tv']:>6.4f}  "
            f"{v['take']:>5.1%}  "
            f"{v['alignment']:14}  "
            f"{owner_tag:>7}  "
            f"{v['dividends']:>10.6f}  "
            f"{v['incentive']:>10.6f}"
        )
    print(THIN_SEP)
    print(f"  Aligned ✅: {aligned_count}   Partial ⚠️: {partial_count}   Misaligned ❌: {misaligned_count}")

    print(f"\n{SEPARATOR}")
    print(f"  TOP STAKERS (UID-level, top 15)")
    print(THIN_SEP)
    print(f"  {'Rank':>4}  {'UID':>4}  {'Hotkey':10}  {'Stake (α)':>14}  {'Stake%':>7}  {'Is Validator':>12}  {'Is Owner':>9}")
    print(THIN_SEP)
    for rank, (uid, stake_val) in enumerate(stake_list[:15], 1):
        hk     = meta.hotkeys[uid] if hasattr(meta, 'hotkeys') else "unknown"
        hk_s   = hk[:8] + "..." if len(hk) > 8 else hk
        is_val = "✅ Yes" if uid in vali_uids else "  No"
        is_own = "🏠 Yes" if hk == OWNER_HOTKEY else "   No"
        print(
            f"  {rank:>4}  "
            f"{uid:>4}  "
            f"{hk_s:10}  "
            f"{stake_val:>14,.2f}  "
            f"{stake_val/total_stake*100:>6.1f}%  "
            f"{is_val:>12}  "
            f"{is_own:>9}"
        )

    print(f"\n{SEPARATOR}")
    print(f"  ⚠️  GENESIS WATCH FLAGS  (Day {days_old})")
    print(THIN_SEP)

    flags = []

    if tao_in_pool < 500:
        flags.append(f"🔴 Ultra-thin reserve ({tao_in_pool:.4f} TAO) — price highly volatile on small flows")
    elif tao_in_pool < 2000:
        flags.append(f"🟡 Thin reserve ({tao_in_pool:.4f} TAO) — entering price discovery phase")
    else:
        flags.append(f"🟢 Reserve building ({tao_in_pool:.4f} TAO) — liquidity stabilizing")

    if genesis_momentum_flag:
        flags.append(f"🟡 Genesis momentum spike ({momentum_str}) — EMA not yet meaningful, Price APY clamped")
    elif abs(momentum_pct) > 5:
        flags.append(f"🔴 High momentum ({momentum_str}) — EMA lagging, elevated price risk")
    elif abs(momentum_pct) > 2:
        flags.append(f"🟡 Moderate momentum ({momentum_str}) — monitor for mean reversion")

    if misaligned_count > 0:
        flags.append(f"🔴 {misaligned_count} misaligned validator(s) — Yuma consensus risk")
    if aligned_count == 0:
        flags.append(f"🔴 No fully Yuma-aligned validators — genesis bootstrapping")

    if ck_top1 is not None and (ck_top1 > 0.50 or (ck_top5 or 0) > 0.70):
        flags.append(f"🔴 Coldkey concentration HIGH RISK — top-1 coldkey controls {ck_top1*100:.1f}%")
    elif uid_top1 > 0.50:
        flags.append(f"🔴 UID concentration HIGH RISK — top-1 UID controls {uid_top1*100:.1f}%")

    if n_validators < 5:
        flags.append(f"🟡 Only {n_validators} validators — subnet still bootstrapping")

    if owner_uid is None:
        flags.append(f"🟡 Owner hotkey not yet active in metagraph — watch for first move")
    elif owner_stake_pct > 0.50:
        flags.append(f"🔴 Owner controls {owner_stake_pct*100:.1f}% of stake — centralization risk")

    if days_old <= 14:
        flags.append(f"🟡 Day {days_old} — subnet within likely immune period. Deregistration not yet possible.")

    if prev:
        uid_growth = n_active - prev.get("n_active", n_active)
        if uid_growth > 0:
            flags.append(f"🟢 +{uid_growth} new active UID(s) since last snapshot — miners entering")
        elif uid_growth < 0:
            flags.append(f"🔴 {abs(uid_growth)} UID(s) exited since last snapshot — miner churn")

        tao_delta = tao_in_pool - prev.get("tao_reserves", tao_in_pool)
        if tao_delta > 100:
            flags.append(f"🟢 TAO reserves grew +{tao_delta:.4f} TAO since last snapshot")
        elif tao_delta < -100:
            flags.append(f"🔴 TAO reserves shrank {tao_delta:.4f} TAO since last snapshot — selling pressure")

        price_prev = prev.get("price", price)
        price_chg  = ((price - price_prev) / price_prev * 100) if price_prev > 0 else 0
        if abs(price_chg) > 20:
            flags.append(f"🔴 Alpha price moved {price_chg:+.1f}% since last snapshot — high volatility")
    else:
        flags.append(f"🟢 First snapshot captured — longitudinal tracking begins today (Day {days_old})")

    for flag in flags:
        print(f"  {flag}")

    if not flags:
        print("  No flags — clean genesis state.")

    print(f"\n{SEPARATOR}\n")

    # ── Serialize ─────────────────────────────────────────────────────────────
    snapshot_data = {
        "subnet_name":              SUBNET_NAME,
        "netuid":                   NETUID,
        "genesis_date":             GENESIS_DATE,
        "days_since_genesis":       days_old,
        "owner_hotkey":             OWNER_HOTKEY,
        "displaced_subnet":         DISPLACED_SUBNET,
        "block":                    block,
        "timestamp":                now,
        "tao_reserves":             tao_in_pool,
        "alpha_reserves":           alpha_in_pool,
        "alpha_out":                alpha_out,
        "price":                    price,
        "moving_price":             moving_price,
        "momentum_pct":             momentum_pct,
        "genesis_momentum_clamped": genesis_momentum_flag,
        "emission_per_block":       emission_per_block,
        "emission_apy":             emission_apy,
        "price_apy":                price_apy,
        "combined_apy":             combined_apy,
        "n_uids":                   n_uids,
        "n_active":                 n_active,
        "n_validators":             n_validators,
        "total_stake":              total_stake,
        "uid_top1_pct":             uid_top1,
        "uid_top3_pct":             uid_top3,
        "uid_top5_pct":             uid_top5,
        "uid_concentration":        uid_concentration,
        "ck_top1_pct":              ck_top1,
        "ck_top3_pct":              ck_top3,
        "ck_top5_pct":              ck_top5,
        "ck_concentration":         ck_concentration,
        "owner_uid":                owner_uid,
        "owner_stake":              owner_stake,
        "owner_stake_pct":          owner_stake_pct,
        "yuma_aligned":             aligned_count,
        "yuma_partial":             partial_count,
        "yuma_misaligned":          misaligned_count,
        "validators":               validators,
        "top_stakers":              [{"uid": uid, "stake": s} for uid, s in stake_list[:15]],
    }

    return snapshot_data


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=f"SN{NETUID} {SUBNET_NAME} Genesis Snapshot")
    parser.add_argument("--stake",   type=float, default=STAKE_AMOUNT, help="TAO amount to simulate")
    parser.add_argument("--no-save", action="store_true",              help="Skip JSON export")
    args = parser.parse_args()

    data = run_snapshot(stake_amount=args.stake, save=not args.no_save)

    if not args.no_save and data:
        save_snapshot(data)
        update_trajectory(data)


if __name__ == "__main__":
    main()
