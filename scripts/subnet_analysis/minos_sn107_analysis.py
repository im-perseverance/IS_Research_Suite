"""
minos_sn107_analysis.py
=========================
SN107 Minos — Genesis Period Deep Dive Tracker
Part of the Intelligence Sovereignty research suite by @perseverance

Tracks the full embryonic lifecycle of SN107 Minos from genesis:
  - Reserve dynamics (TAO + Alpha) — pulled via all_subnets()
  - Alpha price vs. EMA momentum
  - Validator registry: Tv score, Yuma alignment, take rate, stake
  - UID-level & coldkey-level concentration (matching best_subnet.py logic)
  - Daily delta vs. previous snapshot (if available)

Recommended cadence: Daily at 12:00 UTC (or on-demand)
Output: Console + JSON snapshot saved to snapshots/minos/

Usage:
    python minos_sn107_analysis.py
    python minos_sn107_analysis.py --stake 1000
    python minos_sn107_analysis.py --no-save   (skip JSON export)
"""

import bittensor as bt
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────
NETUID            = 107
STAKE_AMOUNT      = 1000.0
SNAPSHOT_DIR      = Path("snapshots/minos")
PREVIOUS_FILE     = SNAPSHOT_DIR / "latest.json"
YUMA_TV_THRESHOLD = 0.80

SEPARATOR = "=" * 100
THIN_SEP  = "-" * 100

# ── Helpers ───────────────────────────────────────────────────────────────────

def classify_concentration(top1_pct: float, top5_pct: float) -> str:
    """Matches best_subnet.py thresholds exactly."""
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


def save_snapshot(data: dict):
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    with open(PREVIOUS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\n💾  Snapshot saved → {PREVIOUS_FILE}")


def get_coldkey_concentration(sub, meta, price: float) -> tuple:
    """
    Pull true staker-level (coldkey) concentration via delegate.nominators.
    Matches best_subnet.py get_subnet_concentration() logic exactly.
    """
    try:
        coldkey_stakes = {}
        for uid in range(len(meta.hotkeys)):
            hotkey = meta.hotkeys[uid]
            try:
                delegate = sub.get_delegate_by_hotkey(hotkey)
                if not delegate or not delegate.nominators:
                    continue
                for coldkey, subnet_stakes_dict in delegate.nominators.items():
                    alpha = float(subnet_stakes_dict.get(NETUID, 0))
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
    print(f"\n{SEPARATOR}")
    print(f"  SN107 MINOS — GENESIS DEEP DIVE SNAPSHOT")
    print(f"  Intelligence Sovereignty Research Suite | @perseverance")
    print(SEPARATOR)

    # ── Connect ───────────────────────────────────────────────────────────────
    print("\nConnecting to Bittensor network...")
    sub   = bt.Subtensor(network="finney")
    block = sub.get_current_block()
    now   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"Block: {block:,} | Timestamp: {now}")

    # ── Pool Data via all_subnets() ───────────────────────────────────────────
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

    # ── Metagraph ─────────────────────────────────────────────────────────────
    print(f"Loading SN{NETUID} metagraph...")
    meta = sub.metagraph(NETUID)

    n_uids       = len(meta.uids)
    active_uids  = [i for i, s in enumerate(meta.stake) if float(s) > 0]
    n_active     = len(active_uids)
    total_stake  = sum(float(s) for s in meta.stake)
    vali_uids    = [i for i, vp in enumerate(meta.validator_permit) if vp]
    n_validators = len(vali_uids)

    # ── UID-level Concentration ───────────────────────────────────────────────
    stake_list = sorted(
        [(i, float(meta.stake[i])) for i in active_uids],
        key=lambda x: x[1], reverse=True
    )
    uid_top1 = stake_list[0][1] / total_stake if stake_list and total_stake > 0 else 0
    uid_top3 = sum(s for _, s in stake_list[:3]) / total_stake if total_stake > 0 else 0
    uid_top5 = sum(s for _, s in stake_list[:5]) / total_stake if total_stake > 0 else 0
    uid_concentration = classify_concentration(uid_top1, uid_top5)

    # ── Coldkey-level Concentration ───────────────────────────────────────────
    print("Computing coldkey concentration (this may take a moment)...")
    ck_top1, ck_top3, ck_top5, ck_concentration = get_coldkey_concentration(sub, meta, price)

    # ── Validator Details ─────────────────────────────────────────────────────
    validators = []
    for uid in vali_uids:
        hotkey       = meta.hotkeys[uid] if hasattr(meta, 'hotkeys') else "unknown"
        hotkey_short = hotkey[:8] + "..." if len(hotkey) > 8 else hotkey
        stake_val    = float(meta.stake[uid])
        tv           = float(meta.validator_trust[uid]) if hasattr(meta, 'validator_trust') else 0.0
        dividends    = float(meta.dividends[uid]) if hasattr(meta, 'dividends') else 0.0
        incentive    = float(meta.incentive[uid])  if hasattr(meta, 'incentive')  else 0.0

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
        })

    validators.sort(key=lambda x: x["stake"], reverse=True)

    aligned_count    = sum(1 for v in validators if v["tv"] >= YUMA_TV_THRESHOLD)
    partial_count    = sum(1 for v in validators if 0.5 <= v["tv"] < YUMA_TV_THRESHOLD)
    misaligned_count = sum(1 for v in validators if v["tv"] < 0.5)

    # ── APY Calculation ───────────────────────────────────────────────────────
    blocks_per_year = 365 * 24 * 3600 / 12
    best_uid_data   = max(validators, key=lambda x: x["tv"] * x["stake_pct"]) if validators else None

    if total_stake > 0 and emission_per_block > 0:
        staker_share     = stake_amount / (total_stake + stake_amount)
        annual_tao_yield = staker_share * emission_per_block * blocks_per_year
        emission_apy     = (annual_tao_yield / stake_amount) * 100
    else:
        emission_apy = 0.0

    daily_momentum = momentum_pct / 30
    price_apy      = ((1 + daily_momentum / 100) ** 365 - 1) * 100
    combined_apy   = emission_apy + price_apy

    # ── Load previous for deltas ──────────────────────────────────────────────
    prev = load_previous()

    def delta_str(key, current, fmt=".2f"):
        if prev and key in prev and prev[key] != 0:
            return f"  Δ {abs_delta(current, prev[key], fmt)}"
        return ""

    # ── Print Report ──────────────────────────────────────────────────────────
    print(f"\n{SEPARATOR}")
    print(f"  POOL DYNAMICS")
    print(THIN_SEP)
    print(f"  TAO Reserves    : {tao_in_pool:>12,.4f} TAO{delta_str('tao_reserves', tao_in_pool)}")
    print(f"  Alpha Reserves  : {alpha_in_pool:>12,.2f} α  {delta_str('alpha_reserves', alpha_in_pool)}")
    print(f"  Alpha Circulating:{alpha_out:>12,.2f} α  {delta_str('alpha_out', alpha_out)}")
    print(f"  Alpha Price     : {price:>12.6f} TAO{delta_str('price', price, '.6f')}")
    print(f"  Moving Price    : {moving_price:>12.6f} TAO{delta_str('moving_price', moving_price, '.6f')}")
    print(f"  Momentum        : {momentum_str:>12}")
    print(f"  Emission/block  : {emission_per_block:>12.6f} TAO{delta_str('emission_per_block', emission_per_block, '.6f')}")

    print(f"\n{SEPARATOR}")
    print(f"  YIELD PROFILE  (simulated stake: {stake_amount:,.1f} TAO)")
    print(THIN_SEP)
    print(f"  Emission APY    : {emission_apy:>+10.4f}%")
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
    print(f"  VALIDATOR REGISTRY — Tv Score & Yuma Alignment")
    print(THIN_SEP)
    print(f"  {'UID':>4}  {'Hotkey':10}  {'Stake (α)':>14}  {'Stake%':>7}  {'Tv':>6}  {'Take':>6}  {'Alignment':14}  {'Dividends':>10}  {'Incentive':>10}")
    print(THIN_SEP)

    for v in validators:
        print(
            f"  {v['uid']:>4}  "
            f"{v['hotkey_short']:10}  "
            f"{v['stake']:>14,.2f}  "
            f"{v['stake_pct']*100:>6.1f}%  "
            f"{v['tv']:>6.4f}  "
            f"{v['take']:>5.1%}  "
            f"{v['alignment']:14}  "
            f"{v['dividends']:>10.6f}  "
            f"{v['incentive']:>10.6f}"
        )

    print(THIN_SEP)
    print(f"  Aligned ✅: {aligned_count}   Partial ⚠️: {partial_count}   Misaligned ❌: {misaligned_count}")

    print(f"\n{SEPARATOR}")
    print(f"  TOP STAKERS (UID-level, top 15)")
    print(THIN_SEP)
    print(f"  {'Rank':>4}  {'UID':>4}  {'Hotkey':10}  {'Stake (α)':>14}  {'Stake%':>7}  {'Is Validator':>12}")
    print(THIN_SEP)
    for rank, (uid, stake_val) in enumerate(stake_list[:15], 1):
        hk    = meta.hotkeys[uid] if hasattr(meta, 'hotkeys') else "unknown"
        hk_s  = hk[:8] + "..." if len(hk) > 8 else hk
        is_val = "✅ Yes" if uid in vali_uids else "  No"
        print(
            f"  {rank:>4}  "
            f"{uid:>4}  "
            f"{hk_s:10}  "
            f"{stake_val:>14,.2f}  "
            f"{stake_val/total_stake*100:>6.1f}%  "
            f"{is_val:>12}"
        )

    # ── Genesis Watch Flags ───────────────────────────────────────────────────
    print(f"\n{SEPARATOR}")
    print(f"  ⚠️  GENESIS WATCH FLAGS")
    print(THIN_SEP)

    flags = []

    if tao_in_pool < 500:
        flags.append(f"🔴 Ultra-thin reserve ({tao_in_pool:.4f} TAO) — price highly volatile on small flows")
    elif tao_in_pool < 2000:
        flags.append(f"🟡 Thin reserve ({tao_in_pool:.4f} TAO) — entering price discovery phase")
    else:
        flags.append(f"🟢 Reserve building ({tao_in_pool:.4f} TAO) — liquidity stabilizing")

    if abs(momentum_pct) > 5:
        flags.append(f"🔴 High momentum ({momentum_str}) — EMA lagging, elevated price risk")
    elif abs(momentum_pct) > 2:
        flags.append(f"🟡 Moderate momentum ({momentum_str}) — monitor for mean reversion")

    if misaligned_count > 0:
        flags.append(f"🔴 {misaligned_count} misaligned validator(s) — Yuma consensus risk")
    if aligned_count == 0:
        flags.append(f"🔴 No fully Yuma-aligned validators detected yet")

    if ck_top1 is not None and (ck_top1 > 0.50 or (ck_top5 or 0) > 0.70):
        flags.append(f"🔴 Coldkey concentration HIGH RISK — top-1 coldkey controls {ck_top1*100:.1f}%")
    elif uid_top1 > 0.50:
        flags.append(f"🔴 UID concentration HIGH RISK — top-1 UID controls {uid_top1*100:.1f}%")

    if n_validators < 5:
        flags.append(f"🟡 Only {n_validators} validators — subnet still bootstrapping")

    if prev:
        uid_growth = n_active - prev.get("n_active", n_active)
        if uid_growth > 0:
            flags.append(f"🟢 +{uid_growth} new active UID(s) since last snapshot — miners entering")
        elif uid_growth < 0:
            flags.append(f"🔴 {uid_growth} UID(s) exited since last snapshot — monitor for whale exit")

        tao_delta = tao_in_pool - prev.get("tao_reserves", tao_in_pool)
        if tao_delta > 0:
            flags.append(f"🟢 TAO reserves grew by +{tao_delta:.4f} TAO since last snapshot")
        elif tao_delta < 0:
            flags.append(f"🔴 TAO reserves shrank by {tao_delta:.4f} TAO since last snapshot")

    for flag in flags:
        print(f"  {flag}")

    if not flags:
        print("  No flags — clean genesis state.")

    print(f"\n{SEPARATOR}\n")

    # ── Serialize for JSON ────────────────────────────────────────────────────
    snapshot_data = {
        "block":              block,
        "timestamp":          now,
        "netuid":             NETUID,
        "tao_reserves":       tao_in_pool,
        "alpha_reserves":     alpha_in_pool,
        "alpha_out":          alpha_out,
        "price":              price,
        "moving_price":       moving_price,
        "momentum_pct":       momentum_pct,
        "emission_per_block": emission_per_block,
        "emission_apy":       emission_apy,
        "price_apy":          price_apy,
        "combined_apy":       combined_apy,
        "n_uids":             n_uids,
        "n_active":           n_active,
        "n_validators":       n_validators,
        "total_stake":        total_stake,
        "uid_top1_pct":       uid_top1,
        "uid_top3_pct":       uid_top3,
        "uid_top5_pct":       uid_top5,
        "uid_concentration":  uid_concentration,
        "ck_top1_pct":        ck_top1,
        "ck_top3_pct":        ck_top3,
        "ck_top5_pct":        ck_top5,
        "ck_concentration":   ck_concentration,
        "yuma_aligned":       aligned_count,
        "yuma_partial":       partial_count,
        "yuma_misaligned":    misaligned_count,
        "validators":         validators,
        "top_stakers":        [{"uid": uid, "stake": s} for uid, s in stake_list[:15]],
    }

    return snapshot_data


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SN107 Minos Genesis Snapshot")
    parser.add_argument("--stake",   type=float, default=STAKE_AMOUNT, help="TAO amount to simulate")
    parser.add_argument("--no-save", action="store_true",              help="Skip JSON export")
    args = parser.parse_args()

    data = run_snapshot(stake_amount=args.stake, save=not args.no_save)

    if not args.no_save and data:
        save_snapshot(data)


if __name__ == "__main__":
    main()
