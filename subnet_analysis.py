
"""
subnet_analysis.py
==========================
Subnet Staking Decision Tool — Intelligence Sovereignty Research Suite
@im_perseverance

For each active subnet, computes:

  Yield metrics:
    - Emission APY (annualised pool-level TAO yield proxy: TAO emission / TAO reserves)
    - Real APY = Emission APY - Net Supply Delta (yield after actual dilution)
    - Combined APY = Real APY + Price APY (dilution-adjusted yield + market momentum)
    - Price APY — 7-day momentum (spot vs 7d-ago snapshot, annualised projection)
    - Price APY — 30-day momentum (spot vs protocol moving_price EMA, annualised)

  Supply health (three-layer model):
    - Gross emission rate: (alpha_out_emission * blocks_per_year) / alpha_out.
        Protocol-native, EMA-smoothed (30-day half-life). Raw dilution pressure.
    - Net supply delta: annualised change in alpha_outstanding between snapshots.
        Captures actual dilution from all sources — emission, staking, unstaking.
    - Burned tokens: total_emission - actual_supply_change. Isolated from staking
        noise by tracking total alpha (alpha_out + alpha_in) vs total emission
        (alpha_out_emission + alpha_in_emission). Only emission creates tokens,
        only burns destroy them — staking/unstaking just moves between pools.
    - Supply defence: burned_tokens / gross_emission. Fraction of emission actively
        destroyed by subnet buyback/burn programs. >1.0 = deflating (burns exceed
        emission). Isolated from staking noise — measures economic management only.

  Capital flow signals:
    - EMA TAO inflow: protocol's smoothed net capital flow (30-day half-life).
        Positive = net staking, negative = net unstaking. Determines emission share.
    - Flow momentum: delta between current and previous EMA inflow. Shows whether
        capital flow is accelerating or decelerating.
    - Flow/price divergence: flags when price momentum contradicts flow momentum.
        PRICE_UP_FLOW_DOWN = latent emission cut (stakers leaving despite price rise).
        PRICE_DOWN_FLOW_UP = latent emission pump (stakers entering despite price drop).
    - Emission/price ratio (EPR): ema_tao_inflow / spot_price. When >1.0, protocol
        is mechanically forced to buy the subnet — protocol-guaranteed bid floor.

  Risk metrics:
    - Liquidation price: tao_reserves / alpha_outstanding. Floor price if deregistered.
    - Liquidation haircut: (liquidation_price - spot_price) / spot_price. Positive =
        liquidation pays premium (bargain), negative = capital loss at deregistration.
    - EMA lag trap flag: spot well below EMA = late-exit danger zone.

  Validator metrics:
    - Best validator by nominator APY (take-adjusted dividends)
    - Validator-miner incentive share (fraction of mining incentive to validator-miners)

Notes on methodology:
  - Price APYs are instantaneous momentum projections, not realised returns.
  - Emission APY uses TAO reserves as denominator — pool-level yield proxy.
  - Real APY uses net supply delta when available (actual dilution experienced),
    falling back to emission APY alone on first run.
  - Combined APY routes through real_apy, not raw emission_apy.
  - 7d price APY tolerates up to 3-day deviation from the 7d target window.

Outputs:
  - Console report ranked by combined APY (30d basis)
  - subnet_analysis/snapshots/staking_snapshot_YYYY-MM-DD.csv
  - subnet_analysis/metadata/staking_metadata_YYYY-MM-DD.json
  - subnet_analysis/trajectory_30d.json    (rolling 30-day window per subnet)
  - subnet_analysis/trajectory_90d.json    (rolling 90-day EMA-effective window)
  - subnet_analysis/trajectory_historical.json (weekly-compressed long-term archive)
  - subnet_analysis/trajectory_ecosystem.json  (ecosystem-level inflation benchmarks)

Usage:
    python subnet_analysis.py
"""

import bittensor as bt
import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

# ── Config ─────────────────────────────────────────────────────────────────
OUTPUT_DIR            = Path("subnet_analysis")
OUTPUT_METADATA_DIR   = OUTPUT_DIR / "metadata"
OUTPUT_SNAPSHOT_DIR   = OUTPUT_DIR / "snapshots"
BLOCKS_PER_DAY    = 7200
BLOCKS_PER_YEAR   = BLOCKS_PER_DAY * 365
MIN_TAO_EMISSION  = 0.0001
MIN_VALIDATOR_TAO = 1000.0
TRAJECTORY_30D    = 30
TRAJECTORY_90D    = 90
IMMUNITY_BLOCKS   = 7200 * 30 * 4  # 4-month immunity period (~4 months of blocks)
OWNER_INFLOW_THRESHOLD = 0.20     # 20% of daily TAO reserve growth
MIN_TOTAL_INFLOW_TAO   = 1.0      # Ignore days with <1 TAO total inflow
EMA_LAG_THRESHOLD = -0.15
MIN_ALPHA_FOR_HAIRCUT = 100.0  # Suppress liquidation haircut for near-empty subnets
SEPARATOR  = "=" * 130
THIN_SEP   = "-" * 130

# ── Helpers ────────────────────────────────────────────────────────────────

def safe_float(val, default=0.0):
    try:
        return float(val)
    except Exception:
        return default

def fmt_pct(val, decimals=2):
    if val is None:
        return "  N/A  "
    return f"{val*100:+.{decimals}f}%"

def fmt_apy(val):
    if val is None:
        return "   N/A   "
    return f"{val*100:+.1f}%"

def fmt_tao(val):
    if val is None:
        return "N/A"
    if abs(val) >= 1_000_000:
        return f"{val/1_000_000:.2f}M"
    if abs(val) >= 1_000:
        return f"{val/1_000:.1f}K"
    return f"{val:.2f}"

def load_json(path):
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)

# ── Supply inflation metrics ───────────────────────────────────────────────

def compute_gross_emission_rate(alpha_out_emission, alpha_out):
    """
    Protocol-native gross emission rate, annualised.

    Measures how fast new alpha tokens are being minted into circulating
    supply: (alpha_out_emission * blocks_per_year) / alpha_out.

    This is the raw dilution pressure before any buyback/burn defences.
    Inherits EMA smoothing from the emission allocation mechanism
    (30-day half-life, ~86.8 day window). No snapshot history needed.

    Returns annualised rate as a float, or None if data insufficient.
    """
    if alpha_out is None or alpha_out <= 0:
        return None
    if alpha_out_emission is None or alpha_out_emission <= 0:
        return 0.0
    return (alpha_out_emission * BLOCKS_PER_YEAR) / alpha_out

def compute_net_supply_delta(netuid, current_alpha_out, trajectory_90d, date_str,
                             current_emission_apy=None):
    """
    Net supply change between current and previous snapshot, annualised.

    Captures the actual dilution a holder experiences — gross emission minus
    any tokens removed from circulation via buybacks, burns, or unstaking
    outflows. This is the real dilution metric, but requires a previous
    snapshot to compute.

    Emission onset guard: if the previous snapshot had no active emission and
    the current one does, the first-day step-change in alpha_outstanding
    (from the initial emission mint) annualises into a meaningless extreme.
    Returns (None, None) on that boundary day — real APY falls back to
    emission APY alone, same as a first-run. Normalises within 2-3 snapshots.

    Returns (annualised_rate, days_gap) or (None, None) if no history.
    """
    key = str(netuid)
    history = trajectory_90d.get(key, [])
    if not history:
        return None, None
    # Exclude current date — trajectory_90d may already contain today's entry
    # from a prior run on the same day, which would give days_gap=0 → None.
    prior = [e for e in history if e.get("date") != date_str]
    if not prior:
        return None, None
    prev = prior[-1]
    prev_alpha = prev.get("alpha_outstanding")
    prev_date  = prev.get("date")
    if not prev_alpha or not prev_date or prev_alpha <= 0:
        return None, None
    try:
        prev_dt = datetime.strptime(prev_date, "%Y-%m-%d")
        curr_dt = datetime.strptime(date_str, "%Y-%m-%d")
        days_gap = (curr_dt - prev_dt).days
    except Exception:
        return None, None
    if days_gap <= 0:
        return None, None
    # Suppress NSD on emission onset day — the step-change in alpha_outstanding
    # from the first emission mint annualises into a meaningless extreme figure.
    # Treat it the same as a first-run: real APY falls back to emission APY alone.
    prev_emission_apy = prev.get("emission_apy")
    if current_emission_apy is not None and prev_emission_apy is None:
        return None, None
    delta_pct = (current_alpha_out - prev_alpha) / prev_alpha
    annualised = delta_pct * (365 / days_gap)
    return annualised, days_gap

def compute_burned_tokens(alpha_out, alpha_in, prev_alpha_out, prev_alpha_in,
                          alpha_out_emission, alpha_in_emission, days_gap):
    """
    Tokens permanently destroyed between two snapshots.

    Uses the total alpha accounting identity: staking/unstaking moves tokens
    between alpha_out (circulating) and alpha_in (pool) but does not change
    their sum. Only emission creates tokens, only burns destroy them.

        total_emission = (alpha_out_emission + alpha_in_emission) * blocks_per_day * days
        actual_change  = (alpha_out + alpha_in) - (prev_alpha_out + prev_alpha_in)
        burned         = total_emission - actual_change

    Returns (burned_token_count, total_emission) or (None, None) if insufficient data.
    """
    if any(v is None for v in [alpha_out, alpha_in, prev_alpha_out, prev_alpha_in,
                                alpha_out_emission, alpha_in_emission]):
        return None, None
    if days_gap is None or days_gap <= 0:
        return None, None
    total_emission   = (alpha_out_emission + alpha_in_emission) * BLOCKS_PER_DAY * days_gap
    total_alpha_now  = alpha_out + alpha_in
    total_alpha_prev = prev_alpha_out + prev_alpha_in
    actual_change    = total_alpha_now - total_alpha_prev
    burned = total_emission - actual_change
    if burned < 0:
        print(f"  ⚠️  Negative burned_tokens ({burned:,.0f}) — data inconsistency, clamping to 0")
    return max(0.0, burned), total_emission

def compute_supply_defence(burned_tokens, total_emission, protocol_absorption):
    """
    Fraction of total emission actively destroyed by subnet owner buyback/burn.

    Isolates manual burns from protocol absorption. Each block, the protocol
    creates alpha to match the TAO injection (tao_in_emission / alpha_price)
    to maintain price equilibrium. This protocol absorption is not owner effort.

    manual_burn    = max(0, burned_tokens - protocol_absorption)
    supply_defence = manual_burn / total_emission

      ~0.0  = no manual burns, subnet owner doing nothing
      ~0.5  = owner burning half of total emission
      >1.0  = owner burning more than total emission — aggressive deflation
      None  = insufficient data (first run)
    """
    if burned_tokens is None or total_emission is None or protocol_absorption is None:
        return None, None
    if total_emission <= 0:
        return None, None
    manual_burn = max(0.0, burned_tokens - protocol_absorption)
    return manual_burn / total_emission, manual_burn

# ── 7-day price APY proxy ──────────────────────────────────────────────────

def compute_7d_price_apy(netuid, current_price, trajectory_90d, date_str):
    """
    Approximate 7-day price momentum by comparing current spot price to the
    closest available snapshot within 3 days of the 7-day-ago target.

    Returns annualised projection (linear) or None if insufficient history.
    The actual period length (which may differ from exactly 7 days) is used
    for the annualisation denominator.
    """
    key = str(netuid)
    history = trajectory_90d.get(key, [])
    # Exclude current date for same reason as compute_net_supply_delta
    history = [e for e in history if e.get("date") != date_str]
    if len(history) < 1:
        return None
    try:
        curr_dt = datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None
    target_dt = curr_dt - timedelta(days=7)
    best = None
    best_diff = float("inf")
    for entry in history:
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
    period_days = max((curr_dt - datetime.strptime(best["date"], "%Y-%m-%d")).days, 1)
    period_return = (current_price - old_price) / old_price
    return period_return * (365 / period_days)

# ── Validator best APY ─────────────────────────────────────────────────────

def get_best_validator(meta, netuid, take_lookup):
    """
    Find the validator with the best take-adjusted nominator APY on a subnet.

    Uses take_lookup(hotkey) callable to retrieve cached delegate take rates,
    avoiding redundant RPC calls for hotkeys that appear across multiple
    subnets. Nominator APY = raw_apy * (1 - take).

    Args:
        meta:        Metagraph instance (synced) for the subnet.
        netuid:      Subnet UID (for context only, not used in computation).
        take_lookup: Callable(hotkey_ss58) -> float or None. Returns the
                     delegate take rate (0.0 to 1.0) or None if unknown.

    Returns:
        Dict with uid, hotkey_short, stake, take, div_pct, est_apy, raw_apy,
        trust. Or None if no qualifying validators found (stake >= MIN_VALIDATOR_TAO).
    """
    try:
        n_uids          = len(meta.uids)
        total_dividends = sum(safe_float(meta.dividends[i]) for i in range(n_uids))
        total_stake     = sum(safe_float(meta.stake[i]) for i in range(n_uids))
        vali_uids       = [i for i, vp in enumerate(meta.validator_permit) if vp]
        best     = None
        best_apy = -999

        for uid in vali_uids:
            stake = safe_float(meta.stake[uid])
            if stake < MIN_VALIDATOR_TAO:
                continue
            div       = safe_float(meta.dividends[uid])
            div_pct   = div / total_dividends if total_dividends > 0 else 0
            stake_pct = stake / total_stake   if total_stake     > 0 else 0
            if stake_pct <= 0:
                continue

            # meta.emission[uid] is the absolute emission per block for this UID (Balance object).
            # The .tao accessor converts from RAO (billionths) to whole units.
            # Post-dTAO, this is ALPHA per block, not TAO — but units are consistent
            # because meta.stake[uid] is also in alpha. The ratio gives per-block yield.
            # meta.dividends[uid] is a u16_normalized_float fraction (0-1), NOT absolute.
            uid_emission = safe_float(meta.emission[uid].tao)
            raw_apy = (uid_emission / stake) * BLOCKS_PER_YEAR if stake > 0 else 0

            hotkey   = meta.hotkeys[uid]
            take_val = take_lookup(hotkey)
            if take_val is not None:
                take = max(0.0, min(1.0, safe_float(take_val, default=0.0)))
            else:
                take = None

            est_apy = raw_apy * (1.0 - take) if take is not None else raw_apy

            if est_apy > best_apy:
                best_apy = est_apy
                best = {
                    "uid":          uid,
                    "hotkey_short": hotkey[:8] + "...",
                    "stake":        stake,
                    "take":         take,
                    "div_pct":      div_pct,
                    "est_apy":      est_apy,
                    "raw_apy":      raw_apy,
                    "trust":        safe_float(meta.validator_trust[uid]) if hasattr(meta, "validator_trust") else None,
                }
        return best
    except Exception:
        return None

# ── Validator-miner incentive share ────────────────────────────────────────

def get_vali_miner_share(meta):
    """
    Fraction of total mining incentive captured by UIDs that also hold
    validator permits (validator-miners). Returns a value between 0 and 1.

    This measures incentive concentration, not emission burn. Validators earn
    dividends from staking; this metric captures how much of the *mining*
    incentive goes to entities that are also validators, which can indicate
    vertical integration or self-dealing.

    A high value means validator-miners dominate the incentive distribution.
    A low value means independent miners capture most of the incentive.
    """
    try:
        n_uids          = len(meta.uids)
        vali_uids       = {i for i, vp in enumerate(meta.validator_permit) if vp}
        total_incentive = sum(safe_float(meta.incentive[i]) for i in range(n_uids))
        miner_incentive = sum(
            safe_float(meta.incentive[i])
            for i in range(n_uids)
            if i not in vali_uids
        )
        if total_incentive <= 0:
            return None
        return max(0.0, 1.0 - (miner_incentive / total_incentive))
    except Exception:
        return None

def get_miner_burn_rate(meta):
    """
    Fraction of miner emission allocation that is burned (not reaching miners).

    meta.incentive[uid] represents each UID's share of the 41% miner emission
    pool. If the subnet burns miner emissions, the sum across all UIDs drops
    below 1.0 — the gap is the burn fraction.

    Returns miner_burn_rate (0.0 = miners fully paid, 1.0 = 100% miner burn),
    or None if data insufficient.

    Note: if meta.incentive is post-burn normalized (always sums to 1.0),
    this function will return 0.0 for all subnets. In that case, the
    protocol_burn_frac formula falls back to 0.18 (owner cut only),
    and the remaining miner burn shows up in supply_defence as a
    conservative inclusion of miner burns in the manual_burn figure.
    """
    try:
        n_uids = len(meta.uids)
        total_incentive = sum(safe_float(meta.incentive[i]) for i in range(n_uids))
        miner_burn_rate = max(0.0, min(1.0, 1.0 - total_incentive))
        return miner_burn_rate
    except Exception:
        return None

# ── Main snapshot ──────────────────────────────────────────────────────────

def run_snapshot():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_METADATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    now      = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    ts_str   = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    traj_30d_path = OUTPUT_DIR / "trajectory_30d.json"
    traj_90d_path = OUTPUT_DIR / "trajectory_90d.json"
    traj_hist_path = OUTPUT_DIR / "trajectory_historical.json"
    traj_30d      = load_json(traj_30d_path)
    traj_90d      = load_json(traj_90d_path)

    print(SEPARATOR)
    print("  SUBNET STAKING SNAPSHOT — Intelligence Sovereignty Research Suite")
    print("  @im_perseverance")
    print(SEPARATOR)
    print(f"\n  Connecting to Bittensor network...")

    sub           = bt.Subtensor(network="finney")
    current_block = sub.get_current_block()
    all_subnets   = sub.all_subnets()

    print(f"  Block     : {current_block:,}")
    print(f"  Timestamp : {ts_str}")
    print(f"  Subnets   : {len(all_subnets)} total")

    # ── EMA TAO inflow (batch fetch) ──────────────────────────────────────
    print("  Fetching EMA TAO inflow (protocol-smoothed capital flows)...")
    try:
        ema_inflows = sub.get_all_ema_tao_inflow()
    except Exception:
        ema_inflows = {}
    print(f"  EMA inflow data : {len(ema_inflows)} subnets\n")

    # ── Delegate take cache ───────────────────────────────────────────────
    delegate_takes = {}
    _take_misses   = set()

    def get_cached_take(hotkey):
        """Fetch delegate take, caching results to avoid repeat RPC calls."""
        if hotkey in delegate_takes:
            return delegate_takes[hotkey]
        if hotkey in _take_misses:
            return None
        try:
            take = sub.get_delegate_take(hotkey)
            delegate_takes[hotkey] = take
            return take
        except Exception:
            _take_misses.add(hotkey)
            return None

    results = []
    _aged_out_buffer = {}  # {netuid_key: [entries]} — captured before 90d cutoff drops them

    for s in all_subnets:
        netuid = s.netuid
        if netuid == 0:
            continue
        tao_emission = safe_float(s.tao_in_emission)
        low_emission = tao_emission < MIN_TAO_EMISSION

        spot_price         = safe_float(s.price)
        moving_price       = safe_float(s.moving_price)
        tao_reserves       = safe_float(s.tao_in)
        alpha_out          = safe_float(s.alpha_out)
        alpha_out_emission = safe_float(s.alpha_out_emission)
        alpha_in_emission  = safe_float(s.alpha_in_emission)
        alpha_in_pool      = safe_float(s.alpha_in)
        volume             = safe_float(s.subnet_volume)
        name               = getattr(s, "subnet_name", f"SN{netuid}") or f"SN{netuid}"

        momentum_30d = (spot_price - moving_price) / moving_price if moving_price > 0 else None
        ema_lag_flag = momentum_30d is not None and momentum_30d < EMA_LAG_THRESHOLD

        emission_apy = None
        if tao_reserves > 0 and tao_emission > 0:
            emission_apy = (tao_emission * BLOCKS_PER_YEAR) / tao_reserves

        # Supply metrics: gross emission, net delta, burned tokens, defence ratio
        gross_emission_rate = compute_gross_emission_rate(alpha_out_emission, alpha_out)
        net_supply_delta, supply_days_gap = compute_net_supply_delta(netuid, alpha_out, traj_90d, date_str,
                                                                            current_emission_apy=emission_apy)

        # Burned tokens: isolated from staking noise via total alpha accounting.
        # Requires previous snapshot's alpha_in_pool stored in trajectory.
        burned_tokens        = None
        total_emission       = None
        supply_defence       = None
        manual_burn          = None
        protocol_absorption  = None
        key = str(netuid)
        prev_entries = [e for e in traj_90d.get(key, []) if e.get("date") != date_str]
        if prev_entries and supply_days_gap is not None:
            prev = prev_entries[-1]
            prev_alpha_in = prev.get("alpha_in_pool")
            prev_alpha_out = prev.get("alpha_outstanding")
            burned_tokens, total_emission = compute_burned_tokens(
                alpha_out, alpha_in_pool, prev_alpha_out, prev_alpha_in,
                alpha_out_emission, alpha_in_emission, supply_days_gap
            )

        # Liquidation price: implied alpha price if subnet is deregistered and
        # TAO reserves are distributed to alpha holders proportionally.
        # Haircut: percentage gain or loss vs current spot price at liquidation.
        #   Positive = liquidation pays you a premium (spot below floor — bargain)
        #   Negative = you lose capital at liquidation (spot above floor — deregistration risk)
        # Suppressed for near-empty subnets where tiny alpha_outstanding creates
        # misleadingly extreme haircut values.
        liquidation_price = tao_reserves / alpha_out if alpha_out > 0 else None
        liquidation_haircut = (
            (liquidation_price - spot_price) / spot_price
            if spot_price > 0 and liquidation_price is not None
            and alpha_out >= MIN_ALPHA_FOR_HAIRCUT
            else None
        )

        # EMA TAO inflow for this subnet
        ema_inflow_data = ema_inflows.get(netuid)
        ema_tao_inflow  = None
        if ema_inflow_data is not None:
            _, ema_balance = ema_inflow_data
            ema_tao_inflow = safe_float(ema_balance)

        # Price-adjusted reserve delta: isolates genuine capital inflow from AMM
        # rebalancing. The AMM's constant product means a rising spot price
        # mechanically inflates tao_reserves even with zero net staking. Dividing
        # reserves by spot normalises this out. When reserves/spot grows, real TAO
        # is entering independent of price. Leads EMA inflow by days to weeks.
        pool_growth_real = None
        pool_ema_divergence = None
        if prev_entries and spot_price > 0:
            prev_tao = prev_entries[-1].get("tao_reserves")
            prev_spot = prev_entries[-1].get("spot_price")
            if prev_tao and prev_spot and prev_spot > 0:
                pool_growth_real = (tao_reserves / spot_price) - (prev_tao / prev_spot)
                # Divergence: real capital entering while EMA still says outflow
                if pool_growth_real is not None and ema_tao_inflow is not None:
                    if pool_growth_real > 0 and ema_tao_inflow < 0:
                        pool_ema_divergence = "ACCUMULATING_PRE_EMA"
                    elif pool_growth_real < 0 and ema_tao_inflow > 0:
                        pool_ema_divergence = "DISTRIBUTING_PRE_EMA"

        # Flow momentum: delta between current and previous EMA inflow.
        # The protocol's EMA flow (30-day half-life) determines emission allocation.
        # Flow momentum shows whether capital flow is accelerating or decelerating.
        # Combined with price momentum, it detects dangerous divergences:
        #   PRICE_UP_FLOW_DOWN  = price rising but stakers leaving → latent emission cut
        #   PRICE_DOWN_FLOW_UP  = price falling but stakers entering → latent emission pump
        flow_momentum          = None
        flow_price_divergence  = None
        if prev_entries and ema_tao_inflow is not None:
            prev_ema_inflow = prev_entries[-1].get("ema_tao_inflow")
            if prev_ema_inflow is not None and prev_ema_inflow != 0:
                flow_momentum = (ema_tao_inflow - prev_ema_inflow) / abs(prev_ema_inflow)
                if momentum_30d is not None and flow_momentum is not None:
                    if momentum_30d > 0.05 and flow_momentum < -0.05:
                        flow_price_divergence = "PRICE_UP_FLOW_DOWN"
                    elif momentum_30d < -0.05 and flow_momentum > 0.05:
                        flow_price_divergence = "PRICE_DOWN_FLOW_UP"

        # Emission-to-price ratio: protocol-deterministic buy pressure signal.
        # When ratio > 1.0, emission exceeds price → chain is mechanically forced
        # to buy this subnet. Protocol-guaranteed bid, not sentiment-dependent.
        # When ratio < 1.0, price has run ahead of flow → no protocol floor.
        # Leading indicator of forced price convergence. Values well above 1.0
        # = strongest structural bid. Negative inflow = no bid regardless of price.
        #
        # emission_price_ratio_raw: computed whenever inflow is positive, regardless
        # of emission status. Used as a leading indicator for pre-emission subnets
        # approaching the emission boundary — tracks EPR build-up before the crossover.
        #
        # emission_price_ratio: gated on active emission. This is the protocol bid
        # signal used for ranking and the EPR leaderboard.
        emission_price_ratio_raw = (
            ema_tao_inflow / spot_price
            if (
                ema_tao_inflow is not None and
                ema_tao_inflow > 0 and
                spot_price > 0
            )
            else None
        )
        emission_price_ratio = emission_price_ratio_raw if not low_emission else None

        price_apy_7d  = compute_7d_price_apy(netuid, spot_price, traj_90d, date_str)

        price_apy_30d = None
        if moving_price > 0 and momentum_30d is not None:
            price_apy_30d = momentum_30d * (365 / 30)

        # Cascading real APY: use net supply delta when available (actual dilution
        # including buyback/burn defences), fall back to emission APY alone on first
        # run (no false precision from gross rate which ignores defences).
        if emission_apy is not None and net_supply_delta is not None:
            real_apy = emission_apy - net_supply_delta
        elif emission_apy is not None:
            real_apy = emission_apy
        else:
            real_apy = None

        # Combined APY = Real APY + Price APY.
        # Routes through real_apy (dilution-adjusted) not raw emission_apy.
        # Falls back to price_apy alone when emission is null (pre-emission subnets),
        # and returns None only when both components are unavailable.
        combined_7d  = (real_apy or 0) + (price_apy_7d  or 0) if (real_apy is not None or price_apy_7d  is not None) else None
        combined_30d = (real_apy or 0) + (price_apy_30d or 0) if (real_apy is not None or price_apy_30d is not None) else None

        best_validator   = None
        vali_miner_share = None
        miner_burn_rate  = None
        owner_hotkey     = getattr(s, "owner_hotkey", None)
        owner_stake      = None
        owner_inflow_pct = None
        owner_inflow_flag = None
        large_entry_anomaly = None
        registered_at    = getattr(s, "network_registered_at", None)
        immune            = (
            registered_at is not None and
            (current_block - registered_at) < IMMUNITY_BLOCKS
        )
        try:
            meta             = sub.metagraph(netuid)
            best_validator   = get_best_validator(meta, netuid, get_cached_take)
            vali_miner_share = get_vali_miner_share(meta)
            miner_burn_rate = get_miner_burn_rate(meta)

            # Owner stake: match owner_coldkey against metagraph coldkeys.
            # Subnet owners register with a coldkey. A single coldkey can control
            # multiple hotkeys (validators/miners) on the same subnet. Sum all
            # stake under the owner's coldkey to get their total exposure.
            owner_coldkey = getattr(s, "owner_coldkey", None)
            if owner_coldkey and hasattr(meta, 'coldkeys') and hasattr(meta, 'stake'):
                owner_total = 0.0
                for idx, ck in enumerate(meta.coldkeys):
                    if ck == owner_coldkey:
                        s_val = safe_float(meta.stake[idx])
                        if s_val:
                            owner_total += s_val
                if owner_total > 0:
                    owner_stake = owner_total
        except Exception:
            pass

        # ── Identity layer: owner inflow detection ────────────────────────
        # Compare current owner stake (alpha) against previous snapshot to
        # detect if owner is dominating inflow (>20% of total daily TAO growth).
        # Owner stake is in alpha — convert delta to TAO via AMM redemption rate
        # (tao_reserves / alpha_outstanding) for unit-consistent comparison.
        cutoff_7d = (now - timedelta(days=7)).strftime("%Y-%m-%d")

        if owner_stake is not None and prev_entries:
            prev_owner_stake = prev_entries[-1].get("owner_stake")
            prev_tao_rsv     = prev_entries[-1].get("tao_reserves")
            if prev_owner_stake is not None and prev_tao_rsv is not None and prev_tao_rsv > 0:
                owner_alpha_delta = max(0.0, owner_stake - prev_owner_stake)
                # Convert alpha delta to TAO equivalent via AMM rate
                owner_inflow_tao = (
                    owner_alpha_delta * tao_reserves / alpha_out
                    if alpha_out > 0 else 0.0
                )
                total_inflow = max(0.0, tao_reserves - prev_tao_rsv)
                if total_inflow > MIN_TOTAL_INFLOW_TAO:
                    owner_inflow_pct = owner_inflow_tao / total_inflow

                    if owner_inflow_pct > OWNER_INFLOW_THRESHOLD:
                        # Convert burned alpha to TAO via AMM redemption rate
                        burn_tao_equiv = (
                            burned_tokens * tao_reserves / alpha_out
                            if burned_tokens and alpha_out > 0 else 0.0
                        )
                        # Flag if owner inflow exceeds real burns by >10% of total
                        # inflow — net economic effect is inflationary regardless
                        # of whether any burn activity exists.
                        excess_pct = (owner_inflow_tao - burn_tao_equiv) / total_inflow
                        if excess_pct > 0.10:
                            owner_inflow_flag = "BUYBACK_GAMING"
                        else:
                            owner_inflow_flag = "BUYBACK_JUSTIFIED"

        # ── Behavioural layer: pool growth anomaly detection ──────────────
        # Catches large entries regardless of wallet identity. Compares today's
        # price-adjusted reserve growth against 7-day trailing average.
        # Only flags when multiple conditions align to filter honest large stakes.
        if pool_growth_real is not None and prev_entries:
            prev_7d = [e for e in prev_entries if e.get("date", "") >= cutoff_7d]
            if prev_7d:
                avg_daily_growth = sum(
                    e.get("pool_growth_real", 0) or 0 for e in prev_7d
                ) / len(prev_7d)
                growth_multiple = (
                    pool_growth_real / avg_daily_growth
                    if avg_daily_growth > 0 else None
                )

                if growth_multiple is not None and growth_multiple > 3.0 and pool_growth_real > 10.0:
                    # Large entry detected — check context to filter honest stakes

                    # Condition 1: price running hard same day (owner pumping spot)
                    price_spike = momentum_30d is not None and momentum_30d > 0.10

                    # Condition 2: EMA inflow still negative or near zero
                    ema_low = ema_tao_inflow is not None and ema_tao_inflow < 0.001

                    # Condition 3: repeated large entries in the last 7 days
                    prior_spikes = sum(
                        1 for e in prev_7d
                        if avg_daily_growth > 0 and
                        ((e.get("pool_growth_real") or 0) > 3 * avg_daily_growth)
                    )

                    if price_spike and ema_low:
                        large_entry_anomaly = "ENTRY_PRICE_PUMP"
                    elif prior_spikes >= 2:
                        large_entry_anomaly = "ENTRY_REPEATED"
                    elif ema_low and prior_spikes >= 1:
                        large_entry_anomaly = "ENTRY_PRE_EMA_PATTERN"

        # ── Supply defence ─────────────────────────────────────────────
        # Protocol absorption = pool injection (alpha_in_emission) + the portion
        # of alpha_out_emission that the protocol burns mechanically:
        #
        #   protocol_burn_fraction = 0.59 - 0.41 * miner_pay_frac
        #
        # Where miner_pay_frac = sum(meta.incentive) — the fraction of miner
        # emission that actually reaches miners (0.0 to 1.0).
        #
        # Derivation (Deepseek, verified):
        #   Validator share of alpha_out always distributed = 41%
        #   Miner share distributed = 41% * miner_pay_frac
        #   Total distributed = 0.41 * (1 + miner_pay_frac)
        #   Protocol burn = 1 - 0.41 * (1 + miner_pay_frac)
        #                 = 0.59 - 0.41 * miner_pay_frac
        #
        # Examples:
        #   miner_pay_frac=1.0 → protocol_burn=0.18 (owner cut only)
        #   miner_pay_frac=0.0 → protocol_burn=0.59 (owner cut + all miner emissions)
        #   miner_pay_frac=0.5 → protocol_burn=0.385
        #
        # This is purely mechanical — not a deliberate owner decision.
        # Everything above this threshold in burned_tokens is manual_burn.
        if burned_tokens is not None and supply_days_gap is not None and supply_days_gap > 0:
            # miner_pay_frac: 1 - miner_burn_rate (miner_burn_rate = fraction NOT reaching miners)
            mbr = miner_burn_rate if miner_burn_rate is not None else 0.0
            miner_pay_frac = max(0.0, min(1.0, 1.0 - mbr))
            protocol_burn_frac = max(0.0, min(1.0, 0.59 - 0.41 * miner_pay_frac))
            # Total protocol absorption = pool injection + mechanical alpha_out burns
            alpha_out_protocol_burn = (alpha_out_emission or 0) * protocol_burn_frac * BLOCKS_PER_DAY * supply_days_gap
            alpha_in_absorbed       = (alpha_in_emission  or 0) * BLOCKS_PER_DAY * supply_days_gap
            protocol_absorption     = alpha_out_protocol_burn + alpha_in_absorbed
            supply_defence, manual_burn = compute_supply_defence(burned_tokens, total_emission, protocol_absorption)

        # Quality of supply defence: is the owner putting skin in the game
        # (buyback/burn, forgoing owner cut) or just burning miner emissions (free)?
        supply_defence_quality = None
        if manual_burn and manual_burn > 0:
            if owner_inflow_flag in ("BUYBACK_JUSTIFIED", "BUYBACK_GAMING"):
                supply_defence_quality = "SKIN_IN_GAME"
            elif owner_inflow_pct is None or owner_inflow_pct < 0.05:
                supply_defence_quality = "MINER_BURN_ONLY"

        if momentum_30d is None:        ema_band = "N/A"
        elif momentum_30d >  0.20:      ema_band = "PREMIUM"
        elif momentum_30d < -0.20:      ema_band = "DISCOUNT"
        else:                           ema_band = "IN BAND"

        row = {
            "netuid":                 netuid,
            "name":                   name,
            "date":                   date_str,
            "block":                  current_block,
            "spot_price":             spot_price,
            "moving_price":           moving_price,
            "momentum_30d":           momentum_30d,
            "ema_band":               ema_band,
            "ema_lag_flag":           ema_lag_flag,
            "low_emission":           low_emission,
            "tao_emission":           tao_emission,
            "tao_reserves":           tao_reserves,
            "alpha_outstanding":      alpha_out,
            "alpha_out_emission":     alpha_out_emission,
            "alpha_in_pool":          alpha_in_pool,
            "volume":                 volume,
            "ema_tao_inflow":         ema_tao_inflow,
            "pool_growth_real":       pool_growth_real,
            "pool_ema_divergence":    pool_ema_divergence,
            "flow_momentum":          flow_momentum,
            "flow_price_divergence":  flow_price_divergence,
            "emission_price_ratio":   emission_price_ratio,
            "emission_price_ratio_raw": emission_price_ratio_raw,
            "emission_apy":           emission_apy,
            "price_apy_7d":           price_apy_7d,
            "price_apy_30d":          price_apy_30d,
            "combined_apy_7d":        combined_7d,
            "combined_apy_30d":       combined_30d,
            "real_apy":               real_apy,
            "gross_emission_rate":    gross_emission_rate,
            "net_supply_delta":       net_supply_delta,
            "supply_days_gap":        supply_days_gap,
            "supply_defence":         supply_defence,
            "supply_defence_quality": supply_defence_quality,
            "burned_tokens":          burned_tokens,
            "manual_burn":            manual_burn,
            "miner_burn_rate":        miner_burn_rate,
            "liquidation_price":      liquidation_price,
            "liquidation_haircut":    liquidation_haircut,
            "owner_coldkey":          owner_coldkey[:8] + "..." if owner_coldkey else None,
            "owner_stake":            owner_stake,
            "owner_inflow_pct":       owner_inflow_pct,
            "owner_inflow_flag":      owner_inflow_flag,
            "large_entry_anomaly":    large_entry_anomaly,
            "immune":                 immune,
            "registered_at":          registered_at,
            "vali_miner_share":       vali_miner_share,
            "best_validator_uid":     best_validator["uid"]          if best_validator else None,
            "best_validator_hotkey":  best_validator["hotkey_short"] if best_validator else None,
            "best_validator_take":    best_validator["take"]         if best_validator else None,
            "best_validator_raw_apy": best_validator["raw_apy"]      if best_validator else None,
            "best_validator_apy":     best_validator["est_apy"]      if best_validator else None,
            "best_validator_div_pct": best_validator["div_pct"]      if best_validator else None,
            "best_validator_trust":   best_validator["trust"]        if best_validator else None,
        }
        results.append(row)

        traj_entry = {
            "date":              date_str,
            "block":             current_block,
            "spot_price":        spot_price,
            "moving_price":      moving_price,
            "alpha_outstanding": alpha_out,
            "alpha_in_pool":     alpha_in_pool,
            "alpha_out_emission": alpha_out_emission,
            "alpha_in_emission": alpha_in_emission,
            "tao_reserves":      tao_reserves,
            "emission_apy":      emission_apy,
            "combined_apy_30d":  combined_30d,
            "real_apy":           real_apy,
            "gross_emission_rate":gross_emission_rate,
            "net_supply_delta":  net_supply_delta,
            "supply_defence":    supply_defence,
            "supply_defence_quality": supply_defence_quality,
            "burned_tokens":     burned_tokens,
            "manual_burn":       manual_burn,
            "miner_burn_rate":   miner_burn_rate,
            "protocol_absorption": protocol_absorption,
            "liquidation_price": liquidation_price,
            "liquidation_haircut": liquidation_haircut,
            "owner_stake":       owner_stake,
            "owner_inflow_pct":  owner_inflow_pct,
            "owner_inflow_flag": owner_inflow_flag,
            "large_entry_anomaly": large_entry_anomaly,
            "ema_tao_inflow":    ema_tao_inflow,
            "pool_growth_real":  pool_growth_real,
            "flow_momentum":     flow_momentum,
            "emission_price_ratio": emission_price_ratio,
            "emission_price_ratio_raw": emission_price_ratio_raw,
            "vali_miner_share":  vali_miner_share,
        }
        key = str(netuid)

        # Update trajectory_90d (EMA-effective window)
        all_hist = traj_90d.get(key, [])
        all_hist = [e for e in all_hist if e.get("date") != date_str]
        all_hist.append(traj_entry)
        all_hist.sort(key=lambda e: e.get("date", ""))
        traj_90d[key] = all_hist

        cutoff_30 = (now - timedelta(days=TRAJECTORY_30D)).strftime("%Y-%m-%d")
        hist_30 = traj_30d.get(key, [])
        hist_30 = [e for e in hist_30 if e.get("date", "") >= cutoff_30 and e.get("date") != date_str]
        hist_30.append(traj_entry)
        hist_30.sort(key=lambda e: e.get("date", ""))
        traj_30d[key] = hist_30

        # Enforce 90-day window on traj_90d (EMA-effective window)
        # Capture aged-out entries before dropping — they'll be compressed into historical
        cutoff_90 = (now - timedelta(days=TRAJECTORY_90D)).strftime("%Y-%m-%d")
        aged_out = [e for e in traj_90d[key] if e.get("date", "") < cutoff_90]
        if aged_out:
            _aged_out_buffer.setdefault(key, []).extend(aged_out)
        traj_90d[key] = [e for e in traj_90d[key] if e.get("date", "") >= cutoff_90]

    # Sort + rank
    results.sort(key=lambda r: r["combined_apy_30d"] if r["combined_apy_30d"] is not None else -999, reverse=True)
    for i, r in enumerate(results):
        r["combined_apy_rank"] = i + 1
    for i, r in enumerate(sorted(results, key=lambda r: r["real_apy"] if r["real_apy"] is not None else -999, reverse=True)):
        r["real_apy_rank"] = i + 1
    for i, r in enumerate(sorted(results, key=lambda r: r["emission_price_ratio"] if r["emission_price_ratio"] is not None else -999, reverse=True)):
        r["epr_rank"] = i + 1
    for i, r in enumerate(sorted(results, key=lambda r: r["tao_emission"], reverse=True)):
        r["emission_rank"] = i + 1

    # Ecosystem inflation stats (based on net supply delta — actual dilution)
    inflation_values = sorted([r["net_supply_delta"] for r in results if r["net_supply_delta"] is not None])
    if inflation_values:
        eco_inflation_median = inflation_values[len(inflation_values) // 2]
        eco_inflation_mean   = sum(inflation_values) / len(inflation_values)
        eco_inflation_min    = inflation_values[0]
        eco_inflation_max    = inflation_values[-1]
        eco_inflation_count  = len(inflation_values)
    else:
        eco_inflation_median = eco_inflation_mean = eco_inflation_min = eco_inflation_max = None
        eco_inflation_count  = 0

    # Ecosystem supply defence stats
    defence_values = sorted([r["supply_defence"] for r in results if r["supply_defence"] is not None])
    if defence_values:
        eco_defence_median = defence_values[len(defence_values) // 2]
        eco_defence_mean   = sum(defence_values) / len(defence_values)
        eco_defence_count  = len(defence_values)
    else:
        eco_defence_median = eco_defence_mean = None
        eco_defence_count  = 0

    eco_burned_total = sum(r["burned_tokens"] for r in results if r["burned_tokens"] is not None)

    # Split into active and low-emission
    active_results       = [r for r in results if not r["low_emission"]]
    low_emission_results = [r for r in results if r["low_emission"]]

    # Console — Active subnets
    print(SEPARATOR)
    print("  SUBNET STAKING RANKINGS — Sorted by Combined APY (30d basis)")
    print("  Combined vs Real vs Emission rank divergence highlights EMA lag traps and dilution")
    print(THIN_SEP)
    print(f"  {'C#':<4} {'R#':<4} {'Em#':<5} {'EPR#':<5} {'SN':<6} {'Name':<22} {'Emiss APY':>10} {'Real APY':>10} "
          f"{'Price 7d':>10} {'Price 30d':>10} {'Comb 7d':>10} {'Comb 30d':>10} "
          f"{'Gross Em':>10} {'Net Delta':>10} {'Defence':>8} {'Liq':>5} {'EP Ratio':>9} {'EMA Band':>10} {'Best Validator'}")
    print(THIN_SEP)

    for r in active_results:
        bv      = r["best_validator_hotkey"] or "N/A"
        bv_apy  = f"{r['best_validator_apy']*100:.1f}%"  if r["best_validator_apy"]  is not None else "N/A"
        bv_take = f"{r['best_validator_take']*100:.0f}%" if r.get("best_validator_take") is not None else "?%"
        bv_tv   = f"{r['best_validator_trust']:.2f}"     if r.get("best_validator_trust") is not None else " N/A"
        ge_str  = fmt_pct(r["gross_emission_rate"]) if r["gross_emission_rate"] is not None else "  N/A  "
        nd_str  = fmt_pct(r["net_supply_delta"])    if r["net_supply_delta"]    is not None else "  N/A  "
        sd_str  = fmt_pct(r["supply_defence"])      if r["supply_defence"]      is not None else "  N/A"
        sdq     = r.get("supply_defence_quality")
        sdq_icon = "🟢" if sdq == "SKIN_IN_GAME" else "🟡" if sdq == "MINER_BURN_ONLY" else "  "
        rank_div  = r["emission_rank"] - r["combined_apy_rank"]
        rank_flag = f"↑{rank_div}" if rank_div >= 5 else (f"↓{abs(rank_div)}" if rank_div <= -5 else "~")
        band_str  = f"⚠️{r['ema_band']}" if r["ema_lag_flag"] else r.get("ema_band", "N/A")
        epr_str  = f"{r['emission_price_ratio']:.3f}" if r["emission_price_ratio"] is not None else "  N/A "
        epr_flag = "🟢" if (r["emission_price_ratio"] is not None and r["emission_price_ratio"] > 1.0) else "  "
        lh = r.get("liquidation_haircut")
        liq_flag = "🟢" if lh is not None and lh > 0.1 else "🔴" if lh is not None and lh < -0.7 else "  "
        print(
            f"  #{r['combined_apy_rank']:<3} #{r['real_apy_rank']:<3} #{r['emission_rank']:<4} #{r['epr_rank']:<4} SN{r['netuid']:<4} {r['name']:<22} "
            f"{fmt_apy(r['emission_apy']):>10} {fmt_apy(r['real_apy']):>10} "
            f"{fmt_apy(r['price_apy_7d']):>10} {fmt_apy(r['price_apy_30d']):>10} "
            f"{fmt_apy(r['combined_apy_7d']):>10} {fmt_apy(r['combined_apy_30d']):>10} "
            f"{ge_str:>10} {nd_str:>10} {sd_str:>8}{sdq_icon} {liq_flag:>4} "
            f"{epr_flag}{epr_str:>8} {band_str:>10}  {bv_tv:>6}  {bv} ({bv_apy} | T:{bv_take})  {rank_flag}"
        )

    # Console — Low-emission subnets
    if low_emission_results:
        print(f"\n{THIN_SEP}")
        print(f"  LOW EMISSION SUBNETS (below {MIN_TAO_EMISSION} TAO/block) — {len(low_emission_results)} subnets")
        print(THIN_SEP)
        print(f"  {'SN':<6} {'Name':<22} {'Emission':>12} {'Reserves':>12} {'Alpha Out':>14} {'Spot Price':>12} {'EMA Band':>10}")
        print(THIN_SEP)
        for r in sorted(low_emission_results, key=lambda r: r["tao_emission"], reverse=True):
            print(
                f"  SN{r['netuid']:<4} {r['name']:<22} "
                f"{r['tao_emission']:.6f}     "
                f"{fmt_tao(r['tao_reserves']):>12} "
                f"{fmt_tao(r['alpha_outstanding']):>14} "
                f"{r['spot_price']:.8f}   "
                f"{r.get('ema_band', 'N/A'):>10}"
            )

    print(SEPARATOR)
    print(f"  Total subnets analysed : {len(results)} ({len(active_results)} active, {len(low_emission_results)} low emission)")
    print(f"  EMA lag traps detected : {sum(1 for r in results if r['ema_lag_flag'])}")
    div_up   = sum(1 for r in results if r.get("flow_price_divergence") == "PRICE_UP_FLOW_DOWN")
    div_down = sum(1 for r in results if r.get("flow_price_divergence") == "PRICE_DOWN_FLOW_UP")
    print(f"  Flow/price divergences : {div_up} price↑/flow↓, {div_down} price↓/flow↑")
    mb_full = sum(1 for r in results if r.get("miner_burn_rate") is not None and r["miner_burn_rate"] >= 0.95)
    print(f"  Miner burn (100%)      : {mb_full} subnets")
    print(f"  Delegate take cache    : {len(delegate_takes)} hits, {len(_take_misses)} misses")
    if eco_inflation_median is not None:
        print(f"  Ecosystem inflation    : median {eco_inflation_median*100:.1f}%/yr ({eco_inflation_median*100/12:.1f}%/mo)  "
              f"mean {eco_inflation_mean*100:.1f}%/yr ({eco_inflation_mean*100/12:.1f}%/mo)  "
              f"(n={eco_inflation_count})")

    # EPR Leaderboard — protocol-bid subnets ranked by emission_price_ratio
    epr_candidates = sorted(
        [r for r in active_results if r["emission_price_ratio"] is not None and r["emission_price_ratio"] > 1.0],
        key=lambda r: r["emission_price_ratio"],
        reverse=True
    )
    if epr_candidates:
        print(f"\n  🟢  PROTOCOL BID ACTIVE — Emission/Price Ratio > 1.0 (chain is forced buyer)")
        print(THIN_SEP)
        print(f"  {'EPR#':<5} {'SN':<6} {'Name':<22} {'EP Ratio':>10} {'EMA Inflow':>12} {'Spot Price':>12} {'Emiss APY':>10} {'Real APY':>10} {'Defence':>8}")
        print(THIN_SEP)
        for r in epr_candidates:
            print(
                f"  #{r['epr_rank']:<4} SN{r['netuid']:<4} {r['name']:<22} "
                f"{r['emission_price_ratio']:>10.3f} "
                f"{fmt_tao(r['ema_tao_inflow']):>12} "
                f"{r['spot_price']:>12.6f} "
                f"{fmt_apy(r['emission_apy']):>10} "
                f"{fmt_apy(r['real_apy']):>10} "
                f"{fmt_pct(r['supply_defence']) if r['supply_defence'] is not None else '  N/A':>8}"
            )

    lag_subnets = [r for r in results if r["ema_lag_flag"]]
    if lag_subnets:
        print(f"\n  ⚠️  EMA LAG TRAP WARNINGS")
        print(THIN_SEP)
        for r in lag_subnets:
            print(f"  SN{r['netuid']} {r['name']:<22} | Spot: {r['spot_price']:.6f} | "
                  f"EMA: {r['moving_price']:.6f} | Momentum: {fmt_pct(r['momentum_30d'])} | Band: {r['ema_band']}")

    flow_divs = [r for r in results if r.get("flow_price_divergence")]
    if flow_divs:
        print(f"\n  ⚠️  FLOW/PRICE DIVERGENCE — Capital flow contradicts price momentum")
        print(THIN_SEP)
        for r in flow_divs:
            div_type = r["flow_price_divergence"]
            if div_type == "PRICE_UP_FLOW_DOWN":
                label = "Price ↑ Flow ↓ — latent emission cut coming"
            else:
                label = "Price ↓ Flow ↑ — latent emission pump coming"
            print(f"  SN{r['netuid']} {r['name']:<22} | {label}")
            print(f"       Price momentum: {fmt_pct(r['momentum_30d'])} | "
                  f"Flow momentum: {fmt_pct(r['flow_momentum'])} | "
                  f"EMA inflow: {r['ema_tao_inflow']}")

    pool_divs = [r for r in results if r.get("pool_ema_divergence")]
    if pool_divs:
        print(f"\n  🔍  POOL/EMA DIVERGENCE — Price-adjusted reserves contradict EMA flow")
        print(THIN_SEP)
        for r in pool_divs:
            div_type = r["pool_ema_divergence"]
            if div_type == "ACCUMULATING_PRE_EMA":
                label = "Real capital entering while EMA still negative — early accumulation"
            else:
                label = "Real capital leaving while EMA still positive — early distribution"
            print(f"  SN{r['netuid']} {r['name']:<22} | {label}")
            print(f"       Pool growth (adj): {r['pool_growth_real']:+.4f} | "
                  f"EMA inflow: {r['ema_tao_inflow']}")

    # Miner burn warnings — subnets burning 100% of miner emissions
    full_miner_burn = [r for r in active_results if r.get("miner_burn_rate") is not None and r["miner_burn_rate"] >= 0.95]
    if full_miner_burn:
        print(f"\n  ⚠️  MINER BURN — {len(full_miner_burn)} subnets at 100% miner burn")
        print(THIN_SEP)
        for r in sorted(full_miner_burn, key=lambda x: x["netuid"]):
            print(f"  🔴 SN{r['netuid']:<4} {r['name']:<22} | 100% miner burn — no incentive to produce work")

    # Deregistration risk — bottom 10 by EMA price, outside immunity period
    # Subnets on this list with liquidation haircut ≤ -10% get a red flag
    dereg_candidates = sorted(
        [r for r in results if not r.get("immune", True) and r["moving_price"] is not None and r["moving_price"] > 0],
        key=lambda r: r["moving_price"]
    )
    if dereg_candidates[:10]:
        print(f"\n  💀  DEREGISTRATION RISK — Lowest EMA price (outside 4-month immunity)")
        print(THIN_SEP)
        print(f"  {'':>3} {'SN':<6} {'Name':<22} {'EMA Price':>12} {'Spot Price':>12} {'Reserves':>12} {'Liq Haircut':>12} {'Emission':>10}")
        print(THIN_SEP)
        for r in dereg_candidates[:10]:
            lh = r.get("liquidation_haircut")
            lh_str = f"{lh*100:+.1f}%" if lh is not None else "N/A"
            dereg_liq_flag = "🔴" if lh is not None and lh <= -0.10 else "  "
            print(
                f"  {dereg_liq_flag} SN{r['netuid']:<4} {r['name']:<22} "
                f"{r['moving_price']:>12.8f} "
                f"{r['spot_price']:>12.8f} "
                f"{fmt_tao(r['tao_reserves']):>12} "
                f"{lh_str:>12} "
                f"{r['tao_emission']:>10.6f}"
            )

    # ── Anomaly Summary ───────────────────────────────────────────────────
    # Consolidated block for investigation-level alerts: owner inflow
    # detection and large entry anomaly patterns.
    owner_flags  = [r for r in results if r.get("owner_inflow_flag") == "BUYBACK_GAMING"]
    genuine_bb   = [r for r in results if r.get("owner_inflow_flag") == "BUYBACK_JUSTIFIED"]
    entry_anom   = [r for r in results if r.get("large_entry_anomaly")]

    if any([owner_flags, genuine_bb, entry_anom]):
        print(f"\n{SEPARATOR}")
        print(f"  🔎  ANOMALY SUMMARY")
        print(SEPARATOR)

        if owner_flags:
            print(f"\n  🔴  BUYBACK GAMING — Owner inflow >{OWNER_INFLOW_THRESHOLD*100:.0f}% of pool growth, exceeds burns by >10%")
            print(THIN_SEP)
            for r in owner_flags:
                pct  = f"{r['owner_inflow_pct']*100:.0f}%" if r.get("owner_inflow_pct") is not None else "?"
                print(f"  🔴 SN{r['netuid']} {r['name']:<22} | Owner is {pct} of inflow, net inflationary after burns")

        if genuine_bb:
            print(f"\n  ✅  BUYBACK JUSTIFIED — Owner inflow >{OWNER_INFLOW_THRESHOLD*100:.0f}% but burns cover excess within 10%")
            print(THIN_SEP)
            for r in genuine_bb:
                pct = f"{r['owner_inflow_pct']*100:.0f}%" if r.get("owner_inflow_pct") is not None else "?"
                print(f"  ✅ SN{r['netuid']} {r['name']:<22} | Owner is {pct} of inflow, burns justify it")

        if entry_anom:
            print(f"\n  LARGE ENTRY ANOMALY — Unusual pool growth pattern")
            print(THIN_SEP)
            for r in entry_anom:
                anomaly = r["large_entry_anomaly"]
                if anomaly == "ENTRY_PRICE_PUMP":
                    icon = "🔴"
                    label = "Large entry + price spike + EMA low — possible pump"
                elif anomaly == "ENTRY_REPEATED":
                    icon = "🟡"
                    label = "Repeated large entries in 7 days — pattern"
                else:
                    icon = "🟡"
                    label = "Large entry pre-EMA with prior spike — building position"
                print(f"  {icon} SN{r['netuid']} {r['name']:<22} | {label}")
                pgr = r.get("pool_growth_real")
                print(f"       Pool growth (adj): {pgr:+.4f} | "
                      f"EMA inflow: {r['ema_tao_inflow']}")

    # CSV
    csv_path   = OUTPUT_SNAPSHOT_DIR / f"staking_snapshot_{date_str}.csv"
    fieldnames = [
        "combined_apy_rank", "real_apy_rank", "emission_rank", "epr_rank", "netuid", "name", "date", "block",
        "spot_price", "moving_price", "momentum_30d", "ema_band", "ema_lag_flag", "low_emission",
        "tao_emission", "tao_reserves", "alpha_outstanding", "alpha_out_emission",
        "alpha_in_pool", "volume", "ema_tao_inflow", "pool_growth_real", "pool_ema_divergence",
        "flow_momentum",
        "flow_price_divergence", "emission_price_ratio", "emission_price_ratio_raw",
        "emission_apy", "price_apy_7d", "price_apy_30d",
        "combined_apy_7d", "combined_apy_30d", "real_apy",
        "gross_emission_rate", "net_supply_delta", "supply_days_gap",
        "supply_defence", "supply_defence_quality", "burned_tokens", "manual_burn", "miner_burn_rate", "liquidation_price", "liquidation_haircut",
        "owner_coldkey", "owner_stake", "owner_inflow_pct", "owner_inflow_flag",
        "large_entry_anomaly", "immune", "registered_at", "vali_miner_share",
        "best_validator_uid", "best_validator_hotkey", "best_validator_take",
        "best_validator_raw_apy", "best_validator_apy",
        "best_validator_div_pct", "best_validator_trust",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    # JSON metadata
    meta_path = OUTPUT_METADATA_DIR / f"staking_metadata_{date_str}.json"
    save_json(meta_path, {
        "date":             date_str,
        "timestamp":        ts_str,
        "block":            current_block,
        "subnets_analysed": len(results),
        "subnets_active":   len([r for r in results if not r["low_emission"]]),
        "subnets_low_emission": len([r for r in results if r["low_emission"]]),
        "ema_lag_traps":    [r["netuid"] for r in results if r["ema_lag_flag"]],
        "ecosystem_inflation": {
            "median_annual":  eco_inflation_median,
            "mean_annual":    eco_inflation_mean,
            "min_annual":     eco_inflation_min,
            "max_annual":     eco_inflation_max,
            "median_monthly": eco_inflation_median / 12 if eco_inflation_median is not None else None,
            "mean_monthly":   eco_inflation_mean / 12   if eco_inflation_mean   is not None else None,
            "count":          eco_inflation_count,
        },
        "ecosystem_supply_defence": {
            "median":       eco_defence_median,
            "mean":         eco_defence_mean,
            "count":        eco_defence_count,
            "total_burned": eco_burned_total,
        },
        "delegate_take_cache": {"hits": len(delegate_takes), "misses": len(_take_misses)},
        "top_10_by_combined_apy_30d": [
            {
                "combined_apy_rank":  r["combined_apy_rank"],
                "real_apy_rank":      r["real_apy_rank"],
                "emission_rank":      r["emission_rank"],
                "netuid":             r["netuid"],
                "name":               r["name"],
                "combined_apy_30d":   r["combined_apy_30d"],
                "real_apy":           r["real_apy"],
                "emission_apy":       r["emission_apy"],
                "price_apy_30d":      r["price_apy_30d"],
                "volume":             r["volume"],
                "supply_inflation":   r["net_supply_delta"],
                "gross_emission_rate":r["gross_emission_rate"],
                "net_supply_delta":  r["net_supply_delta"],
                "supply_defence":    r["supply_defence"],
                "burned_tokens":     r["burned_tokens"],
                "liquidation_price": r["liquidation_price"],
                "liquidation_haircut": r["liquidation_haircut"],
                "ema_tao_inflow":     r["ema_tao_inflow"],
                "emission_price_ratio": r["emission_price_ratio"],
                "epr_rank":           r["epr_rank"],
                "vali_miner_share":   r["vali_miner_share"],
                "ema_band":           r["ema_band"],
                "ema_lag_flag":       r["ema_lag_flag"],
                "best_validator":     r["best_validator_hotkey"],
                "best_validator_apy": r["best_validator_apy"],
                "best_validator_take":r["best_validator_take"],
                "best_validator_trust":r["best_validator_trust"],
            }
            for r in results[:10]
        ],
    })

    save_json(traj_30d_path, traj_30d)
    save_json(traj_90d_path, traj_90d)

    # ── Historical compression ────────────────────────────────────────────
    # Entries that aged out of the 90-day window get compressed into weekly
    # averages in trajectory_historical.json. One entry per subnet per ISO week.
    # Keeps the file bounded while preserving long-term trend data.
    traj_hist = load_json(traj_hist_path)
    if not isinstance(traj_hist, dict):
        traj_hist = {}

    cutoff_90 = (now - timedelta(days=TRAJECTORY_90D)).strftime("%Y-%m-%d")
    NUMERIC_FIELDS = [
        "spot_price", "moving_price", "alpha_outstanding", "alpha_in_pool",
        "tao_reserves", "emission_apy", "combined_apy_30d", "real_apy",
        "gross_emission_rate", "net_supply_delta", "supply_defence",
        "burned_tokens", "manual_burn", "liquidation_price", "liquidation_haircut",
        "ema_tao_inflow", "pool_growth_real", "flow_momentum", "emission_price_ratio",
        "vali_miner_share",
    ]

    # Compress aged-out entries from 90d window into weekly averages
    for source_data in [_aged_out_buffer]:
        if not source_data:
            continue
        for sn_key, entries in source_data.items():
            aged_out = [e for e in entries if e.get("date", "") < cutoff_90]
            if not aged_out:
                continue
            # Group by ISO week
            weeks = defaultdict(list)
            for e in aged_out:
                try:
                    dt = datetime.strptime(e["date"], "%Y-%m-%d")
                    week_key = dt.strftime("%G-W%V")  # ISO year-week
                except Exception:
                    continue
                weeks[week_key].append(e)

            hist_subnet = traj_hist.get(sn_key, [])
            existing_weeks = {e.get("week") for e in hist_subnet}

            for week_key, week_entries in weeks.items():
                if week_key in existing_weeks:
                    continue
                compressed = {"week": week_key, "entries": len(week_entries)}
                compressed["date_start"] = min(e["date"] for e in week_entries)
                compressed["date_end"]   = max(e["date"] for e in week_entries)
                for field in NUMERIC_FIELDS:
                    vals = [e[field] for e in week_entries if e.get(field) is not None]
                    compressed[field] = round(sum(vals) / len(vals), 6) if vals else None
                hist_subnet.append(compressed)

            hist_subnet.sort(key=lambda e: e.get("week", ""))
            traj_hist[sn_key] = hist_subnet

    save_json(traj_hist_path, traj_hist)

    # Ecosystem-level trajectory (inflation + defence benchmarks over time)
    eco_traj_path = OUTPUT_DIR / "trajectory_ecosystem.json"
    eco_traj = load_json(eco_traj_path)
    if not isinstance(eco_traj, list):
        eco_traj = []
    eco_entry = {
        "date":              date_str,
        "block":             current_block,
        "inflation_median_annual":  eco_inflation_median,
        "inflation_mean_annual":    eco_inflation_mean,
        "inflation_median_monthly": eco_inflation_median / 12 if eco_inflation_median is not None else None,
        "inflation_mean_monthly":   eco_inflation_mean / 12   if eco_inflation_mean   is not None else None,
        "inflation_min":     eco_inflation_min,
        "inflation_max":     eco_inflation_max,
        "inflation_count":   eco_inflation_count,
        "defence_median":    eco_defence_median,
        "defence_mean":      eco_defence_mean,
        "defence_count":     eco_defence_count,
        "total_burned":      eco_burned_total,
        "subnets_active":    len(active_results),
        "subnets_total":     len(results),
        "ema_lag_traps":     sum(1 for r in results if r["ema_lag_flag"]),
        "flow_divergences":  sum(1 for r in results if r.get("flow_price_divergence")),
    }
    eco_traj = [e for e in eco_traj if e.get("date") != date_str]
    eco_traj.append(eco_entry)
    eco_traj.sort(key=lambda e: e.get("date", ""))
    save_json(eco_traj_path, eco_traj)

    print(f"\n💾  Outputs saved:")
    print(f"    {csv_path}")
    print(f"    {meta_path}")
    print(f"    {traj_30d_path}  (rolling 30d per subnet)")
    print(f"    {traj_90d_path}  (rolling 90d EMA-effective window)")
    print(f"    {traj_hist_path}  (weekly-compressed long-term archive)")
    print(f"    {eco_traj_path}  (ecosystem inflation + defence benchmarks)")
    print(f"\n{SEPARATOR}\n")


if __name__ == "__main__":
    run_snapshot()
