"""
subnet_analysis.py — v3 (Root Reborn / Conviction era)
======================================================
Subnet Staking Decision Tool — Intelligence Sovereignty Research Suite
@im_perseverance

v3 rebuild on the bittensor v11 SDK via the shared `chain_analysis` layer.
Every read in a run comes from a single pinned block.

For each active subnet, computes:

  Yield metrics (unchanged from v2):
    - Emission APY, Real APY, Combined APY, Price APY 7d / EMA-basis

  Supply health — FIVE-layer model (was three):
    - Gross emission rate: protocol-native raw dilution pressure.
    - Net supply delta: annualised actual dilution between snapshots.
    - Burned (PRIMARY = chain counter): delta of AlphaAssets.AlphaBurned.
    - Recycled (separate counter): delta of AlphaAssets.AlphaRecycled.
        v2 conflated burn and recycle inside one derived estimate; the chain
        tracks them separately and both destroy circulating float.
    - Locked float: conviction-locked alpha — supply that mechanically
        cannot trade. locked_float_ratio = total_locked / alpha_out.

    The v2 derived estimator (total-alpha conservation identity) is KEPT but
    demoted to a reconciliation alarm: estimator vs counter divergence beyond
    band is itself a finding. The estimator's negative-burn warnings were
    fingerprinting the SubnetAlphaOut corruption months before the chain
    shipped its correction (migrate_backfill_historical_alpha_burned).

  Commitment matrix (new — the conviction/supply-defence thesis engine):
    - Burn coverage = manual_burn / owner-cut emissions received.
        >1.0 = team net-deflationary out of its own pocket. The theater
        filter: raw burn rate cannot distinguish costly effort from
        recycling free owner-cut income.
    - Owner conviction: owner-locked alpha mass + share of alpha_out.
    - Community conviction: general (non-owner) locked mass.
    - 2x2 classification:
        FORTRESS  = burning + owner-locked   (scarce and defended)
        EXPOSED   = burning, no owner lock   (generous but undefended)
        DORMITORY = owner lock, no burning   (defended, no scarcity effort)
        NEITHER   = neither                  (red flag past launch phase)

  Governance exposure (new — the ownership-gate arithmetic):
    Eligible alpha for the 18% takeover gate is
    SubnetAlphaOut - SubnetProtocolAlpha - AlphaBurned: every burned alpha
    LOWERS the bar a challenger must clear. Flags:
        TAKEOVER_WATCH    = gate-eligible subnet, non-owner leader >= 50%
                            of threshold
        TAKEOVER_IMMINENT = non-owner leader >= 80% of threshold
        UNDEFENDED_BURNER = gate-eligible + active burns + no owner lock
                            ("funding the siege equipment")

  Capital flow signals (v2 set retained, one structural change):
    - EMA TAO flow, flow momentum, flow/price divergence, pool/EMA
      divergence, EPR — all unchanged.
    - Root Reborn note: the mechanical root-proportion sell stream is GONE
      (root dividends escrow into validator baskets, default accumulate-in-
      place). New column: basket_share = aggregate root-fund alpha held on
      this subnet / alpha_out — latent sell pressure for the day root weight
      curation opens.

  Risk metrics (unchanged): liquidation price/haircut, EMA lag trap —
    except the EMA annualisation basis is now read live per subnet
    (EMAPriceHalvingBlocks is governance-settable; v2 hardcoded 30 days).

  Validator metrics (unchanged in meaning): best take-adjusted nominator
    APY, validator-miner incentive share, miner burn rate — re-plumbed onto
    the v11 runtime metagraph (dict of parallel lists) and the one-call
    `delegates` read (no more per-hotkey take RPCs).

Continuity:
  - CSV schema is a superset of v2 (new columns appended at the end).
  - Trajectory files are the same files with new per-entry fields. First v3
    run has no previous counter values in trajectory -> burn source falls
    back to the estimator (flagged ESTIMATOR); counters engage from run 2.

Outputs (same paths as v2):
  - Console report ranked by combined APY
  - subnet_analysis/snapshots/staking_snapshot_YYYY-MM-DD.csv
  - subnet_analysis/metadata/staking_metadata_YYYY-MM-DD.json
  - subnet_analysis/trajectory_30d.json / _90d.json / _historical.json
  - subnet_analysis/trajectory_ecosystem.json

Usage:
    python subnet_analysis.py
"""

import asyncio
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

import chain_analysis as chain

# ── Config ─────────────────────────────────────────────────────────────────
OUTPUT_DIR            = Path("subnet_analysis")
OUTPUT_METADATA_DIR   = OUTPUT_DIR / "metadata"
OUTPUT_SNAPSHOT_DIR   = OUTPUT_DIR / "snapshots"

BLOCKS_PER_DAY    = chain.BLOCKS_PER_DAY
BLOCKS_PER_YEAR   = chain.BLOCKS_PER_YEAR         # physical rate — APY annualisation
ONE_YEAR_BLOCKS   = chain.ONE_YEAR_BLOCKS         # governance gate age — ownership checks

MIN_TAO_EMISSION  = 0.0001
MIN_VALIDATOR_TAO = 1000.0
TRAJECTORY_30D    = 30
TRAJECTORY_90D    = 90
IMMUNITY_BLOCKS   = 7200 * 30 * 4
OWNER_INFLOW_THRESHOLD = 0.20
MIN_TOTAL_INFLOW_TAO   = 1.0
EMA_LAG_THRESHOLD = -0.15
MIN_ALPHA_FOR_HAIRCUT = 100.0
DEFAULT_EMA_DAYS  = 30.0            # fallback only — live value read per subnet

# Commitment matrix thresholds (provisional — tune against the first weeks
# of live conviction data; document any change in the article).
BURN_COVERAGE_ACTIVE = 0.25   # burning >= 25% of owner-cut income = "burning"
OWNER_LOCK_MIN_RATIO = 0.005  # owner lock >= 0.5% of alpha_out = "locked"
TAKEOVER_WATCH_PCT     = 0.50
TAKEOVER_IMMINENT_PCT  = 0.80

CONVICTION_CONCURRENCY = 8
METAGRAPH_CONCURRENCY  = 4

SEPARATOR  = "=" * 130
THIN_SEP   = "-" * 130

# ── Helpers ────────────────────────────────────────────────────────────────

def safe_float(val, default=0.0):
    return chain.f(val, default)

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

load_json = chain.load_json
save_json = chain.save_json

# ── Supply inflation metrics ───────────────────────────────────────────────

def compute_gross_emission_rate(alpha_out_emission, alpha_out):
    """
    Protocol-native gross emission rate, annualised (unchanged from v2).
    (alpha_out_emission * blocks_per_year) / alpha_out — raw dilution
    pressure before any buyback/burn defences.
    """
    if alpha_out is None or alpha_out <= 0:
        return None
    if alpha_out_emission is None or alpha_out_emission <= 0:
        return 0.0
    return (alpha_out_emission * BLOCKS_PER_YEAR) / alpha_out

def compute_net_supply_delta(netuid, current_alpha_out, trajectory_90d, date_str,
                             current_emission_apy=None):
    """
    Net supply change between current and previous snapshot, annualised
    (unchanged from v2, including the emission-onset guard).
    Returns (annualised_rate, days_gap) or (None, None).
    """
    key = str(netuid)
    history = trajectory_90d.get(key, [])
    if not history:
        return None, None
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
    prev_emission_apy = prev.get("emission_apy")
    if current_emission_apy is not None and prev_emission_apy is None:
        return None, None
    delta_pct = (current_alpha_out - prev_alpha) / prev_alpha
    annualised = delta_pct * (365 / days_gap)
    return annualised, days_gap

def compute_estimator_burned(alpha_out, alpha_in, prev_alpha_out, prev_alpha_in,
                             alpha_out_emission, alpha_in_emission, days_gap):
    """
    v2 derived burn estimate via the total-alpha conservation identity.
    DEMOTED in v3: no longer the primary burn source — it feeds the
    reconciliation alarm against the chain counters. Negative raw values are
    reported (not just clamped): they are the corruption fingerprint.

    Returns (burned_clamped, total_emission, raw_burned) or (None, None, None).
    """
    if any(v is None for v in [alpha_out, alpha_in, prev_alpha_out, prev_alpha_in,
                                alpha_out_emission, alpha_in_emission]):
        return None, None, None
    if days_gap is None or days_gap <= 0:
        return None, None, None
    total_emission   = (alpha_out_emission + alpha_in_emission) * BLOCKS_PER_DAY * days_gap
    actual_change    = (alpha_out + alpha_in) - (prev_alpha_out + prev_alpha_in)
    raw_burned = total_emission - actual_change
    return max(0.0, raw_burned), total_emission, raw_burned

def compute_counter_burned(pool, prev_entry, days_gap):
    """
    v3 PRIMARY burn source: deltas of the on-chain AlphaAssets counters
    between this snapshot and the previous trajectory entry.

    Returns (burned_delta, recycled_delta) or (None, None) when either side
    lacks counter values (first v3 run over v2 trajectories, or a read
    failure — never assume zero burns from missing data).
    """
    if prev_entry is None or days_gap is None or days_gap <= 0:
        return None, None
    cur_b, cur_r = pool.get("alpha_burned"), pool.get("alpha_recycled")
    prev_b, prev_r = prev_entry.get("alpha_burned_cum"), prev_entry.get("alpha_recycled_cum")
    if None in (cur_b, cur_r, prev_b, prev_r):
        return None, None
    return max(0.0, cur_b - prev_b), max(0.0, cur_r - prev_r)

def compute_supply_defence(burned_tokens, total_emission, protocol_absorption):
    """
    Fraction of total emission actively destroyed by owner buyback/burn
    (unchanged from v2). manual_burn = max(0, burned - protocol_absorption).
    """
    if burned_tokens is None or total_emission is None or protocol_absorption is None:
        return None, None
    if total_emission <= 0:
        return None, None
    manual_burn = max(0.0, burned_tokens - protocol_absorption)
    return manual_burn / total_emission, manual_burn

def compute_burn_coverage(manual_burn, alpha_out_emission, owner_cut, days_gap):
    """
    Burn coverage = manual burns / owner-cut emissions received (the theater
    filter). The owner receives owner_cut (~18%) of alpha_out emission
    automatically; burning less than that is recycling free income, not
    costly supply defence. >1.0 = net-deflationary out of pocket.
    """
    if manual_burn is None or alpha_out_emission is None or days_gap is None:
        return None, None
    if days_gap <= 0 or owner_cut is None or owner_cut <= 0:
        return None, None
    owner_cut_emission = alpha_out_emission * owner_cut * BLOCKS_PER_DAY * days_gap
    if owner_cut_emission <= 0:
        return None, owner_cut_emission
    return manual_burn / owner_cut_emission, owner_cut_emission

def classify_commitment(burn_coverage, owner_lock_ratio):
    """
    2x2 commitment matrix. Requires BOTH signals observable; returns None
    (unclassified) when either is unknown — absence of data is not absence
    of commitment.
    """
    if burn_coverage is None or owner_lock_ratio is None:
        return None
    burning = burn_coverage >= BURN_COVERAGE_ACTIVE
    locked  = owner_lock_ratio >= OWNER_LOCK_MIN_RATIO
    if burning and locked:      return "FORTRESS"
    if burning and not locked:  return "EXPOSED"
    if locked and not burning:  return "DORMITORY"
    return "NEITHER"

def governance_flags(gate_eligible, conviction, burn_coverage, owner_lock_ratio):
    """
    Ownership-gate exposure flags from the subnet_convictions read.
    Returns (flag_or_None, leader_is_owner, leader_pct, leader_blocks).
    """
    leader = (conviction or {}).get("leader")
    leader_is_owner = leader.get("is_owner") if leader else None
    leader_pct      = leader.get("pct_of_threshold") if leader else None
    leader_blocks   = leader.get("blocks_to_threshold") if leader else None
    flag = None
    if gate_eligible and leader and not leader.get("is_owner"):
        if leader_pct is not None and leader_pct >= TAKEOVER_IMMINENT_PCT:
            flag = "TAKEOVER_IMMINENT"
        elif leader_pct is not None and leader_pct >= TAKEOVER_WATCH_PCT:
            flag = "TAKEOVER_WATCH"
    if flag is None and gate_eligible:
        if (burn_coverage is not None and burn_coverage >= BURN_COVERAGE_ACTIVE
                and owner_lock_ratio is not None
                and owner_lock_ratio < OWNER_LOCK_MIN_RATIO):
            flag = "UNDEFENDED_BURNER"
    return flag, leader_is_owner, leader_pct, leader_blocks

# ── 7-day price APY proxy (unchanged from v2) ──────────────────────────────

def compute_7d_price_apy(netuid, current_price, trajectory_90d, date_str):
    key = str(netuid)
    history = [e for e in trajectory_90d.get(key, []) if e.get("date") != date_str]
    if len(history) < 1:
        return None
    try:
        curr_dt = datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None
    target_dt = curr_dt - timedelta(days=7)
    best, best_diff = None, float("inf")
    for entry in history:
        try:
            entry_dt = datetime.strptime(entry["date"], "%Y-%m-%d")
            diff = abs((entry_dt - target_dt).days)
            if diff < best_diff:
                best_diff, best = diff, entry
        except Exception:
            continue
    if best is None or best_diff > 3:
        return None
    old_price = best.get("spot_price")
    if not old_price or old_price <= 0 or current_price <= 0:
        return None
    period_days = max((curr_dt - datetime.strptime(best["date"], "%Y-%m-%d")).days, 1)
    return ((current_price - old_price) / old_price) * (365 / period_days)

# ── v11 runtime metagraph adapters ─────────────────────────────────────────
# The v11 `metagraph` read returns the runtime struct: a dict of parallel
# per-uid lists. Field names normalised here in one place.

def mg_list(graph, *names, default=None):
    for n in names:
        v = graph.get(n)
        if v is not None:
            return v
    return default if default is not None else []

def _norm01(v):
    """Normalise a u16-encoded or already-float 0..1 field defensively."""
    x = safe_float(v)
    return x / 65535.0 if x > 1.0 else x

def get_best_validator(graph, take_map):
    """
    Best take-adjusted nominator APY on a subnet (v2 logic on v11 shapes).
    take_map: {hotkey: take_fraction} from the one-call delegates read.
    """
    try:
        hotkeys    = mg_list(graph, "hotkeys")
        stakes     = mg_list(graph, "total_stake", "stake", "alpha_stake")
        dividends  = mg_list(graph, "dividends")
        emissions  = mg_list(graph, "emission", "emissions")
        permits    = mg_list(graph, "validator_permit", "validator_permits")
        v_trust    = mg_list(graph, "validator_trust", default=[])
        n_uids     = len(hotkeys)
        if n_uids == 0:
            return None
        total_dividends = sum(_norm01(dividends[i]) for i in range(n_uids)) if dividends else 0
        total_stake     = sum(safe_float(stakes[i]) for i in range(n_uids)) if stakes else 0
        best, best_apy = None, -999
        for uid in range(n_uids):
            if not (permits and uid < len(permits) and permits[uid]):
                continue
            stake = safe_float(stakes[uid]) if uid < len(stakes) else 0.0
            if stake < MIN_VALIDATOR_TAO:
                continue
            div      = _norm01(dividends[uid]) if uid < len(dividends) else 0.0
            div_pct  = div / total_dividends if total_dividends > 0 else 0
            stake_pct = stake / total_stake if total_stake > 0 else 0
            if stake_pct <= 0:
                continue
            uid_emission = safe_float(emissions[uid]) if uid < len(emissions) else 0.0
            raw_apy = (uid_emission / stake) * BLOCKS_PER_YEAR if stake > 0 else 0
            hotkey = hotkeys[uid]
            take_val = take_map.get(hotkey)
            take = max(0.0, min(1.0, safe_float(take_val))) if take_val is not None else None
            est_apy = raw_apy * (1.0 - take) if take is not None else raw_apy
            if est_apy > best_apy:
                best_apy = est_apy
                best = {
                    "uid": uid,
                    "hotkey_short": str(hotkey)[:8] + "...",
                    "stake": stake, "take": take, "div_pct": div_pct,
                    "est_apy": est_apy, "raw_apy": raw_apy,
                    "trust": _norm01(v_trust[uid]) if uid < len(v_trust) else None,
                }
        return best
    except Exception:
        return None

def get_vali_miner_share(graph):
    """Fraction of mining incentive captured by validator-miners (v2 logic)."""
    try:
        incentives = mg_list(graph, "incentives", "incentive")
        permits    = mg_list(graph, "validator_permit", "validator_permits")
        n_uids = len(incentives)
        if n_uids == 0:
            return None
        vali = {i for i in range(min(n_uids, len(permits))) if permits[i]}
        total = sum(_norm01(incentives[i]) for i in range(n_uids))
        miner = sum(_norm01(incentives[i]) for i in range(n_uids) if i not in vali)
        if total <= 0:
            return None
        return max(0.0, 1.0 - (miner / total))
    except Exception:
        return None

def get_miner_burn_rate(graph):
    """Fraction of miner emission allocation burned (v2 logic, v11 shapes)."""
    try:
        incentives = mg_list(graph, "incentives", "incentive")
        if not incentives:
            return None
        total = sum(_norm01(v) for v in incentives)
        return max(0.0, min(1.0, 1.0 - total))
    except Exception:
        return None

# ── Chain collection (async — everything that touches the network) ─────────

async def _gather_limited(coros, limit):
    sem = asyncio.Semaphore(limit)
    async def _wrap(c):
        async with sem:
            return await c
    return await asyncio.gather(*[_wrap(c) for c in coros], return_exceptions=True)

async def collect(snap, block, client):
    """
    All chain reads for one run, from one pinned block. Returns plain dicts
    so `analyse()` stays pure and offline-testable.
    """
    print("  Fetching pool state, governance params, names, owners, flows...")
    pools, params, names, owners, ema_flows, baskets = await asyncio.gather(
        chain.all_subnet_pool_state(snap),
        chain.governance_params(snap),
        chain.read(snap, "subnet_names", default={}),
        chain.qmap(snap, "SubtensorModule", "SubnetOwner"),
        chain.qmap(snap, "SubtensorModule", "SubnetEmaTaoFlow"),
        chain.root_baskets(snap),
    )
    names = {int(k): v for k, v in (names or {}).items()}
    basket_exposure = await chain.basket_subnet_exposure(snap, baskets)

    # EMA TAO flow: storage carries (last_block, I64F64 rao) per subnet.
    ema_inflow = {}
    for n, v in (ema_flows or {}).items():
        try:
            raw = v[1] if isinstance(v, (tuple, list)) and len(v) == 2 else v
            ema_inflow[int(n)] = chain.fixed_to_float(raw, frac_bits=64) / chain.RAO_PER_TAO
        except Exception:
            continue

    # Delegate takes: one call replaces v2's per-hotkey RPC cache.
    print("  Fetching delegate takes (one-call catalog)...")
    take_map = {}
    for d in await chain.delegates(snap):
        hk = getattr(d, "hotkey", None) or (d.get("hotkey") if isinstance(d, dict) else None)
        tk = getattr(d, "take", None) if not isinstance(d, dict) else d.get("take")
        if hk is not None and tk is not None:
            take_map[str(hk)] = safe_float(tk)

    netuids = sorted(pools.keys())

    print(f"  Fetching conviction state ({len(netuids)} subnets, "
          f"concurrency {CONVICTION_CONCURRENCY})...")
    conv_results = await _gather_limited(
        [chain.subnet_convictions(snap, n) for n in netuids], CONVICTION_CONCURRENCY)
    convictions = {n: (c if isinstance(c, dict) else None)
                   for n, c in zip(netuids, conv_results)}

    print(f"  Fetching metagraphs ({len(netuids)} subnets, "
          f"concurrency {METAGRAPH_CONCURRENCY})...")
    metagraphs = {}
    mg_errors = {}
    for n in netuids:
        try:
            m = await chain.metagraph(snap, n)
            if m is not None:
                metagraphs[n] = m
            else:
                mg_errors[n] = "returned None"
        except Exception as e:
            mg_errors[n] = f"{type(e).__name__}: {e}"

    non_none = len(metagraphs)
    print(f"  Metagraphs loaded: {non_none}/{len(netuids)}")
    if mg_errors:
        sample_errors = list(mg_errors.items())[:5]
        for sn, err in sample_errors:
            print(f"    SN{sn} metagraph error: {err}")
    sample = next(iter(metagraphs.values()), None)
    if sample:
        if isinstance(sample, dict):
            print(f"  Sample metagraph type: dict, keys: {list(sample.keys())[:20]}")
        else:
            print(f"  Sample metagraph type: {type(sample).__name__}, attrs: {[a for a in dir(sample) if not a.startswith('_')][:20]}")
    # Fill None for missing subnets so analyse() doesn't KeyError
    for n in netuids:
        if n not in metagraphs:
            metagraphs[n] = None

    return {
        "block": block,
        "pools": pools,
        "params": params,
        "names": names,
        "owners": {int(k): str(v) for k, v in (owners or {}).items()},
        "ema_inflow": ema_inflow,
        "basket_exposure": basket_exposure,
        "take_map": take_map,
        "convictions": convictions,
        "metagraphs": metagraphs,
    }

# ── Pure analysis ──────────────────────────────────────────────────────────

def analyse(data, traj_90d, now):
    """
    Build per-subnet result rows + trajectory entries from collected chain
    data and trajectory history. Pure function — no I/O, no network.
    Returns (results, traj_entries {netuid_key: entry}).
    """
    date_str      = now.strftime("%Y-%m-%d")
    current_block = data["block"]
    params        = data["params"]
    owner_cut     = params.get("owner_cut") or 0.18
    results       = []
    traj_entries  = {}

    for netuid in sorted(data["pools"].keys()):
        pool = data["pools"][netuid]
        name = data["names"].get(netuid) or f"SN{netuid}"
        conviction = data["convictions"].get(netuid)
        graph      = data["metagraphs"].get(netuid)

        spot_price         = pool["spot_price"]
        moving_price       = pool["moving_price"]
        tao_reserves       = pool["tao_reserves"]
        alpha_out          = pool["alpha_outstanding"]
        alpha_in_pool      = pool["alpha_in_pool"]
        alpha_out_emission = pool["alpha_out_emission"]
        alpha_in_emission  = pool["alpha_in_emission"]
        tao_emission       = pool["tao_in_emission"]
        volume             = pool["volume"]
        registered_at      = pool["registered_at"]
        low_emission = (tao_emission or 0.0) < MIN_TAO_EMISSION

        # Live EMA basis (v2 hardcoded 30d — now governance-settable per subnet)
        ema_days = DEFAULT_EMA_DAYS
        if pool.get("ema_halving_blocks"):
            ema_days = pool["ema_halving_blocks"] / BLOCKS_PER_DAY

        momentum_30d = (spot_price - moving_price) / moving_price if moving_price and moving_price > 0 else None
        ema_lag_flag = momentum_30d is not None and momentum_30d < EMA_LAG_THRESHOLD

        emission_apy = None
        if tao_reserves and tao_reserves > 0 and tao_emission and tao_emission > 0:
            emission_apy = (tao_emission * BLOCKS_PER_YEAR) / tao_reserves

        gross_emission_rate = compute_gross_emission_rate(alpha_out_emission, alpha_out)
        net_supply_delta, supply_days_gap = compute_net_supply_delta(
            netuid, alpha_out, traj_90d, date_str, current_emission_apy=emission_apy)

        key = str(netuid)
        prev_entries = [e for e in traj_90d.get(key, []) if e.get("date") != date_str]
        prev = prev_entries[-1] if prev_entries else None

        # ── Burn layer: counters primary, estimator as reconciliation ─────
        est_burned = est_total_emission = est_raw = None
        if prev is not None and supply_days_gap is not None:
            est_burned, est_total_emission, est_raw = compute_estimator_burned(
                alpha_out, alpha_in_pool, prev.get("alpha_outstanding"),
                prev.get("alpha_in_pool"), alpha_out_emission, alpha_in_emission,
                supply_days_gap)

        counter_burned, counter_recycled = compute_counter_burned(pool, prev, supply_days_gap)

        if counter_burned is not None:
            burned_tokens = counter_burned
            burned_source = "COUNTER"
            total_emission = est_total_emission
            if total_emission is None and supply_days_gap:
                total_emission = ((alpha_out_emission or 0) + (alpha_in_emission or 0)) \
                                 * BLOCKS_PER_DAY * supply_days_gap
        else:
            burned_tokens = est_burned
            burned_source = "ESTIMATOR" if est_burned is not None else None
            total_emission = est_total_emission

        recon = None
        if prev is not None:
            recon = chain.burn_reconciliation(
                {"alpha_burned": prev.get("alpha_burned_cum"),
                 "alpha_recycled": prev.get("alpha_recycled_cum")},
                {"alpha_burned": pool.get("alpha_burned"),
                 "alpha_recycled": pool.get("alpha_recycled"),
                 "alpha_out_emission": alpha_out_emission,
                 "_days_gap": supply_days_gap, "netuid": netuid},
                est_burned)
        recon_flag = recon["flag"] if recon else None
        recon_gap_relative = recon.get("gap_relative") if recon else None

        # ── Liquidation floor (unchanged) ────────────────────────────────
        liquidation_price = tao_reserves / alpha_out if alpha_out and alpha_out > 0 else None
        liquidation_haircut = (
            (liquidation_price - spot_price) / spot_price
            if spot_price and spot_price > 0 and liquidation_price is not None
            and alpha_out and alpha_out >= MIN_ALPHA_FOR_HAIRCUT
            else None
        )

        # ── Flow signals (v2 set) ────────────────────────────────────────
        ema_tao_inflow = data["ema_inflow"].get(netuid)

        pool_growth_real = None
        pool_ema_divergence = None
        if prev is not None and spot_price and spot_price > 0:
            prev_tao, prev_spot = prev.get("tao_reserves"), prev.get("spot_price")
            if prev_tao and prev_spot and prev_spot > 0:
                pool_growth_real = (tao_reserves / spot_price) - (prev_tao / prev_spot)
                if pool_growth_real is not None and ema_tao_inflow is not None:
                    if pool_growth_real > 0 and ema_tao_inflow < 0:
                        pool_ema_divergence = "ACCUMULATING_PRE_EMA"
                    elif pool_growth_real < 0 and ema_tao_inflow > 0:
                        pool_ema_divergence = "DISTRIBUTING_PRE_EMA"

        flow_momentum = None
        flow_price_divergence = None
        if prev is not None and ema_tao_inflow is not None:
            prev_ema_inflow = prev.get("ema_tao_inflow")
            if prev_ema_inflow is not None and prev_ema_inflow != 0:
                flow_momentum = (ema_tao_inflow - prev_ema_inflow) / abs(prev_ema_inflow)
                if momentum_30d is not None and flow_momentum is not None:
                    if momentum_30d > 0.05 and flow_momentum < -0.05:
                        flow_price_divergence = "PRICE_UP_FLOW_DOWN"
                    elif momentum_30d < -0.05 and flow_momentum > 0.05:
                        flow_price_divergence = "PRICE_DOWN_FLOW_UP"

        emission_price_ratio_raw = (
            ema_tao_inflow / spot_price
            if (ema_tao_inflow is not None and ema_tao_inflow > 0
                and spot_price and spot_price > 0)
            else None
        )
        emission_price_ratio = emission_price_ratio_raw if not low_emission else None

        # Root Reborn flow input: latent sell pressure from root baskets.
        basket_alpha = data["basket_exposure"].get(netuid, 0.0)
        basket_share = (basket_alpha / alpha_out) if alpha_out and alpha_out > 0 else None

        # ── Price APYs (EMA basis now live) ──────────────────────────────
        price_apy_7d = compute_7d_price_apy(netuid, spot_price, traj_90d, date_str)
        price_apy_30d = None
        if moving_price and moving_price > 0 and momentum_30d is not None and ema_days > 0:
            price_apy_30d = momentum_30d * (365 / ema_days)

        if emission_apy is not None and net_supply_delta is not None:
            real_apy = emission_apy - net_supply_delta
        elif emission_apy is not None:
            real_apy = emission_apy
        else:
            real_apy = None

        combined_7d  = (real_apy or 0) + (price_apy_7d  or 0) if (real_apy is not None or price_apy_7d  is not None) else None
        combined_30d = (real_apy or 0) + (price_apy_30d or 0) if (real_apy is not None or price_apy_30d is not None) else None

        # ── Validator + owner layers ─────────────────────────────────────
        best_validator = get_best_validator(graph, data["take_map"]) if graph else None
        vali_miner_share = get_vali_miner_share(graph) if graph else None
        miner_burn_rate  = get_miner_burn_rate(graph)  if graph else None

        owner_coldkey = data["owners"].get(netuid)
        owner_stake = None
        if graph and owner_coldkey:
            coldkeys = mg_list(graph, "coldkeys")
            stakes   = mg_list(graph, "total_stake", "stake", "alpha_stake")
            total = 0.0
            for idx, ck in enumerate(coldkeys):
                if str(ck) == owner_coldkey and idx < len(stakes):
                    total += safe_float(stakes[idx])
            if total > 0:
                owner_stake = total

        immune = (registered_at is not None and
                  (current_block - registered_at) < IMMUNITY_BLOCKS)

        # Owner inflow detection (v2 identity layer, unchanged)
        owner_inflow_pct = None
        owner_inflow_flag = None
        if owner_stake is not None and prev is not None:
            prev_owner_stake = prev.get("owner_stake")
            prev_tao_rsv     = prev.get("tao_reserves")
            if prev_owner_stake is not None and prev_tao_rsv is not None and prev_tao_rsv > 0:
                owner_alpha_delta = max(0.0, owner_stake - prev_owner_stake)
                owner_inflow_tao = (owner_alpha_delta * tao_reserves / alpha_out
                                    if alpha_out and alpha_out > 0 else 0.0)
                total_inflow = max(0.0, tao_reserves - prev_tao_rsv)
                if total_inflow > MIN_TOTAL_INFLOW_TAO:
                    owner_inflow_pct = owner_inflow_tao / total_inflow
                    if owner_inflow_pct > OWNER_INFLOW_THRESHOLD:
                        burn_tao_equiv = (burned_tokens * tao_reserves / alpha_out
                                          if burned_tokens and alpha_out and alpha_out > 0 else 0.0)
                        excess_pct = (owner_inflow_tao - burn_tao_equiv) / total_inflow
                        owner_inflow_flag = ("BUYBACK_GAMING" if excess_pct > 0.10
                                             else "BUYBACK_JUSTIFIED")

        # Behavioural layer: large entry anomaly (v2, unchanged)
        large_entry_anomaly = None
        cutoff_7d = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        if pool_growth_real is not None and prev_entries:
            prev_7d = [e for e in prev_entries if e.get("date", "") >= cutoff_7d]
            if prev_7d:
                avg_daily_growth = sum(e.get("pool_growth_real", 0) or 0 for e in prev_7d) / len(prev_7d)
                growth_multiple = (pool_growth_real / avg_daily_growth
                                   if avg_daily_growth > 0 else None)
                if growth_multiple is not None and growth_multiple > 3.0 and pool_growth_real > 10.0:
                    price_spike = momentum_30d is not None and momentum_30d > 0.10
                    ema_low = ema_tao_inflow is not None and ema_tao_inflow < 0.001
                    prior_spikes = sum(
                        1 for e in prev_7d
                        if avg_daily_growth > 0 and
                        ((e.get("pool_growth_real") or 0) > 3 * avg_daily_growth))
                    if price_spike and ema_low:
                        large_entry_anomaly = "ENTRY_PRICE_PUMP"
                    elif prior_spikes >= 2:
                        large_entry_anomaly = "ENTRY_REPEATED"
                    elif ema_low and prior_spikes >= 1:
                        large_entry_anomaly = "ENTRY_PRE_EMA_PATTERN"

        # ── Supply defence + manual burn (v2 decomposition, v3 inputs) ───
        supply_defence = manual_burn = protocol_absorption = None
        if burned_tokens is not None and supply_days_gap and supply_days_gap > 0:
            mbr = miner_burn_rate if miner_burn_rate is not None else 0.0
            miner_pay_frac = max(0.0, min(1.0, 1.0 - mbr))
            protocol_burn_frac = max(0.0, min(1.0, 0.59 - 0.41 * miner_pay_frac))
            alpha_out_protocol_burn = (alpha_out_emission or 0) * protocol_burn_frac * BLOCKS_PER_DAY * supply_days_gap
            alpha_in_absorbed       = (alpha_in_emission  or 0) * BLOCKS_PER_DAY * supply_days_gap
            protocol_absorption     = alpha_out_protocol_burn + alpha_in_absorbed
            supply_defence, manual_burn = compute_supply_defence(
                burned_tokens, total_emission, protocol_absorption)

        supply_defence_quality = None
        if manual_burn and manual_burn > 0:
            if owner_inflow_flag in ("BUYBACK_JUSTIFIED", "BUYBACK_GAMING"):
                supply_defence_quality = "SKIN_IN_GAME"
            elif owner_inflow_pct is None or owner_inflow_pct < 0.05:
                supply_defence_quality = "MINER_BURN_ONLY"

        # ── Commitment matrix (v3) ───────────────────────────────────────
        burn_coverage, owner_cut_emission = compute_burn_coverage(
            manual_burn, alpha_out_emission, owner_cut, supply_days_gap)

        total_locked_alpha = owner_locked_alpha = community_locked_alpha = None
        locked_float_ratio = owner_lock_ratio = None
        if conviction is not None:
            total_locked_alpha = conviction.get("total_locked_alpha")
            owner_locked_alpha = (conviction.get("owner") or {}).get("locked_alpha", 0.0) \
                                 if conviction.get("owner") else 0.0
            if total_locked_alpha is not None and owner_locked_alpha is not None:
                community_locked_alpha = max(0.0, total_locked_alpha - owner_locked_alpha)
            if alpha_out and alpha_out > 0:
                if total_locked_alpha is not None:
                    locked_float_ratio = total_locked_alpha / alpha_out
                if owner_locked_alpha is not None:
                    owner_lock_ratio = owner_locked_alpha / alpha_out

        commitment_class = classify_commitment(burn_coverage, owner_lock_ratio)

        # ── Governance exposure (v3) ─────────────────────────────────────
        gate_eligible = (registered_at is not None and
                         (current_block - registered_at) >= ONE_YEAR_BLOCKS)
        gov_flag, leader_is_owner, leader_pct, leader_blocks = governance_flags(
            gate_eligible, conviction, burn_coverage, owner_lock_ratio)

        if momentum_30d is None:        ema_band = "N/A"
        elif momentum_30d >  0.20:      ema_band = "PREMIUM"
        elif momentum_30d < -0.20:      ema_band = "DISCOUNT"
        else:                           ema_band = "IN BAND"

        row = {
            "netuid": netuid, "name": name, "date": date_str, "block": current_block,
            "spot_price": spot_price, "moving_price": moving_price,
            "momentum_30d": momentum_30d, "ema_band": ema_band,
            "ema_lag_flag": ema_lag_flag, "low_emission": low_emission,
            "tao_emission": tao_emission, "tao_reserves": tao_reserves,
            "alpha_outstanding": alpha_out, "alpha_out_emission": alpha_out_emission,
            "alpha_in_pool": alpha_in_pool, "volume": volume,
            "ema_tao_inflow": ema_tao_inflow, "pool_growth_real": pool_growth_real,
            "pool_ema_divergence": pool_ema_divergence, "flow_momentum": flow_momentum,
            "flow_price_divergence": flow_price_divergence,
            "emission_price_ratio": emission_price_ratio,
            "emission_price_ratio_raw": emission_price_ratio_raw,
            "emission_apy": emission_apy, "price_apy_7d": price_apy_7d,
            "price_apy_30d": price_apy_30d, "combined_apy_7d": combined_7d,
            "combined_apy_30d": combined_30d, "real_apy": real_apy,
            "gross_emission_rate": gross_emission_rate,
            "net_supply_delta": net_supply_delta, "supply_days_gap": supply_days_gap,
            "supply_defence": supply_defence,
            "supply_defence_quality": supply_defence_quality,
            "burned_tokens": burned_tokens, "manual_burn": manual_burn,
            "miner_burn_rate": miner_burn_rate,
            "liquidation_price": liquidation_price,
            "liquidation_haircut": liquidation_haircut,
            "owner_coldkey": owner_coldkey[:8] + "..." if owner_coldkey else None,
            "owner_stake": owner_stake, "owner_inflow_pct": owner_inflow_pct,
            "owner_inflow_flag": owner_inflow_flag,
            "large_entry_anomaly": large_entry_anomaly,
            "immune": immune, "registered_at": registered_at,
            "vali_miner_share": vali_miner_share,
            "best_validator_uid":     best_validator["uid"]          if best_validator else None,
            "best_validator_hotkey":  best_validator["hotkey_short"] if best_validator else None,
            "best_validator_take":    best_validator["take"]         if best_validator else None,
            "best_validator_raw_apy": best_validator["raw_apy"]      if best_validator else None,
            "best_validator_apy":     best_validator["est_apy"]      if best_validator else None,
            "best_validator_div_pct": best_validator["div_pct"]      if best_validator else None,
            "best_validator_trust":   best_validator["trust"]        if best_validator else None,
            # ── v3 columns (appended — CSV superset of v2) ──
            "burned_source": burned_source,
            "alpha_burned_cum": pool.get("alpha_burned"),
            "alpha_recycled_cum": pool.get("alpha_recycled"),
            "recycled_tokens": counter_recycled,
            "recon_flag": recon_flag, "recon_gap_relative": recon_gap_relative,
            "burn_coverage": burn_coverage, "owner_cut_emission": owner_cut_emission,
            "total_locked_alpha": total_locked_alpha,
            "locked_float_ratio": locked_float_ratio,
            "owner_locked_alpha": owner_locked_alpha,
            "owner_lock_ratio": owner_lock_ratio,
            "community_locked_alpha": community_locked_alpha,
            "commitment_class": commitment_class,
            "gate_eligible": gate_eligible,
            "leader_is_owner": leader_is_owner,
            "leader_pct_of_threshold": leader_pct,
            "leader_blocks_to_threshold": leader_blocks,
            "governance_flag": gov_flag,
            "basket_alpha": basket_alpha, "basket_share": basket_share,
            "ema_halving_days": ema_days,
        }
        results.append(row)

        traj_entries[key] = {
            "date": date_str, "block": current_block,
            "spot_price": spot_price, "moving_price": moving_price,
            "alpha_outstanding": alpha_out, "alpha_in_pool": alpha_in_pool,
            "alpha_out_emission": alpha_out_emission,
            "alpha_in_emission": alpha_in_emission,
            "tao_reserves": tao_reserves, "emission_apy": emission_apy,
            "combined_apy_30d": combined_30d, "real_apy": real_apy,
            "gross_emission_rate": gross_emission_rate,
            "net_supply_delta": net_supply_delta,
            "supply_defence": supply_defence,
            "supply_defence_quality": supply_defence_quality,
            "burned_tokens": burned_tokens, "manual_burn": manual_burn,
            "miner_burn_rate": miner_burn_rate,
            "protocol_absorption": protocol_absorption,
            "liquidation_price": liquidation_price,
            "liquidation_haircut": liquidation_haircut,
            "owner_stake": owner_stake, "owner_inflow_pct": owner_inflow_pct,
            "owner_inflow_flag": owner_inflow_flag,
            "large_entry_anomaly": large_entry_anomaly,
            "ema_tao_inflow": ema_tao_inflow,
            "pool_growth_real": pool_growth_real,
            "flow_momentum": flow_momentum,
            "emission_price_ratio": emission_price_ratio,
            "emission_price_ratio_raw": emission_price_ratio_raw,
            "vali_miner_share": vali_miner_share,
            # v3 trajectory fields
            "burned_source": burned_source,
            "alpha_burned_cum": pool.get("alpha_burned"),
            "alpha_recycled_cum": pool.get("alpha_recycled"),
            "recycled_tokens": counter_recycled,
            "recon_flag": recon_flag,
            "burn_coverage": burn_coverage,
            "total_locked_alpha": total_locked_alpha,
            "locked_float_ratio": locked_float_ratio,
            "owner_locked_alpha": owner_locked_alpha,
            "owner_lock_ratio": owner_lock_ratio,
            "commitment_class": commitment_class,
            "governance_flag": gov_flag,
            "leader_pct_of_threshold": leader_pct,
            "basket_share": basket_share,
        }

    # Ranks (v2, unchanged)
    results.sort(key=lambda r: r["combined_apy_30d"] if r["combined_apy_30d"] is not None else -999, reverse=True)
    for i, r in enumerate(results):
        r["combined_apy_rank"] = i + 1
    for i, r in enumerate(sorted(results, key=lambda r: r["real_apy"] if r["real_apy"] is not None else -999, reverse=True)):
        r["real_apy_rank"] = i + 1
    for i, r in enumerate(sorted(results, key=lambda r: r["emission_price_ratio"] if r["emission_price_ratio"] is not None else -999, reverse=True)):
        r["epr_rank"] = i + 1
    for i, r in enumerate(sorted(results, key=lambda r: r["tao_emission"] or 0, reverse=True)):
        r["emission_rank"] = i + 1
    return results, traj_entries

# ── Console report ─────────────────────────────────────────────────────────

def print_report(results, eco):
    active_results       = [r for r in results if not r["low_emission"]]
    low_emission_results = [r for r in results if r["low_emission"]]

    print(SEPARATOR)
    print("  SUBNET STAKING RANKINGS — Sorted by Combined APY (30d basis)")
    print("  Combined vs Real vs Emission rank divergence highlights EMA lag traps and dilution")
    print(THIN_SEP)
    print(f"  {'C#':<4} {'R#':<4} {'Em#':<5} {'EPR#':<5} {'SN':<6} {'Name':<22} {'Emiss APY':>10} {'Real APY':>10} "
          f"{'Price 7d':>10} {'Price 30d':>10} {'Comb 7d':>10} {'Comb 30d':>10} "
          f"{'Gross Em':>10} {'Net Delta':>10} {'Defence':>8} {'BurnCov':>8} {'Lock%':>7} {'Matrix':>10} {'Liq':>5} {'EP Ratio':>9} {'EMA Band':>10} {'Best Validator'}")
    print(THIN_SEP)

    matrix_icon = {"FORTRESS": "🏰", "EXPOSED": "🩸", "DORMITORY": "🛏️", "NEITHER": "⚪"}
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
        bc_str  = f"{r['burn_coverage']:.2f}x" if r.get("burn_coverage") is not None else "  N/A "
        lk_str  = fmt_pct(r["locked_float_ratio"], 1) if r.get("locked_float_ratio") is not None else "  N/A"
        mx      = r.get("commitment_class")
        mx_str  = f"{matrix_icon.get(mx, '  ')}{mx[:8]}" if mx else "   N/A  "
        rank_div  = r["emission_rank"] - r["combined_apy_rank"]
        rank_flag = f"↑{rank_div}" if rank_div >= 5 else (f"↓{abs(rank_div)}" if rank_div <= -5 else "~")
        band_str  = f"⚠️{r['ema_band']}" if r["ema_lag_flag"] else r.get("ema_band", "N/A")
        epr_str  = f"{r['emission_price_ratio']:.3f}" if r["emission_price_ratio"] is not None else "  N/A "
        epr_flag = "🟢" if (r["emission_price_ratio"] is not None and r["emission_price_ratio"] > 1.0) else "  "
        lh = r.get("liquidation_haircut")
        liq_flag = "🟢" if lh is not None and lh > 0.1 else "🔴" if lh is not None and lh < -0.7 else "  "
        src_mark = "†" if r.get("burned_source") == "ESTIMATOR" else " "
        print(
            f"  #{r['combined_apy_rank']:<3} #{r['real_apy_rank']:<3} #{r['emission_rank']:<4} #{r['epr_rank']:<4} SN{r['netuid']:<4} {r['name']:<22} "
            f"{fmt_apy(r['emission_apy']):>10} {fmt_apy(r['real_apy']):>10} "
            f"{fmt_apy(r['price_apy_7d']):>10} {fmt_apy(r['price_apy_30d']):>10} "
            f"{fmt_apy(r['combined_apy_7d']):>10} {fmt_apy(r['combined_apy_30d']):>10} "
            f"{ge_str:>10} {nd_str:>10} {sd_str:>8}{sdq_icon}{src_mark} {bc_str:>7} {lk_str:>7} {mx_str:>10} {liq_flag:>4} "
            f"{epr_flag}{epr_str:>8} {band_str:>10}  {bv_tv:>6}  {bv} ({bv_apy} | T:{bv_take})  {rank_flag}"
        )
    print(f"\n  † burn figure from derived estimator (counter history not yet available)")

    if low_emission_results:
        print(f"\n{THIN_SEP}")
        print(f"  LOW EMISSION SUBNETS (below {MIN_TAO_EMISSION} TAO/block) — {len(low_emission_results)} subnets")
        print(THIN_SEP)
        print(f"  {'SN':<6} {'Name':<22} {'Emission':>12} {'Reserves':>12} {'Alpha Out':>14} {'Spot Price':>12} {'EMA Band':>10}")
        print(THIN_SEP)
        for r in sorted(low_emission_results, key=lambda r: r["tao_emission"] or 0, reverse=True):
            print(
                f"  SN{r['netuid']:<4} {r['name']:<22} "
                f"{(r['tao_emission'] or 0):.6f}     "
                f"{fmt_tao(r['tao_reserves']):>12} "
                f"{fmt_tao(r['alpha_outstanding']):>14} "
                f"{(r['spot_price'] or 0):.8f}   "
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
    counter_n = sum(1 for r in results if r.get("burned_source") == "COUNTER")
    est_n     = sum(1 for r in results if r.get("burned_source") == "ESTIMATOR")
    print(f"  Burn source            : {counter_n} counter, {est_n} estimator fallback")
    if eco["inflation_median"] is not None:
        print(f"  Ecosystem inflation    : median {eco['inflation_median']*100:.1f}%/yr ({eco['inflation_median']*100/12:.1f}%/mo)  "
              f"mean {eco['inflation_mean']*100:.1f}%/yr ({eco['inflation_mean']*100/12:.1f}%/mo)  "
              f"(n={eco['inflation_count']})")
    if eco["locked_total"] is not None:
        print(f"  Conviction locked      : {fmt_tao(eco['locked_total'])} α across "
              f"{eco['locked_subnets']} subnets (median float ratio {fmt_pct(eco['locked_ratio_median'])})")

    # ── Commitment matrix (v3) ────────────────────────────────────────────
    classed = [r for r in active_results if r.get("commitment_class")]
    if classed:
        counts = defaultdict(int)
        for r in classed:
            counts[r["commitment_class"]] += 1
        print(f"\n  🏰  COMMITMENT MATRIX — burn coverage x owner conviction "
              f"(FORTRESS {counts['FORTRESS']} | EXPOSED {counts['EXPOSED']} | "
              f"DORMITORY {counts['DORMITORY']} | NEITHER {counts['NEITHER']})")
        print(THIN_SEP)
        highlight = [r for r in classed if r["commitment_class"] in ("FORTRESS", "EXPOSED")]
        if highlight:
            print(f"  {'':>3} {'SN':<6} {'Name':<22} {'Class':<10} {'BurnCov':>8} {'OwnerLock':>10} {'CommLock':>10} {'Float Lk':>9} {'Defence':>8} {'Gate':>5}")
            print(THIN_SEP)
            for r in sorted(highlight, key=lambda x: -(x.get("burn_coverage") or 0)):
                print(
                    f"  {matrix_icon[r['commitment_class']]} SN{r['netuid']:<4} {r['name']:<22} {r['commitment_class']:<10} "
                    f"{(r['burn_coverage'] or 0):>7.2f}x "
                    f"{fmt_pct(r['owner_lock_ratio'], 2):>10} "
                    f"{fmt_tao(r['community_locked_alpha']):>10} "
                    f"{fmt_pct(r['locked_float_ratio'], 1):>9} "
                    f"{fmt_pct(r['supply_defence']):>8} "
                    f"{'YES' if r['gate_eligible'] else 'no':>5}"
                )

    # ── Governance exposure (v3) ──────────────────────────────────────────
    gov = [r for r in results if r.get("governance_flag")]
    if gov:
        print(f"\n  ⚔️   GOVERNANCE EXPOSURE — ownership-gate arithmetic")
        print(THIN_SEP)
        order = {"TAKEOVER_IMMINENT": 0, "TAKEOVER_WATCH": 1, "UNDEFENDED_BURNER": 2}
        for r in sorted(gov, key=lambda x: order.get(x["governance_flag"], 9)):
            gf = r["governance_flag"]
            if gf == "TAKEOVER_IMMINENT":
                icon, label = "🔴", f"non-owner leader at {r['leader_pct_of_threshold']*100:.0f}% of 18% gate"
            elif gf == "TAKEOVER_WATCH":
                icon, label = "🟡", f"non-owner leader at {r['leader_pct_of_threshold']*100:.0f}% of 18% gate"
            else:
                icon, label = "🟠", "active burns shrinking eligible alpha, no owner lock — funding the siege"
            blocks = r.get("leader_blocks_to_threshold")
            eta = f" | ~{blocks/BLOCKS_PER_DAY:.0f}d to threshold" if blocks else ""
            print(f"  {icon} SN{r['netuid']:<4} {r['name']:<22} | {gf}: {label}{eta}")

    # ── Reconciliation alarm (v3) ─────────────────────────────────────────
    recon_div = [r for r in results if r.get("recon_flag") == "DIVERGENT"]
    if recon_div:
        print(f"\n  🧮  BURN RECONCILIATION DIVERGENCE — estimator vs chain counters")
        print(THIN_SEP)
        for r in recon_div:
            rel = r.get("recon_gap_relative")
            print(f"  ⚠️  SN{r['netuid']:<4} {r['name']:<22} | estimator off by "
                  f"{rel*100:+.0f}% vs counters — investigate accounting")

    # EPR leaderboard (v2, unchanged)
    epr_candidates = sorted(
        [r for r in active_results if r["emission_price_ratio"] is not None and r["emission_price_ratio"] > 1.0],
        key=lambda r: r["emission_price_ratio"], reverse=True)
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
                  f"EMA: {r['moving_price']:.6f} | Momentum: {fmt_pct(r['momentum_30d'])} | "
                  f"Band: {r['ema_band']} | EMA basis: {r['ema_halving_days']:.0f}d")

    flow_divs = [r for r in results if r.get("flow_price_divergence")]
    if flow_divs:
        print(f"\n  ⚠️  FLOW/PRICE DIVERGENCE — Capital flow contradicts price momentum")
        print(THIN_SEP)
        for r in flow_divs:
            label = ("Price ↑ Flow ↓ — latent emission cut coming"
                     if r["flow_price_divergence"] == "PRICE_UP_FLOW_DOWN"
                     else "Price ↓ Flow ↑ — latent emission pump coming")
            print(f"  SN{r['netuid']} {r['name']:<22} | {label}")
            print(f"       Price momentum: {fmt_pct(r['momentum_30d'])} | "
                  f"Flow momentum: {fmt_pct(r['flow_momentum'])} | "
                  f"EMA inflow: {r['ema_tao_inflow']}")

    pool_divs = [r for r in results if r.get("pool_ema_divergence")]
    if pool_divs:
        print(f"\n  🔍  POOL/EMA DIVERGENCE — Price-adjusted reserves contradict EMA flow")
        print(THIN_SEP)
        for r in pool_divs:
            label = ("Real capital entering while EMA still negative — early accumulation"
                     if r["pool_ema_divergence"] == "ACCUMULATING_PRE_EMA"
                     else "Real capital leaving while EMA still positive — early distribution")
            print(f"  SN{r['netuid']} {r['name']:<22} | {label}")
            print(f"       Pool growth (adj): {r['pool_growth_real']:+.4f} | "
                  f"EMA inflow: {r['ema_tao_inflow']}")

    full_miner_burn = [r for r in active_results if r.get("miner_burn_rate") is not None and r["miner_burn_rate"] >= 0.95]
    if full_miner_burn:
        print(f"\n  ⚠️  MINER BURN — {len(full_miner_burn)} subnets at 100% miner burn")
        print(THIN_SEP)
        for r in sorted(full_miner_burn, key=lambda x: x["netuid"]):
            print(f"  🔴 SN{r['netuid']:<4} {r['name']:<22} | 100% miner burn — no incentive to produce work")

    dereg_candidates = sorted(
        [r for r in results if not r.get("immune", True) and r["moving_price"] is not None and r["moving_price"] > 0],
        key=lambda r: r["moving_price"])
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
                f"{(r['tao_emission'] or 0):>10.6f}"
            )

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
                pct = f"{r['owner_inflow_pct']*100:.0f}%" if r.get("owner_inflow_pct") is not None else "?"
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
                    icon, label = "🔴", "Large entry + price spike + EMA low — possible pump"
                elif anomaly == "ENTRY_REPEATED":
                    icon, label = "🟡", "Repeated large entries in 7 days — pattern"
                else:
                    icon, label = "🟡", "Large entry pre-EMA with prior spike — building position"
                print(f"  {icon} SN{r['netuid']} {r['name']:<22} | {label}")
                print(f"       Pool growth (adj): {r.get('pool_growth_real'):+.4f} | "
                      f"EMA inflow: {r['ema_tao_inflow']}")

# ── Ecosystem stats ────────────────────────────────────────────────────────

def ecosystem_stats(results):
    def _median(vals):
        vals = sorted(vals)
        return vals[len(vals) // 2] if vals else None
    inflation = [r["net_supply_delta"] for r in results if r["net_supply_delta"] is not None]
    defence   = [r["supply_defence"] for r in results if r["supply_defence"] is not None]
    locked    = [r["total_locked_alpha"] for r in results if r.get("total_locked_alpha")]
    locked_ratios = [r["locked_float_ratio"] for r in results if r.get("locked_float_ratio") is not None]
    matrix = defaultdict(int)
    for r in results:
        if r.get("commitment_class"):
            matrix[r["commitment_class"]] += 1
    return {
        "inflation_median": _median(inflation),
        "inflation_mean":   sum(inflation) / len(inflation) if inflation else None,
        "inflation_min":    min(inflation) if inflation else None,
        "inflation_max":    max(inflation) if inflation else None,
        "inflation_count":  len(inflation),
        "defence_median":   _median(defence),
        "defence_mean":     sum(defence) / len(defence) if defence else None,
        "defence_count":    len(defence),
        "burned_total":     sum(r["burned_tokens"] for r in results if r["burned_tokens"] is not None),
        "recycled_total":   sum(r["recycled_tokens"] for r in results if r.get("recycled_tokens") is not None),
        "locked_total":     sum(locked) if locked else None,
        "locked_subnets":   len(locked),
        "locked_ratio_median": _median(locked_ratios),
        "matrix_counts":    dict(matrix),
        "governance_flags": {f: sum(1 for r in results if r.get("governance_flag") == f)
                             for f in ("TAKEOVER_IMMINENT", "TAKEOVER_WATCH", "UNDEFENDED_BURNER")},
        "recon_divergent":  sum(1 for r in results if r.get("recon_flag") == "DIVERGENT"),
        "counter_coverage": sum(1 for r in results if r.get("burned_source") == "COUNTER"),
    }

# ── Persistence ────────────────────────────────────────────────────────────

CSV_FIELDNAMES = [
    # v2 columns (order preserved)
    "combined_apy_rank", "real_apy_rank", "emission_rank", "epr_rank", "netuid", "name", "date", "block",
    "spot_price", "moving_price", "momentum_30d", "ema_band", "ema_lag_flag", "low_emission",
    "tao_emission", "tao_reserves", "alpha_outstanding", "alpha_out_emission",
    "alpha_in_pool", "volume", "ema_tao_inflow", "pool_growth_real", "pool_ema_divergence",
    "flow_momentum",
    "flow_price_divergence", "emission_price_ratio", "emission_price_ratio_raw",
    "emission_apy", "price_apy_7d", "price_apy_30d",
    "combined_apy_7d", "combined_apy_30d", "real_apy",
    "gross_emission_rate", "net_supply_delta", "supply_days_gap",
    "supply_defence", "supply_defence_quality", "burned_tokens", "manual_burn", "miner_burn_rate",
    "liquidation_price", "liquidation_haircut",
    "owner_coldkey", "owner_stake", "owner_inflow_pct", "owner_inflow_flag",
    "large_entry_anomaly", "immune", "registered_at", "vali_miner_share",
    "best_validator_uid", "best_validator_hotkey", "best_validator_take",
    "best_validator_raw_apy", "best_validator_apy",
    "best_validator_div_pct", "best_validator_trust",
    # v3 columns (appended)
    "burned_source", "alpha_burned_cum", "alpha_recycled_cum", "recycled_tokens",
    "recon_flag", "recon_gap_relative",
    "burn_coverage", "owner_cut_emission",
    "total_locked_alpha", "locked_float_ratio",
    "owner_locked_alpha", "owner_lock_ratio", "community_locked_alpha",
    "commitment_class", "gate_eligible",
    "leader_is_owner", "leader_pct_of_threshold", "leader_blocks_to_threshold",
    "governance_flag", "basket_alpha", "basket_share", "ema_halving_days",
]

NUMERIC_FIELDS = [
    "spot_price", "moving_price", "alpha_outstanding", "alpha_in_pool",
    "tao_reserves", "emission_apy", "combined_apy_30d", "real_apy",
    "gross_emission_rate", "net_supply_delta", "supply_defence",
    "burned_tokens", "manual_burn", "liquidation_price", "liquidation_haircut",
    "ema_tao_inflow", "pool_growth_real", "flow_momentum", "emission_price_ratio",
    "vali_miner_share",
    # v3 additions
    "recycled_tokens", "burn_coverage", "total_locked_alpha",
    "locked_float_ratio", "owner_locked_alpha", "owner_lock_ratio",
    "leader_pct_of_threshold", "basket_share",
]

def persist(results, traj_entries, eco, now, block):
    """CSV + metadata + trajectory windows + historical compression + ecosystem."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_METADATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    date_str = now.strftime("%Y-%m-%d")
    ts_str   = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    traj_30d_path  = OUTPUT_DIR / "trajectory_30d.json"
    traj_90d_path  = OUTPUT_DIR / "trajectory_90d.json"
    traj_hist_path = OUTPUT_DIR / "trajectory_historical.json"
    traj_30d = load_json(traj_30d_path)
    traj_90d = load_json(traj_90d_path)

    cutoff_30 = (now - timedelta(days=TRAJECTORY_30D)).strftime("%Y-%m-%d")
    cutoff_90 = (now - timedelta(days=TRAJECTORY_90D)).strftime("%Y-%m-%d")
    _aged_out_buffer = {}

    for key, entry in traj_entries.items():
        all_hist = [e for e in traj_90d.get(key, []) if e.get("date") != date_str]
        all_hist.append(entry)
        all_hist.sort(key=lambda e: e.get("date", ""))
        aged_out = [e for e in all_hist if e.get("date", "") < cutoff_90]
        if aged_out:
            _aged_out_buffer.setdefault(key, []).extend(aged_out)
        traj_90d[key] = [e for e in all_hist if e.get("date", "") >= cutoff_90]

        hist_30 = [e for e in traj_30d.get(key, [])
                   if e.get("date", "") >= cutoff_30 and e.get("date") != date_str]
        hist_30.append(entry)
        hist_30.sort(key=lambda e: e.get("date", ""))
        traj_30d[key] = hist_30

    # CSV
    csv_path = OUTPUT_SNAPSHOT_DIR / f"staking_snapshot_{date_str}.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    # Metadata
    meta_path = OUTPUT_METADATA_DIR / f"staking_metadata_{date_str}.json"
    save_json(meta_path, {
        "date": date_str, "timestamp": ts_str, "block": block,
        "subnets_analysed": len(results),
        "subnets_active":   len([r for r in results if not r["low_emission"]]),
        "subnets_low_emission": len([r for r in results if r["low_emission"]]),
        "ema_lag_traps": [r["netuid"] for r in results if r["ema_lag_flag"]],
        "ecosystem_inflation": {
            "median_annual":  eco["inflation_median"],
            "mean_annual":    eco["inflation_mean"],
            "min_annual":     eco["inflation_min"],
            "max_annual":     eco["inflation_max"],
            "median_monthly": eco["inflation_median"] / 12 if eco["inflation_median"] is not None else None,
            "mean_monthly":   eco["inflation_mean"] / 12   if eco["inflation_mean"]   is not None else None,
            "count":          eco["inflation_count"],
        },
        "ecosystem_supply_defence": {
            "median": eco["defence_median"], "mean": eco["defence_mean"],
            "count": eco["defence_count"], "total_burned": eco["burned_total"],
            "total_recycled": eco["recycled_total"],
        },
        "ecosystem_conviction": {
            "total_locked": eco["locked_total"],
            "subnets_with_locks": eco["locked_subnets"],
            "locked_float_ratio_median": eco["locked_ratio_median"],
            "commitment_matrix": eco["matrix_counts"],
        },
        "governance_exposure": eco["governance_flags"],
        "burn_accounting": {
            "counter_coverage": eco["counter_coverage"],
            "reconciliation_divergent": eco["recon_divergent"],
        },
        "top_10_by_combined_apy_30d": [
            {
                "combined_apy_rank": r["combined_apy_rank"], "real_apy_rank": r["real_apy_rank"],
                "emission_rank": r["emission_rank"], "netuid": r["netuid"], "name": r["name"],
                "combined_apy_30d": r["combined_apy_30d"], "real_apy": r["real_apy"],
                "emission_apy": r["emission_apy"], "price_apy_30d": r["price_apy_30d"],
                "volume": r["volume"], "supply_inflation": r["net_supply_delta"],
                "gross_emission_rate": r["gross_emission_rate"],
                "net_supply_delta": r["net_supply_delta"], "supply_defence": r["supply_defence"],
                "burned_tokens": r["burned_tokens"], "burned_source": r["burned_source"],
                "burn_coverage": r["burn_coverage"], "commitment_class": r["commitment_class"],
                "locked_float_ratio": r["locked_float_ratio"],
                "liquidation_price": r["liquidation_price"],
                "liquidation_haircut": r["liquidation_haircut"],
                "ema_tao_inflow": r["ema_tao_inflow"],
                "emission_price_ratio": r["emission_price_ratio"], "epr_rank": r["epr_rank"],
                "vali_miner_share": r["vali_miner_share"], "ema_band": r["ema_band"],
                "ema_lag_flag": r["ema_lag_flag"],
                "best_validator": r["best_validator_hotkey"],
                "best_validator_apy": r["best_validator_apy"],
                "best_validator_take": r["best_validator_take"],
                "best_validator_trust": r["best_validator_trust"],
            }
            for r in results[:10]
        ],
    })

    save_json(traj_30d_path, traj_30d)
    save_json(traj_90d_path, traj_90d)

    # Historical weekly compression (v2, NUMERIC_FIELDS extended)
    traj_hist = load_json(traj_hist_path)
    if not isinstance(traj_hist, dict):
        traj_hist = {}
    for sn_key, entries in _aged_out_buffer.items():
        aged_out = [e for e in entries if e.get("date", "") < cutoff_90]
        if not aged_out:
            continue
        weeks = defaultdict(list)
        for e in aged_out:
            try:
                dt = datetime.strptime(e["date"], "%Y-%m-%d")
                weeks[dt.strftime("%G-W%V")].append(e)
            except Exception:
                continue
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

    # Ecosystem trajectory
    eco_traj_path = OUTPUT_DIR / "trajectory_ecosystem.json"
    eco_traj = load_json(eco_traj_path, default=[])
    if not isinstance(eco_traj, list):
        eco_traj = []
    eco_entry = {
        "date": date_str, "block": block,
        "inflation_median_annual":  eco["inflation_median"],
        "inflation_mean_annual":    eco["inflation_mean"],
        "inflation_median_monthly": eco["inflation_median"] / 12 if eco["inflation_median"] is not None else None,
        "inflation_mean_monthly":   eco["inflation_mean"] / 12   if eco["inflation_mean"]   is not None else None,
        "inflation_min": eco["inflation_min"], "inflation_max": eco["inflation_max"],
        "inflation_count": eco["inflation_count"],
        "defence_median": eco["defence_median"], "defence_mean": eco["defence_mean"],
        "defence_count": eco["defence_count"], "total_burned": eco["burned_total"],
        "subnets_active": len([r for r in results if not r["low_emission"]]),
        "subnets_total": len(results),
        "ema_lag_traps": sum(1 for r in results if r["ema_lag_flag"]),
        "flow_divergences": sum(1 for r in results if r.get("flow_price_divergence")),
        # v3 ecosystem fields
        "total_recycled": eco["recycled_total"],
        "total_locked": eco["locked_total"],
        "locked_subnets": eco["locked_subnets"],
        "locked_float_ratio_median": eco["locked_ratio_median"],
        "commitment_matrix": eco["matrix_counts"],
        "governance_flags": eco["governance_flags"],
        "recon_divergent": eco["recon_divergent"],
        "counter_coverage": eco["counter_coverage"],
    }
    eco_traj = [e for e in eco_traj if e.get("date") != date_str]
    eco_traj.append(eco_entry)
    eco_traj.sort(key=lambda e: e.get("date", ""))
    save_json(eco_traj_path, eco_traj)

    print(f"\n💾  Outputs saved:")
    for p in [csv_path, meta_path, traj_30d_path, traj_90d_path, traj_hist_path, eco_traj_path]:
        print(f"    {p}")
    print(f"\n{SEPARATOR}\n")
    return csv_path

# ── Entry point ────────────────────────────────────────────────────────────

def run_snapshot():
    now      = datetime.now(timezone.utc)
    ts_str   = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    print(SEPARATOR)
    print("  SUBNET STAKING SNAPSHOT v3 — Intelligence Sovereignty Research Suite")
    print("  @im_perseverance")
    print(SEPARATOR)
    print(f"\n  Connecting to Bittensor network (v11, pinned snapshot)...")

    data, block = chain.run(collect)
    print(f"  Block     : {block:,} (pinned — all reads from this block)")
    print(f"  Timestamp : {ts_str}")
    print(f"  Subnets   : {len(data['pools'])}")
    print(f"  Governance: owner_cut={data['params']['owner_cut']:.2%} "
          f"tao_weight={data['params']['tao_weight']:.2%}\n")

    traj_90d = load_json(OUTPUT_DIR / "trajectory_90d.json")
    results, traj_entries = analyse(data, traj_90d, now)
    eco = ecosystem_stats(results)
    print_report(results, eco)
    persist(results, traj_entries, eco, now, block)


if __name__ == "__main__":
    run_snapshot()
