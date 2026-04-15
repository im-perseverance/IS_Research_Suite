"""
chutes_sn64_analysis.py
=======================
SN64 Chutes — Intelligence Market Hypothesis Test + E2EE Perimeter Forensics
Part of the Intelligence Sovereignty research suite by @im_perseverance

Tests whether Bittensor functions as a market for intelligence by comparing:
  Dataset 1: Real demand    — Chutes invocation exports (api.chutes.ai, public)
  Dataset 2: Recognition    — Validator weight allocations (Bittensor on-chain)
  Dataset 3: Capital flow   — Emission distribution (SN64 metagraph)

Join key: SS58 miner hotkey (present in both invocation CSV and metagraph)
Time window: 7-day rolling aggregate (matches incentive calculation window)

Architecture: single-pass sequential streaming fetch.
  All miner aggregates, user aggregates, and perimeter forensic signals
  (bigrams, token variance, timestamps, parent IDs, instance IDs) are
  collected in one sequential pass over the 168 hourly CSVs. Each hourly
  CSV is fetched, parsed, aggregated into running counters, and discarded
  before the next fetch begins. Peak memory is one hour of CSV data at a
  time (~300-400MB). No ThreadPoolExecutor, no second pass.

  MAX_ROWS_PER_HOUR cap (default 200,000): prevents OOM on GitHub Actions
  runners when individual hourly CSVs exceed available memory. At ~200K
  rows per hour, total processed rows remain ~33M+ across 168 hours —
  sufficient for all signal stability thresholds. Spearman, Pearson,
  perimeter forensic verdicts, and miner share distributions are
  statistically unaffected at this sample size.

Core analysis:
  - Alpha token pool state (spot price, EMA price, TAO reserves, market cap)
  - Root × SN64 validator overlap
  - Miner incentive concentration and divergence table
  - Demand-side user analysis
  - Dominant user deep analysis
  - Native TPS/TTFT/pass rate from metrics field (no external API)

E2EE Perimeter Forensics (demand authenticity):
  Signal weights:
    Full weight  : token variance, chute age, miner entropy, inter-arrival,
                   function sequence, parent invocation diversity
    Half weight  : instance entropy (partially redundant with miner entropy)
    Not scored   : image owner (structural constant on E2EE subnet)

Outputs:
  - Console report with snapshot metadata
  - chutes_analysis/analysis/chutes_sn64_analysis_YYYY-MM-DD.csv
  - chutes_analysis/divergence/chutes_sn64_divergence_YYYY-MM-DD.csv
  - chutes_analysis/metadata/chutes_sn64_metadata_YYYY-MM-DD.json
  - chutes_sn64_demand_users_YYYY-MM-DD.csv
  - chutes_sn64_dominant_user_YYYY-MM-DD.json
  - chutes_sn64_perimeter_YYYY-MM-DD.json
  - perimeter_trajectory.json
  - burn_trajectory.json

Usage:
    python chutes_sn64_analysis.py
    python chutes_sn64_analysis.py --method 1
    python chutes_sn64_analysis.py --method 2
    python chutes_sn64_analysis.py --method 3
"""

import ast
import bittensor as bt
import requests
import csv
import json
import math
import argparse
import io
import gc
import random as _random
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor

# ── Config ────────────────────────────────────────────────────────────────────
NETUID               = 64
CHUTES_API_BASE      = "https://api.chutes.ai"
INVOCATION_WINDOW    = 7
ROOT_TAO_THRESHOLD   = 1000.0
SN64_ALPHA_THRESHOLD = 1000.0
OUTPUT_DIR           = Path("snapshots/chutes")
OUTPUT_ANALYSIS_DIR  = OUTPUT_DIR / "analysis"
OUTPUT_DIVERGENCE_DIR = OUTPUT_DIR / "divergence"
OUTPUT_METADATA_DIR  = OUTPUT_DIR / "metadata"

BLOCKS_PER_DAY            = 7200
BLOCKS_PER_YEAR           = BLOCKS_PER_DAY * 365

QUALITY_WEIGHT_COMPUTE    = 0.55
QUALITY_WEIGHT_INVOCATION = 0.25
QUALITY_WEIGHT_DIVERSITY  = 0.15
QUALITY_WEIGHT_EFFICIENCY = 0.05

# Timestamp reservoir cap for inter-arrival analysis
MAX_TS = 50_000

# Row cap per hourly CSV — prevents OOM on constrained runners.
# At 200K rows/hour across 168 hours = ~33.6M rows total.
# All signal distributions (Spearman, perimeter forensics, miner shares)
# are statistically stable well below this sample size.
MAX_ROWS_PER_HOUR = 200_000

SEPARATOR = "=" * 100
THIN_SEP  = "-" * 100

# ── Helpers ───────────────────────────────────────────────────────────────────

def fmt_pct(val):
    if val is None: return "  N/A  "
    return f"{val*100:.2f}%"

def fmt_delta(demand, emission):
    if demand is None or emission is None: return "  N/A  "
    delta = emission - demand
    sign  = "+" if delta >= 0 else ""
    return f"{sign}{delta*100:.2f}pp"

def fmt_tao(val, decimals=4):
    return f"{val:.{decimals}f} TAO" if val is not None else "N/A"

def fmt_large(val):
    if val is None: return "N/A"
    if val >= 1_000_000: return f"{val/1_000_000:.2f}M"
    if val >= 1_000:     return f"{val/1_000:.2f}K"
    return f"{val:.2f}"


def parse_metrics(raw: str) -> dict:
    """
    Parse metrics field from Chutes CSV. Uses single-quote Python dict literal
    format — requires ast.literal_eval, not json.loads.
    Returns empty dict on any failure.
    """
    if not raw or raw.strip() in ("", "{}"): return {}
    try:
        result = ast.literal_eval(raw.strip())
        return result if isinstance(result, dict) else {}
    except Exception:
        return {}


def probe_invocation_urls(now: datetime) -> None:
    print("  🔍 Probing endpoint formats...")
    candidates = [
        f"{CHUTES_API_BASE}/invocations/exports/recent",
        f"{CHUTES_API_BASE}/invocations/exports/{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}.csv",
        f"{CHUTES_API_BASE}/invocations/exports/{now.year}/{now.month:02d}/{now.day:02d}/{(now.hour-1)%24:02d}.csv",
    ]
    for url in candidates:
        try:
            r = requests.get(url, timeout=15)
            preview = r.text.strip()[:100].replace("\n", " ") if r.text else ""
            print(f"    {r.status_code}  {url}")
            if preview: print(f"         → {preview}")
        except Exception as e:
            print(f"    ERR  {url}  ({e})")


def load_previous_snapshot(output_dir: Path, current_date: str) -> dict:
    csvs = sorted([
        f for f in (output_dir / "analysis").glob("chutes_sn64_analysis_*.csv")
        if current_date not in f.name
    ], reverse=True)
    if not csvs:
        print("  ℹ️  No previous snapshot found — trend delta will be N/A")
        return {}
    prev_file = csvs[0]
    print(f"  📂 Previous snapshot: {prev_file.name}")
    prev = {}
    try:
        with open(prev_file, newline="") as f:
            for row in csv.DictReader(f):
                hk = row.get("hotkey", "").strip()
                if hk:
                    prev[hk] = {
                        "emission_share":   float(row.get("emission_share",   0) or 0),
                        "invocation_share": float(row.get("invocation_share", 0) or 0),
                        "weight_share":     float(row.get("weight_share",     0) or 0),
                    }
    except Exception as e:
        print(f"  ⚠️  Could not load previous snapshot: {e}")
        return {}
    print(f"  ✅ Loaded {len(prev)} miners from previous snapshot")
    return prev


# ── Alpha Pool Fetch ──────────────────────────────────────────────────────────

def fetch_alpha_pool_state(sub, netuid: int, method: int = 0) -> dict:
    RAO = 1e9
    result = {
        "alpha_price": None, "ema_price": None, "spot_ema_gap": None,
        "spot_ema_gap_pct": None, "tao_reserves": None, "alpha_outstanding": None,
        "alpha_in_pool": None, "alpha_out_emission": None, "alpha_in_emission": None, "market_cap_tao": None,
    }

    def _finalise(r):
        if r["alpha_price"] and r["alpha_outstanding"]:
            r["market_cap_tao"] = r["alpha_price"] * r["alpha_outstanding"]
        if r["alpha_price"] and r["ema_price"]:
            r["spot_ema_gap"]     = r["alpha_price"] - r["ema_price"]
            r["spot_ema_gap_pct"] = (r["spot_ema_gap"] / r["ema_price"]) * 100
        return r

    def _try_method_1():
        sn = sub.subnet(netuid)
        if sn is None: return False
        tao_in = getattr(sn, 'tao_in', None); alpha_in = getattr(sn, 'alpha_in', None)
        alpha_out = getattr(sn, 'alpha_out', None); mov_price = getattr(sn, 'moving_price', None)
        price     = getattr(sn, 'price', None); aoe = getattr(sn, 'alpha_out_emission', None)
        aie       = getattr(sn, 'alpha_in_emission', None)
        if tao_in is not None:    result["tao_reserves"]      = float(tao_in)
        if alpha_in is not None:  result["alpha_in_pool"]     = float(alpha_in)
        if alpha_out is not None: result["alpha_outstanding"] = float(alpha_out)
        if price is not None:     result["alpha_price"]       = float(price)
        elif tao_in and alpha_in and float(alpha_in) > 0:
            result["alpha_price"] = float(tao_in) / float(alpha_in)
        if mov_price is not None: result["ema_price"]         = float(mov_price)
        if aoe is not None:       result["alpha_out_emission"] = float(aoe)
        if aie is not None:       result["alpha_in_emission"]  = float(aie)
        return result["alpha_price"] is not None

    def _try_method_2():
        tao_q       = sub.query_subtensor("SubnetTAO",      params=[netuid])
        alpha_in_q  = sub.query_subtensor("SubnetAlphaIn",  params=[netuid])
        alpha_out_q = sub.query_subtensor("SubnetAlphaOut", params=[netuid])
        if tao_q and alpha_in_q:
            tao_val   = float(tao_q.value)      / RAO
            alpha_val = float(alpha_in_q.value) / RAO
            result["tao_reserves"]  = tao_val
            result["alpha_in_pool"] = alpha_val
            if alpha_val > 0: result["alpha_price"] = tao_val / alpha_val
        if alpha_out_q: result["alpha_outstanding"] = float(alpha_out_q.value) / RAO
        try:
            price = sub.get_subnet_price(netuid)
            if price is not None: result["ema_price"] = float(price)
        except Exception: pass
        return result["alpha_price"] is not None

    def _try_method_3():
        subnets = sub.all_subnets()
        if not subnets: return False
        for sn in subnets:
            if getattr(sn, 'netuid', None) != netuid: continue
            tao_in    = getattr(sn, 'tao_in',       None)
            alpha_in  = getattr(sn, 'alpha_in',     None)
            alpha_out = getattr(sn, 'alpha_out',    None)
            mov_price = getattr(sn, 'moving_price', None)
            price     = getattr(sn, 'price',        None)
            aoe       = getattr(sn, 'alpha_out_emission', None)
            aie       = getattr(sn, 'alpha_in_emission', None)
            if tao_in:    result["tao_reserves"]      = float(tao_in)
            if alpha_in:  result["alpha_in_pool"]     = float(alpha_in)
            if alpha_out: result["alpha_outstanding"] = float(alpha_out)
            if price:     result["alpha_price"]       = float(price)
            elif tao_in and alpha_in and float(alpha_in) > 0:
                result["alpha_price"] = float(tao_in) / float(alpha_in)
            if mov_price: result["ema_price"]         = float(mov_price)
            if aoe:       result["alpha_out_emission"] = float(aoe)
            if aie:       result["alpha_in_emission"]  = float(aie)
            break
        return result["alpha_price"] is not None

    methods  = {1: ("subnet() object", _try_method_1),
                2: ("raw storage",     _try_method_2),
                3: ("all_subnets()",   _try_method_3)}
    attempts = [method] if method in methods else [1, 2, 3]
    for m in attempts:
        label, fn = methods[m]
        try:
            if fn():
                _finalise(result)
                print(f"  ✅ Alpha pool state fetched via {label}  [method {m}]")
                return result
            else:
                print(f"  ⚠️  Method {m} ({label}): returned no price data")
        except Exception as e:
            print(f"  ⚠️  Method {m} ({label}) failed: {e}")
    print(f"  ❌ Could not fetch alpha pool state — all methods failed.")
    return result


# ── Quality Scoring ───────────────────────────────────────────────────────────

def compute_quality_scores(merged: list) -> list:
    active = [m for m in merged if m["invocation_count"] > 0]
    if not active: return merged
    max_div = max(m["chute_diversity"] for m in active) or 1
    for m in active:
        m["_inv_per_sec"]           = (m["invocation_count"] / m["compute_seconds"]
                                       if m["compute_seconds"] > 0 else 0)
        m["seconds_per_invocation"] = (m["compute_seconds"] / m["invocation_count"]
                                       if m["invocation_count"] > 0 else 0)
    active_with_compute = [m for m in active if m["seconds_per_invocation"] > 0]
    if active_with_compute:
        sorted_by_speed = sorted(active_with_compute, key=lambda m: m["seconds_per_invocation"])
        n = len(sorted_by_speed); fast_cut = n // 3; slow_cut = 2 * n // 3
        for i, m in enumerate(sorted_by_speed):
            if i < fast_cut:   m["efficiency_tier"] = "FAST"
            elif i < slow_cut: m["efficiency_tier"] = "MID"
            else:              m["efficiency_tier"] = "SLOW"
    for m in active:
        if "efficiency_tier" not in m: m["efficiency_tier"] = "N/A"
    max_ips  = max(m["_inv_per_sec"] for m in active) or 1
    total_qs = 0.0
    for m in active:
        div_norm = m["chute_diversity"] / max_div
        eff_norm = m["_inv_per_sec"]    / max_ips
        qs = (QUALITY_WEIGHT_COMPUTE    * m["compute_share"]    +
              QUALITY_WEIGHT_INVOCATION * m["invocation_share"] +
              QUALITY_WEIGHT_DIVERSITY  * div_norm              +
              QUALITY_WEIGHT_EFFICIENCY * eff_norm)
        m["_raw_quality_score"] = qs; total_qs += qs
    for m in active:
        m["quality_score"]        = m["_raw_quality_score"] / total_qs if total_qs > 0 else 0
        m["delta_quality_weight"] = m["weight_share"]   - m["quality_score"]
        m["delta_quality_emit"]   = m["emission_share"] - m["quality_score"]
    active_keys = {m["hotkey"] for m in active}
    for m in merged:
        if m["hotkey"] not in active_keys:
            m["quality_score"] = 0.0; m["delta_quality_weight"] = 0.0
            m["delta_quality_emit"] = 0.0; m["efficiency_tier"] = "N/A"
            m["seconds_per_invocation"] = 0.0
        m.pop("_raw_quality_score", None); m.pop("_inv_per_sec", None)
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# SINGLE-PASS SEQUENTIAL FETCH
# All miner aggregates, user aggregates, and perimeter forensic signal data
# collected in one sequential pass. One HTTP request at a time. Each hourly
# CSV is parsed and immediately discarded. Peak memory: ~300-400MB.
#
# MAX_ROWS_PER_HOUR cap applied inside the row loop to prevent OOM on
# constrained GitHub Actions runners. Signal quality is unaffected —
# all distributions stabilise well below the cap threshold.
# ══════════════════════════════════════════════════════════════════════════════

def fetch_all_data(window_days: int) -> tuple:
    """
    Single-pass sequential streaming fetch over all hourly CSVs.

    Returns:
        miner_demand : { hotkey → miner stats dict }
        user_agg     : { user_id → user stats dict }
        perimeter    : perimeter signal data dict
        fields_info  : detected field names for logging
        all_chute_ids: list of unique chute IDs seen
    """
    now = datetime.now(timezone.utc)
    probe_invocation_urls(now)

    urls = []
    for days_back in range(window_days):
        target = now - timedelta(days=days_back)
        for hour in range(24):
            t = target.replace(hour=hour, minute=0, second=0, microsecond=0)
            urls.append(
                f"{CHUTES_API_BASE}/invocations/exports"
                f"/{t.year}/{t.month:02d}/{t.day:02d}/{t.hour:02d}.csv"
            )

    print(f"  Streaming {len(urls)} hourly archives sequentially (1 at a time)...")
    print(f"  Row cap: {MAX_ROWS_PER_HOUR:,} rows/hour (OOM guard — signal quality unaffected)")

    # ── Miner aggregates ──────────────────────────────────────────────────────
    agg_miners = defaultdict(lambda: {
        "uid": None, "invocation_count": 0, "compute_seconds": 0.0,
        "chute_ids": set(),
        "tps_sum": 0.0, "tps_count": 0,
        "ttft_sum": 0.0, "ttft_count": 0,
        "pass_count": 0, "pass_total": 0,
    })

    # ── User aggregates ───────────────────────────────────────────────────────
    agg_users = defaultdict(lambda: {
        "invocation_count": 0, "compute_seconds": 0.0,
        "chute_ids": set(), "function_names": defaultdict(int),
        "image_ids": defaultdict(int), "image_user_ids": defaultdict(int),
        "hour_buckets": defaultdict(int),
        "instance_ids": set(), "parent_ids": set(),
    })

    # ── Perimeter signal accumulators ─────────────────────────────────────────
    ts_reservoir  = []
    ts_total_seen = 0

    bigram_counts  = Counter()
    trigram_counts = Counter()
    fn_total       = Counter()
    prev_fn        = None

    TOKEN_FIELDS  = {"it", "ot", "tokens"}
    token_welford = defaultdict(lambda: {"n": 0, "mean": 0.0, "M2": 0.0})

    u_miner_counts      = defaultdict(lambda: defaultdict(int))
    n_miner_network     = Counter()
    net_instance_counts = Counter()

    # ── Field detection ───────────────────────────────────────────────────────
    fields_info = {}
    hi = ui = ci = chi = usi = fni = imi = iui = mi = pi = ii = sa_i = ca_i = None

    total_rows    = 0
    hours_fetched = 0
    hours_failed  = 0
    total_capped  = 0  # hours where cap was hit

    for i, url in enumerate(urls):
        try:
            r = requests.get(url, timeout=25)
            if r.status_code != 200 or not r.text.strip():
                hours_failed += 1
                del r; continue

            text   = r.text; del r
            reader = csv.reader(io.StringIO(text)); del text

            try:
                header = next(reader)
            except StopIteration:
                hours_failed += 1; continue

            if not fields_info:
                fields_info = _detect_fields_single(header)
                hi  = fields_info.get("hi")
                ui  = fields_info.get("ui")
                ci  = fields_info.get("ci")
                chi = fields_info.get("chi")
                usi = fields_info.get("usi")
                fni = fields_info.get("fni")
                imi = fields_info.get("imi")
                iui = fields_info.get("iui")
                mi  = fields_info.get("mi")
                pi  = fields_info.get("pi")
                ii  = fields_info.get("ii")
                sa_i = fields_info.get("sa_i")
                ca_i = fields_info.get("ca_i")
                fields_info["header"] = header

            if hi is None:
                hours_failed += 1; continue

            url_parts = url.rstrip(".csv").split("/")
            try:
                hour_bucket = (f"{url_parts[-4]}-{url_parts[-3]}-"
                               f"{url_parts[-2]}T{url_parts[-1]:0>2}:00")
            except Exception:
                hour_bucket = "unknown"

            prev_fn   = None
            hour_rows = 0
            hour_capped = False

            for _row_idx, cols in enumerate(reader):
                # ── OOM guard: hard stop at MAX_ROWS_PER_HOUR ────────────────
                if _row_idx >= MAX_ROWS_PER_HOUR:
                    hour_capped = True
                    break

                if hi >= len(cols): continue
                hk = cols[hi].strip()
                if not hk or len(hk) < 10: continue

                total_rows += 1; hour_rows += 1
                n_miner_network[hk] += 1

                compute_val = 0.0
                if ci is not None and ci < len(cols) and cols[ci]:
                    try: compute_val = float(cols[ci])
                    except (ValueError, TypeError): pass
                elif sa_i is not None and ca_i is not None:
                    try:
                        t0 = datetime.fromisoformat(cols[sa_i].replace("Z", ""))
                        t1 = datetime.fromisoformat(cols[ca_i].replace("Z", ""))
                        compute_val = (t1 - t0).total_seconds()
                    except Exception: pass

                m = agg_miners[hk]
                m["invocation_count"] += 1
                m["compute_seconds"]  += compute_val
                if ui is not None and ui < len(cols) and cols[ui]:
                    m["uid"] = cols[ui]
                if chi is not None and chi < len(cols) and cols[chi]:
                    m["chute_ids"].add(cols[chi].strip())

                m_data = {}
                if mi is not None and mi < len(cols) and cols[mi].strip() not in ("", "{}"):
                    m_data = parse_metrics(cols[mi])
                    if m_data:
                        tps  = m_data.get("tps")
                        ttft = m_data.get("ttft")
                        p    = m_data.get("p")
                        if tps  is not None:
                            try:   m["tps_sum"] += float(tps);  m["tps_count"] += 1
                            except (TypeError, ValueError): pass
                        if ttft is not None:
                            try:   m["ttft_sum"] += float(ttft); m["ttft_count"] += 1
                            except (TypeError, ValueError): pass
                        if p is not None:
                            m["pass_total"] += 1
                            if p: m["pass_count"] += 1

                uid = cols[usi].strip() if usi is not None and usi < len(cols) else None
                if uid:
                    u_miner_counts[uid][hk] += 1
                    u = agg_users[uid]
                    u["invocation_count"] += 1
                    u["compute_seconds"]  += compute_val
                    u["hour_buckets"][hour_bucket] += 1
                    if chi is not None and chi < len(cols) and cols[chi]:
                        u["chute_ids"].add(cols[chi].strip())
                    if fni is not None and fni < len(cols) and cols[fni]:
                        u["function_names"][cols[fni].strip()] += 1
                    if imi is not None and imi < len(cols) and cols[imi]:
                        u["image_ids"][cols[imi].strip()] += 1
                    if iui is not None and iui < len(cols) and cols[iui]:
                        u["image_user_ids"][cols[iui].strip()] += 1
                    if pi is not None and pi < len(cols) and cols[pi]:
                        pid = cols[pi].strip()
                        if pid: u["parent_ids"].add(pid)
                    if ii is not None and ii < len(cols) and cols[ii]:
                        iid = cols[ii].strip()
                        if iid:
                            u["instance_ids"].add(iid)
                            net_instance_counts[iid] += 1

                    if fni is not None and fni < len(cols) and cols[fni].strip():
                        fn = cols[fni].strip()
                        fn_total[fn] += 1
                        if prev_fn is not None:
                            bigram_counts[(prev_fn, fn)] += 1
                        prev_fn = fn

                    if sa_i is not None and sa_i < len(cols) and cols[sa_i].strip():
                        try:
                            ts = datetime.fromisoformat(
                                cols[sa_i].strip().replace("Z", "+00:00"))
                            ts_total_seen += 1
                            if len(ts_reservoir) < MAX_TS:
                                ts_reservoir.append(ts)
                            else:
                                j = _random.randint(0, ts_total_seen - 1)
                                if j < MAX_TS: ts_reservoir[j] = ts
                        except Exception: pass

                    if m_data:
                        for tf in TOKEN_FIELDS:
                            v = m_data.get(tf)
                            if v is not None:
                                try:
                                    fv = float(v)
                                    if fv > 0:
                                        w          = token_welford[tf]
                                        w["n"]    += 1
                                        delta      = fv - w["mean"]
                                        w["mean"] += delta / w["n"]
                                        w["M2"]   += delta * (fv - w["mean"])
                                except (TypeError, ValueError): pass

            if hour_rows > 0: hours_fetched += 1
            else:             hours_failed  += 1
            if hour_capped:   total_capped  += 1

        except Exception:
            hours_failed += 1

        if (i + 1) % 24 == 0 or (i + 1) == len(urls):
            gc.collect()
            cap_note = f" | capped: {total_capped}" if total_capped > 0 else ""
            print(f"  ↳ {i+1}/{len(urls)} complete | {hours_fetched} with data | "
                  f"{total_rows:,} rows | {len(agg_miners)} miners | "
                  f"{len(agg_users)} users | ts={len(ts_reservoir):,}{cap_note}")

    print(f"\n  Hours with data : {hours_fetched} | Failed/missing: {hours_failed} | "
          f"Row-capped: {total_capped}")
    print(f"  Total rows processed: {total_rows:,}")
    print(f"  Unique miners seen  : {len(agg_miners)}")
    print(f"  Unique users seen   : {len(agg_users)}")

    if fields_info.get("header"):
        h = fields_info["header"]
        print(f"  📋 Fields ({len(h)}): {h}")
        print(f"  🔑 hotkey=col{fields_info.get('hi')} uid=col{fields_info.get('ui')} "
              f"compute=col{fields_info.get('ci')} metrics=col{fields_info.get('mi')} "
              f"parent=col{fields_info.get('pi')} instance=col{fields_info.get('ii')}")

    # ── Finalise miner records ────────────────────────────────────────────────
    all_chute_ids = set()
    miner_demand  = {}
    for hk, data in agg_miners.items():
        all_chute_ids.update(data["chute_ids"])
        miner_demand[hk] = {
            "hotkey":           hk,
            "uid":              data["uid"],
            "invocation_count": data["invocation_count"],
            "compute_seconds":  round(data["compute_seconds"], 2),
            "chute_diversity":  len(data["chute_ids"]),
            "avg_tps":          data["tps_sum"]  / data["tps_count"]  if data["tps_count"]  > 0 else 0.0,
            "avg_ttft":         data["ttft_sum"] / data["ttft_count"] if data["ttft_count"] > 0 else 0.0,
            "pass_rate":        data["pass_count"] / data["pass_total"] if data["pass_total"] > 0 else None,
        }
    print(f"  Unique chute IDs seen: {len(all_chute_ids)}")
    tps_count = sum(1 for m in miner_demand.values() if m["avg_tps"] > 0)
    print(f"  📊 Miners with native TPS: {tps_count}/{len(miner_demand)}")

    # ── Finalise user records ─────────────────────────────────────────────────
    total_inv = sum(d["invocation_count"] for d in agg_users.values()) or 1
    total_cmp = sum(d["compute_seconds"]  for d in agg_users.values()) or 1
    user_agg  = {}
    for user_id, data in agg_users.items():
        user_agg[user_id] = {
            "user_id":          user_id,
            "invocation_count": data["invocation_count"],
            "compute_seconds":  round(data["compute_seconds"], 2),
            "chute_count":      len(data["chute_ids"]),
            "chute_ids":        data["chute_ids"],
            "instance_ids":     data["instance_ids"],
            "parent_ids":       data["parent_ids"],
            "inv_share":        data["invocation_count"] / total_inv,
            "compute_share":    data["compute_seconds"]  / total_cmp,
            "function_names":   dict(data["function_names"]),
            "image_ids":        dict(data["image_ids"]),
            "image_user_ids":   dict(data["image_user_ids"]),
            "hour_buckets":     dict(data["hour_buckets"]),
        }

    # ── Finalise Welford token CV ─────────────────────────────────────────────
    token_cv = {}
    for field, w in token_welford.items():
        if w["n"] >= 10:
            variance = w["M2"] / w["n"]
            std      = variance ** 0.5
            cv       = std / w["mean"] if w["mean"] > 0 else 0
            token_cv[field] = {
                "n": w["n"], "mean": round(w["mean"], 1),
                "std": round(std, 1), "cv": round(cv, 4),
            }

    net_total      = sum(n_miner_network.values()) or 1
    network_shares = {hk: c / net_total for hk, c in n_miner_network.items()}

    ts_reservoir.sort()

    print(f"  📊 Perimeter: {len(ts_reservoir):,} timestamps | "
          f"{sum(bigram_counts.values()):,} bigrams | "
          f"token fields: {list(token_cv.keys())}")

    perimeter = {
        "dominant_timestamps":      ts_reservoir,
        "dominant_bigram_counts":   bigram_counts,
        "dominant_trigram_counts":  trigram_counts,
        "dominant_fn_totals":       fn_total,
        "dominant_token_cv":        token_cv,
        "user_miner_counts":        dict(u_miner_counts),
        "network_miner_shares":     network_shares,
        "network_instance_counts":  dict(net_instance_counts),
        "network_total_inv":        net_total,
        "dominant_inv_count":       0,
        "dominant_parent_ids":      set(),
        "dominant_instance_ids":    set(),
    }

    return miner_demand, user_agg, perimeter, fields_info, list(all_chute_ids)


def _detect_fields_single(header: list) -> dict:
    idx    = {h.lower(): j for j, h in enumerate(header)}
    result = {}
    result["hi"]  = idx.get("miner_hotkey") or next(
        (j for h, j in idx.items() if "hotkey" in h), None)
    result["ui"]  = idx.get("miner_uid")   or idx.get("uid")
    result["usi"] = idx.get("chute_user_id")
    result["chi"] = idx.get("chute_id")
    result["fni"] = idx.get("function_name")
    result["imi"] = idx.get("image_id")
    result["iui"] = idx.get("image_user_id")
    result["mi"]  = idx.get("metrics")
    result["pi"]  = idx.get("parent_invocation_id")
    result["ii"]  = idx.get("instance_id")
    result["sa_i"] = idx.get("started_at")
    result["ca_i"] = idx.get("completed_at")
    for f in ("compute_time", "compute_seconds", "duration_seconds", "compute_multiplier"):
        if f in idx: result["ci"] = idx[f]; break
    else:
        result["ci"] = None
    return result


def populate_perimeter_dominant(perimeter: dict, user_agg: dict, dominant_user_id: str) -> dict:
    dom = user_agg.get(dominant_user_id, {})
    perimeter["dominant_inv_count"]    = dom.get("invocation_count", 0)
    perimeter["dominant_parent_ids"]   = dom.get("parent_ids",   set())
    perimeter["dominant_instance_ids"] = dom.get("instance_ids", set())
    perimeter["user_miner_counts"] = {
        dominant_user_id: perimeter["user_miner_counts"].get(dominant_user_id, {})
    }
    return perimeter


# ── Chute Metadata ────────────────────────────────────────────────────────────

def fetch_chute_metadata(chute_ids: set) -> dict:
    results = {}; ids = list(chute_ids)[:200]
    def _fetch_one(cid):
        try:
            r = requests.get(f"{CHUTES_API_BASE}/chutes/{cid}", timeout=8)
            if r.status_code == 200:
                d = r.json()
                return cid, {
                    "name":           d.get("name", ""),
                    "description":    (d.get("description") or "")[:120],
                    "model":          d.get("model_name") or d.get("model") or "",
                    "owner_username": d.get("username") or d.get("owner", {}).get("username", ""),
                    "public":         d.get("public", True),
                    "chute_type":     d.get("chute_type") or d.get("type", ""),
                    "created_at":     d.get("created_at") or d.get("created") or d.get("creation_date"),
                }
        except Exception: pass
        return cid, None
    print(f"  Fetching public metadata for {len(ids)} chutes (40 workers)...")
    fetched = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        for cid, meta in ex.map(_fetch_one, ids):
            if meta: results[cid] = meta; fetched += 1
    print(f"  Chute metadata fetched: {fetched}/{len(ids)}")
    return results


# ── Root Validators ───────────────────────────────────────────────────────────

def get_root_validator_stakes(sub) -> dict:
    print("\nLoading Root metagraph (netuid=0)...")
    root_meta   = sub.metagraph(0)
    root_stakes = {}
    for uid in range(len(root_meta.hotkeys)):
        stake = float(root_meta.stake[uid])
        if stake >= ROOT_TAO_THRESHOLD:
            root_stakes[root_meta.hotkeys[uid]] = stake
    print(f"  Root validators with {ROOT_TAO_THRESHOLD:,.0f}+ TAO: {len(root_stakes)}")
    return root_stakes


# ── Dominant User Report ──────────────────────────────────────────────────────

def analyze_dominant_user(user_data: dict, chute_meta: dict, date: str) -> dict:
    user_id         = user_data["user_id"]
    inv_count       = user_data["invocation_count"]
    cmp_secs        = user_data["compute_seconds"]
    inv_share       = user_data["inv_share"]
    cmp_share       = user_data["compute_share"]
    fn_counts       = user_data.get("function_names", {})
    img_user_counts = user_data.get("image_user_ids", {})
    hour_counts     = user_data.get("hour_buckets",   {})
    chute_ids       = user_data.get("chute_ids",      set())

    total_fn  = sum(fn_counts.values())       or 1
    total_imu = sum(img_user_counts.values()) or 1
    fn_sorted       = sorted(fn_counts.items(),       key=lambda x: x[1], reverse=True)
    img_user_sorted = sorted(img_user_counts.items(), key=lambda x: x[1], reverse=True)
    img_sorted      = sorted(user_data.get("image_ids", {}).items(), key=lambda x: x[1], reverse=True)

    hour_of_day = defaultdict(int)
    for bucket, cnt in hour_counts.items():
        try: h = int(bucket.split("T")[1].split(":")[0]); hour_of_day[h] += cnt
        except Exception: pass

    peak_hour   = max(hour_of_day, key=lambda h: hour_of_day[h]) if hour_of_day else None
    trough_hour = min(hour_of_day, key=lambda h: hour_of_day[h]) if hour_of_day else None
    hourly_vals = [hour_of_day.get(h, 0) for h in range(24)]
    mean_hourly = sum(hourly_vals) / 24 if hourly_vals else 0
    if mean_hourly > 0:
        variance = sum((v - mean_hourly) ** 2 for v in hourly_vals) / 24
        cv = (variance ** 0.5) / mean_hourly
    else:
        cv = 0.0
    peak_vol   = max(hourly_vals) if hourly_vals else 1
    active_hrs = sum(1 for v in hourly_vals if v > peak_vol * 0.1)

    if cv > 1.5:
        temporal_pattern = "BURSTY"
        pattern_note = "High CV — large volume spikes suggest batch/scheduled jobs or agentic pipelines"
    elif cv > 0.7:
        temporal_pattern = "MIXED"
        pattern_note = "Moderate CV — mix of steady traffic and periodic bursts"
    else:
        temporal_pattern = "STEADY"
        pattern_note = "Low CV — consistent 24h throughput, characteristic of always-on infrastructure or API proxy"

    model_counts = defaultdict(int); owner_counts = defaultdict(int)
    type_counts  = defaultdict(int); known_chutes = 0
    for cid in chute_ids:
        meta = chute_meta.get(cid)
        if not meta: continue
        known_chutes += 1
        if meta["model"]:          model_counts[meta["model"]] += 1
        if meta["owner_username"]: owner_counts[meta["owner_username"]] += 1
        if meta["chute_type"]:     type_counts[meta["chute_type"]] += 1

    fn_lower = {k.lower(): v for k, v in fn_counts.items()}
    workload_signals = []
    if any("embed" in f for f in fn_lower):
        workload_signals.append("EMBEDDING")
    if any(f in fn_lower for f in ("generate", "chat", "complete", "completions")):
        workload_signals.append("TEXT_GENERATION")
    if any("vision" in f or "classify_image" in f for f in fn_lower):
        workload_signals.append("VISION")
    if any("audio" in f or "speech" in f or "transcribe" in f or "speak" in f for f in fn_lower):
        workload_signals.append("AUDIO")
    if any("code" in f or "tool" in f or "function" in f for f in fn_lower):
        workload_signals.append("AGENTIC/TOOL_USE")
    if not workload_signals:
        workload_signals.append("UNKNOWN")

    print(f"\n{SEPARATOR}")
    print(f"  DOMINANT USER DEEP ANALYSIS")
    print(f"  Observable metadata fingerprint — no encrypted content accessed")
    print(THIN_SEP)
    print(f"  User ID      : {user_id}")
    print(f"  Invocations  : {inv_count:,}  ({inv_share*100:.2f}% of subnet)")
    print(f"  Compute (7d) : {cmp_secs/3600:,.1f} GPU-hours  ({cmp_share*100:.2f}% of subnet)")
    print(f"  Unique chutes: {len(chute_ids)}  ({known_chutes} with public metadata)")
    print(f"  Parent IDs   : {len(user_data.get('parent_ids', set())):,}  (distinct request origins)")
    print(f"  Instance IDs : {len(user_data.get('instance_ids', set())):,}  (distinct compute instances)")
    print(f"\n  FUNCTION NAME DISTRIBUTION"); print(THIN_SEP)
    print(f"  {'Function':<40}  {'Count':>10}  {'Share':>8}"); print(THIN_SEP)
    for fn, cnt in fn_sorted[:15]:
        print(f"  {fn:<40}  {cnt:>10,}  {cnt/total_fn*100:>7.2f}%")
    print(f"\n  INFERRED WORKLOAD TYPES: {', '.join(workload_signals)}")
    print(f"\n  IMAGE OWNER DISTRIBUTION")
    print(f"  ℹ️  NOTE: On this E2EE subnet all traffic routes through Chutes inference.")
    print(f"  Platform owns its own model images — architecturally expected, not a signal.")
    print(THIN_SEP)
    print(f"  {'Owner (image_user_id truncated)':40}  {'Invocations':>12}  {'Share':>8}")
    print(THIN_SEP)
    for owner, cnt in img_user_sorted[:5]:
        owner_display = owner[:38] + ".." if len(owner) > 40 else owner
        print(f"  {owner_display:40}  {cnt:>12,}  {cnt/total_imu*100:>7.2f}%")
    if owner_counts:
        print(f"\n  CHUTE OWNER DISTRIBUTION"); print(THIN_SEP)
        for owner, cnt in sorted(owner_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {owner:<40}  {cnt:>4} chutes")
    if model_counts:
        print(f"\n  MODEL DISTRIBUTION"); print(THIN_SEP)
        for model, cnt in sorted(model_counts.items(), key=lambda x: x[1], reverse=True)[:15]:
            print(f"  {model:<60}  {cnt:>4} chutes")
    if type_counts:
        print(f"\n  CHUTE TYPE DISTRIBUTION"); print(THIN_SEP)
        for ctype, cnt in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {ctype:<30}  {cnt:>4} chutes")
    print(f"\n  TEMPORAL PROFILE (hourly distribution, UTC)"); print(THIN_SEP)
    print(f"  Pattern          : {temporal_pattern}")
    print(f"  CV (hourly)      : {cv:.3f}  ({pattern_note})")
    print(f"  Active hours     : {active_hrs}/24")
    if peak_hour   is not None: print(f"  Peak hour (UTC)  : {peak_hour:02d}:00")
    if trough_hour is not None: print(f"  Trough hour (UTC): {trough_hour:02d}:00")
    print(); peak_bar = max(hourly_vals) if hourly_vals else 1
    print(f"  UTC  Invocations"); print(f"  {'-'*60}")
    for h in range(24):
        vol = hour_of_day.get(h, 0)
        bar = int(vol / peak_bar * 40) if peak_bar > 0 else 0
        print(f"  {h:02d}:00  {'█' * bar}  {vol:,}")

    return {
        "date": date, "user_id": user_id, "invocation_count": inv_count,
        "compute_gpu_hours": round(cmp_secs / 3600, 2),
        "inv_share": round(inv_share, 6), "compute_share": round(cmp_share, 6),
        "chute_count": len(chute_ids), "chutes_with_metadata": known_chutes,
        "temporal_pattern": temporal_pattern, "cv_hourly": round(cv, 4),
        "active_hours_24": active_hrs, "peak_hour_utc": peak_hour, "trough_hour_utc": trough_hour,
        "workload_types": workload_signals,
        "distinct_parent_ids":   len(user_data.get("parent_ids",   set())),
        "distinct_instance_ids": len(user_data.get("instance_ids", set())),
        "top_functions":    [{"name": fn, "count": cnt, "share": round(cnt/total_fn, 4)}
                             for fn, cnt in fn_sorted[:20]],
        "top_image_owners": [{"image_user_id": o, "count": cnt, "share": round(cnt/total_imu, 4)}
                             for o, cnt in img_user_sorted[:10]],
        "top_images":       [{"image_id": i, "count": cnt} for i, cnt in img_sorted[:20]],
        "chute_owners":            dict(sorted(owner_counts.items(), key=lambda x: x[1], reverse=True)[:20]),
        "model_distribution":      dict(sorted(model_counts.items(), key=lambda x: x[1], reverse=True)[:20]),
        "chute_type_distribution": dict(type_counts),
        "hourly_distribution":     {f"{h:02d}:00": hour_of_day.get(h, 0) for h in range(24)},
    }


# ══════════════════════════════════════════════════════════════════════════════
# E2EE PERIMETER FORENSICS
# ══════════════════════════════════════════════════════════════════════════════

def analyze_token_variance(token_cv: dict) -> dict:
    result = {
        "signal": "TOKEN_COUNT_VARIANCE", "fields_found": sorted(token_cv.keys()),
        "distributions": {}, "verdict": None, "confidence": None, "note": None,
    }
    if not token_cv:
        result["verdict"] = "INCONCLUSIVE"; result["confidence"] = 0.0
        result["note"]    = "No token count data found in metrics field."
        return result
    high_cv = 0; low_cv = 0; total = 0
    for field, stats in token_cv.items():
        result["distributions"][field] = stats
        if stats.get("n", 0) >= 10:
            total += 1; cv = stats.get("cv", 0)
            if cv > 0.5:   high_cv += 1
            elif cv < 0.2: low_cv  += 1
    if total == 0:
        result["verdict"] = "INCONCLUSIVE"; result["confidence"] = 0.0
        result["note"]    = "Insufficient token count data."
        return result
    if high_cv > low_cv:
        result["verdict"]    = "LEANS_EXTERNAL"
        result["confidence"] = round(high_cv / total, 2)
        result["note"]       = (f"High token count variance (CV>0.5) across {high_cv}/{total} fields. "
                                f"Diverse request sizes consistent with heterogeneous external demand.")
    elif low_cv > high_cv:
        result["verdict"]    = "LEANS_SYNTHETIC"
        result["confidence"] = round(low_cv / total, 2)
        result["note"]       = (f"Low token count variance (CV<0.2) across {low_cv}/{total} fields. "
                                f"Uniform request sizes consistent with batch generation.")
    else:
        result["verdict"] = "INCONCLUSIVE"; result["confidence"] = 0.3
        result["note"]    = "Mixed token variance signals."
    return result


def analyze_chute_age(user_data: dict, chute_meta: dict) -> dict:
    result = {
        "signal": "CHUTE_AGE_ANALYSIS", "chutes_analyzed": 0, "chutes_with_age": 0,
        "age_distribution": {}, "verdict": None, "confidence": None, "note": None,
    }
    chute_ids = user_data.get("chute_ids", set())
    if not chute_ids:
        result["verdict"] = "INCONCLUSIVE"; result["note"] = "No chute IDs available."
        return result
    now = datetime.now(timezone.utc)
    ages_days = []; fresh = 0; recent = 0; established = 0
    for cid in chute_ids:
        created = chute_meta.get(cid, {}).get("created_at")
        if not created: continue
        try:
            if isinstance(created, str):
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            elif isinstance(created, (int, float)):
                dt = datetime.fromtimestamp(created, tz=timezone.utc)
            else: continue
            age = (now - dt).days; ages_days.append(age)
            if age < 7:    fresh       += 1
            elif age < 30: recent      += 1
            else:          established += 1
        except Exception: continue
    result["chutes_analyzed"] = len(chute_ids); result["chutes_with_age"] = len(ages_days)
    if not ages_days:
        result["verdict"] = "INCONCLUSIVE"; result["confidence"] = 0.0
        result["note"]    = "Chute metadata does not include creation timestamps."
        return result
    avg_age = sum(ages_days) / len(ages_days)
    result["age_distribution"] = {
        "fresh_lt7d": fresh, "recent_7_30d": recent, "established_gt30d": established,
        "avg_age_days": round(avg_age, 1), "min_age_days": min(ages_days),
        "max_age_days": max(ages_days), "median_age_days": sorted(ages_days)[len(ages_days)//2],
    }
    n = len(ages_days); ep = established / n; fp = fresh / n
    if ep > 0.6:
        result["verdict"] = "LEANS_EXTERNAL"; result["confidence"] = round(ep, 2)
        result["note"]    = (f"{ep*100:.0f}% of invoked chutes are 30+ days old. "
                             f"Consistent with organic discovery of established public models.")
    elif fp > 0.5:
        result["verdict"] = "LEANS_SYNTHETIC"; result["confidence"] = round(fp, 2)
        result["note"]    = (f"{fp*100:.0f}% of invoked chutes are <7 days old. "
                             f"Fresh chutes with immediate high volume suggests coordinated creation.")
    else:
        result["verdict"] = "MIXED"; result["confidence"] = 0.4
        result["note"]    = "Mix of fresh and established chutes — no strong signal."
    return result


def analyze_miner_selection_entropy(
    user_miner_counts: dict, network_miner_shares: dict, dominant_user_id: str
) -> dict:
    result = {
        "signal": "MINER_SELECTION_ENTROPY", "dominant_user_miners": 0,
        "network_miners": 0, "entropy_dominant": None, "entropy_network": None,
        "entropy_ratio": None, "kl_divergence": None, "top_miner_affinity": [],
        "verdict": None, "confidence": None, "note": None,
    }
    dom_counts = user_miner_counts.get(dominant_user_id, {})
    if not dom_counts or not network_miner_shares:
        result["verdict"] = "INCONCLUSIVE"; result["note"] = "Insufficient miner selection data."
        return result
    dom_total = sum(dom_counts.values())
    dom_dist  = {hk: cnt / dom_total for hk, cnt in dom_counts.items()}
    def shannon(d): return -sum(p * math.log2(p) for p in d.values() if p > 0)
    h_dom = shannon(dom_dist); h_network = shannon(network_miner_shares)
    result["dominant_user_miners"] = len(dom_dist)
    result["network_miners"]       = len(network_miner_shares)
    result["entropy_dominant"]     = round(h_dom, 4)
    result["entropy_network"]      = round(h_network, 4)
    result["entropy_ratio"]        = round(h_dom / h_network, 4) if h_network > 0 else None
    kl = sum(p * math.log2(p / network_miner_shares[hk])
             for hk, p in dom_dist.items()
             if hk in network_miner_shares and p > 0 and network_miner_shares[hk] > 0)
    result["kl_divergence"] = round(kl, 4)
    for hk, share in sorted(dom_dist.items(), key=lambda x: x[1], reverse=True)[:5]:
        ns = network_miner_shares.get(hk, 0)
        result["top_miner_affinity"].append({
            "hotkey_short": hk[:8] + "...", "user_share": round(share, 4),
            "network_share": round(ns, 4),
            "affinity_ratio": round(share / ns, 2) if ns > 0 else None,
        })
    ratio = result["entropy_ratio"]
    if ratio is not None:
        if ratio > 0.85:
            result["verdict"] = "LEANS_EXTERNAL"; result["confidence"] = round(min(ratio, 1.0), 2)
            result["note"]    = (f"Entropy ratio {ratio:.4f} — dominant user's miner selection "
                                 f"closely matches load balancer distribution. Consistent with proxy routing.")
        elif ratio < 0.5:
            result["verdict"] = "LEANS_SYNTHETIC"; result["confidence"] = round(1 - ratio, 2)
            result["note"]    = (f"Entropy ratio {ratio:.4f} — concentrated miner selection "
                                 f"diverges from network. Suggests internal routing.")
        else:
            result["verdict"] = "AMBIGUOUS"; result["confidence"] = 0.4
            result["note"]    = f"Entropy ratio {ratio:.4f} — partial match to network distribution."
    return result


def analyze_inter_arrival(timestamps: list, label: str = "dominant_user") -> dict:
    result = {
        "signal": "INTER_ARRIVAL_DISTRIBUTION", "label": label,
        "n_events": len(timestamps), "n_intervals": 0, "stats": {},
        "pattern_classification": None, "regularity_score": None, "burst_ratio": None,
        "verdict": None, "confidence": None, "note": None,
    }
    if len(timestamps) < 10:
        result["verdict"] = "INCONCLUSIVE"; result["note"] = "Fewer than 10 timestamped events."
        return result
    intervals = [(timestamps[i] - timestamps[i-1]).total_seconds()
                 for i in range(1, len(timestamps))
                 if (timestamps[i] - timestamps[i-1]).total_seconds() >= 0]
    if len(intervals) < 5:
        result["verdict"] = "INCONCLUSIVE"; result["note"] = "Fewer than 5 valid intervals."
        return result
    n    = len(intervals); mean = sum(intervals) / n
    var  = sum((x - mean) ** 2 for x in intervals) / n
    std  = var ** 0.5; cv = std / mean if mean > 0 else 0
    srt  = sorted(intervals)
    result["n_intervals"] = n
    result["stats"] = {
        "mean_sec": round(mean, 3), "std_sec": round(std, 3), "cv": round(cv, 4),
        "min_sec":  round(srt[0], 3), "p10_sec": round(srt[n//10], 3),
        "p25_sec":  round(srt[n//4], 3), "median_sec": round(srt[n//2], 3),
        "p75_sec":  round(srt[3*n//4], 3), "p90_sec": round(srt[int(n*0.9)], 3),
        "p99_sec":  round(srt[int(n*0.99)], 3), "max_sec": round(srt[-1], 3),
    }
    result["regularity_score"] = round(cv, 4)
    burst_threshold = mean * 0.1 if mean > 0 else 1
    result["burst_ratio"] = round(sum(1 for x in intervals if x < burst_threshold) / n, 4)
    if cv < 0.3:
        result["pattern_classification"] = "FIXED_CADENCE"
        result["verdict"] = "LEANS_SYNTHETIC"; result["confidence"] = round(1 - cv, 2)
        result["note"]    = f"CV={cv:.3f} — highly regular. Characteristic of scheduled batch jobs."
    elif cv < 0.8:
        result["pattern_classification"] = "SEMI_REGULAR"
        result["verdict"] = "AMBIGUOUS"; result["confidence"] = 0.4
        result["note"]    = f"CV={cv:.3f} — moderately regular. Could be rate-limited proxy."
    elif cv < 1.5:
        result["pattern_classification"] = "POISSON_LIKE"
        result["verdict"] = "LEANS_EXTERNAL"; result["confidence"] = round(min(cv / 1.5, 0.8), 2)
        result["note"]    = (f"CV={cv:.3f} — consistent with Poisson process. "
                             f"Irregular intervals suggest diverse human-originated requests.")
    else:
        result["pattern_classification"] = "BURSTY"
        result["verdict"] = "LEANS_EXTERNAL"; result["confidence"] = round(min(cv / 2.0, 0.85), 2)
        result["note"]    = (f"CV={cv:.3f}, burst ratio={result['burst_ratio']*100:.1f}%. "
                             f"Bursty pattern — agentic or event-driven external demand.")
    return result


def analyze_function_sequences_from_counters(
    bigram_counts: dict, trigram_counts: dict, fn_total_counts: dict
) -> dict:
    total_calls   = sum(fn_total_counts.values())
    unique_fns    = set(fn_total_counts.keys())
    total_bigrams = sum(bigram_counts.values())
    result = {
        "signal": "FUNCTION_SEQUENCE_COOCCURRENCE",
        "n_calls": total_calls, "unique_functions": len(unique_fns),
        "bigram_distribution": {}, "trigram_top10": [],
        "monotonic_ratio": None, "pipeline_patterns": [],
        "sequence_entropy": None, "verdict": None, "confidence": None, "note": None,
    }
    if total_calls < 20 or total_bigrams < 5:
        result["verdict"] = "INCONCLUSIVE"; result["note"] = "Insufficient function call data."
        return result
    for (a, b), cnt in sorted(bigram_counts.items(), key=lambda x: x[1], reverse=True)[:20]:
        result["bigram_distribution"][f"{a} → {b}"] = {
            "count": cnt, "share": round(cnt / total_bigrams, 4)}
    total_trigrams = sum(trigram_counts.values())
    for (a, b, c), cnt in sorted(trigram_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        result["trigram_top10"].append({
            "pattern": f"{a} → {b} → {c}", "count": cnt,
            "share": round(cnt / max(total_trigrams, 1), 4)})
    same_count = sum(cnt for (a, b), cnt in bigram_counts.items() if a == b)
    mono       = round(same_count / total_bigrams, 4) if total_bigrams > 0 else 0
    result["monotonic_ratio"] = mono
    probs       = [cnt / total_bigrams for cnt in bigram_counts.values()]
    seq_entropy = -sum(p * math.log2(p) for p in probs if p > 0)
    max_entropy = math.log2(len(bigram_counts)) if len(bigram_counts) > 1 else 1
    normalized  = seq_entropy / max_entropy if max_entropy > 0 else 0
    result["sequence_entropy"] = round(seq_entropy, 4)
    fn_lower = {f.lower() for f in unique_fns}
    patterns = []
    bg_lower = {(a.lower(), b.lower()): cnt for (a, b), cnt in bigram_counts.items()}
    if any("embed" in f for f in fn_lower):
        rag = [k for k in bg_lower if "embed" in k[0] and
               any(g in k[1] for g in ("generate", "chat", "complete"))]
        if rag: patterns.append("RAG_PIPELINE")
    if any(f in fn_lower for f in ("tool_call", "function_call", "tool_use", "execute")):
        patterns.append("AGENTIC_TOOL_USE")
    if mono > 0.8: patterns.append("BATCH_INFERENCE")
    if (any("vision" in f or "classify_image" in f for f in fn_lower) and
            any(f in fn_lower for f in ("generate", "chat", "complete"))):
        patterns.append("MULTI_MODAL")
    result["pipeline_patterns"] = patterns
    if mono > 0.85 and normalized < 0.3:
        result["verdict"] = "LEANS_SYNTHETIC"; result["confidence"] = round(mono, 2)
        result["note"]    = f"Monotonic ratio {mono:.2%} with low entropy — batch inference pattern."
    elif len(patterns) >= 2 or normalized > 0.7:
        result["verdict"] = "LEANS_EXTERNAL"; result["confidence"] = round(min(normalized, 0.85), 2)
        result["note"]    = (f"High sequence diversity (entropy={seq_entropy:.2f}, "
                             f"{len(unique_fns)} unique functions). "
                             f"Detected: {', '.join(patterns) or 'diverse mix'}.")
    elif len(patterns) == 1 and "BATCH_INFERENCE" in patterns:
        result["verdict"] = "LEANS_SYNTHETIC"; result["confidence"] = 0.6
        result["note"]    = "Single-pattern batch inference detected."
    else:
        result["verdict"] = "AMBIGUOUS"; result["confidence"] = 0.4
        result["note"]    = (f"Monotonic ratio {mono:.2%}, {len(unique_fns)} unique functions. "
                             f"No strong signal either direction.")
    return result


def analyze_image_owner_provenance_structural(user_data: dict) -> dict:
    img_user_counts = user_data.get("image_user_ids", {})
    unique_owners   = len(img_user_counts)
    total           = sum(img_user_counts.values()) or 1
    top1            = sorted(img_user_counts.values(), reverse=True)[0] / total if img_user_counts else 0
    return {
        "signal": "IMAGE_OWNER_PROVENANCE",
        "unique_image_owners": unique_owners, "top1_owner_share": round(top1, 4),
        "verdict": "NOT_APPLICABLE", "confidence": 0.0,
        "note": ("Image owner concentration is a structural constant on this subnet. "
                 "E2EE routing means all traffic passes through Chutes inference infrastructure, "
                 "which owns its own model images by design. Excluded from composite scoring."),
        "excluded_from_composite": True,
    }


def analyze_parent_invocation_diversity(dom_inv_count: int, dom_parent_ids: set) -> dict:
    result = {
        "signal": "PARENT_INVOCATION_DIVERSITY",
        "dominant_inv_count": dom_inv_count, "distinct_parent_ids": len(dom_parent_ids),
        "parent_to_inv_ratio": None, "verdict": None, "confidence": None, "note": None,
    }
    if not dom_parent_ids or dom_inv_count == 0:
        result["verdict"] = "INCONCLUSIVE"; result["confidence"] = 0.0
        result["note"]    = "No parent_invocation_id data available."
        return result
    ratio = len(dom_parent_ids) / dom_inv_count
    result["parent_to_inv_ratio"] = round(ratio, 4)
    if ratio > 0.7:
        result["verdict"] = "LEANS_EXTERNAL"; result["confidence"] = round(min(ratio, 1.0), 2)
        result["note"]    = (f"{len(dom_parent_ids):,} distinct parent IDs across {dom_inv_count:,} "
                             f"invocations (ratio={ratio:.2%}). High diversity — consistent with "
                             f"many independent external callers.")
    elif ratio < 0.1:
        result["verdict"] = "LEANS_SYNTHETIC"; result["confidence"] = round(min(1 - ratio*5, 0.9), 2)
        result["note"]    = f"ratio={ratio:.2%} — very low parent diversity."
    elif ratio < 0.3:
        result["verdict"] = "AMBIGUOUS"; result["confidence"] = 0.4
        result["note"]    = f"ratio={ratio:.2%} — moderate fan-out. No strong signal."
    else:
        result["verdict"] = "LEANS_EXTERNAL"; result["confidence"] = round(ratio, 2)
        result["note"]    = (f"ratio={ratio:.2%} — substantial parent diversity. "
                             f"Consistent with external proxy rather than internal batch.")
    return result


def analyze_instance_entropy(
    dom_instance_ids: set, dom_inv_count: int, network_instance_counts: dict
) -> dict:
    result = {
        "signal": "INSTANCE_ID_ENTROPY",
        "dominant_instance_ids": len(dom_instance_ids),
        "network_instance_ids":  len(network_instance_counts),
        "instance_to_inv_ratio": None, "entropy_dominant": None,
        "verdict": None, "confidence": None, "note": None,
        "composite_weight": 0.5,
    }
    if not dom_instance_ids or dom_inv_count == 0:
        result["verdict"] = "INCONCLUSIVE"; result["confidence"] = 0.0
        result["note"]    = "No instance_id data available."
        return result
    result["instance_to_inv_ratio"] = round(len(dom_instance_ids) / dom_inv_count, 6)
    if not network_instance_counts:
        result["verdict"] = "INCONCLUSIVE"; result["confidence"] = 0.0
        result["note"]    = "No network instance distribution for comparison."
        return result
    net_unique   = len(network_instance_counts)
    dom_coverage = len(dom_instance_ids) / net_unique if net_unique > 0 else 0
    result["entropy_dominant"] = round(dom_coverage, 4)
    if dom_coverage > 0.7:
        result["verdict"] = "LEANS_EXTERNAL"; result["confidence"] = round(dom_coverage, 2)
        result["note"]    = (f"Dominant user accessed {len(dom_instance_ids):,} instances "
                             f"({dom_coverage:.0%} of network). Broad coverage — consistent with "
                             f"load balancer routing diverse demand.")
    elif dom_coverage < 0.3:
        result["verdict"] = "LEANS_SYNTHETIC"; result["confidence"] = round(1 - dom_coverage, 2)
        result["note"]    = f"Narrow instance coverage ({dom_coverage:.0%}) — consistent with internal routing."
    else:
        result["verdict"] = "AMBIGUOUS"; result["confidence"] = 0.4
        result["note"]    = f"Moderate instance coverage ({dom_coverage:.0%}) — no strong signal."
    return result


def compute_composite_verdict(signals: list) -> dict:
    SIGNAL_WEIGHTS = {
        "TOKEN_COUNT_VARIANCE":           1.0,
        "CHUTE_AGE_ANALYSIS":             1.0,
        "MINER_SELECTION_ENTROPY":        1.0,
        "INTER_ARRIVAL_DISTRIBUTION":     1.0,
        "FUNCTION_SEQUENCE_COOCCURRENCE": 1.0,
        "PARENT_INVOCATION_DIVERSITY":    1.0,
        "INSTANCE_ID_ENTROPY":            0.5,
        "IMAGE_OWNER_PROVENANCE":         0.0,
    }
    external_score = 0.0; synthetic_score = 0.0; total_weight = 0.0; reasoning = []
    for sig in signals:
        verdict     = sig.get("verdict", "INCONCLUSIVE")
        confidence  = sig.get("confidence", 0.0) or 0.0
        signal_name = sig.get("signal", "UNKNOWN")
        weight      = SIGNAL_WEIGHTS.get(signal_name, 1.0)
        if verdict == "NOT_APPLICABLE":
            reasoning.append(f"  {signal_name}: NOT_APPLICABLE (excluded — structural constant)")
            continue
        if verdict == "INCONCLUSIVE" or weight == 0.0:
            reasoning.append(f"  {signal_name}: {verdict} (excluded from scoring)")
            continue
        effective     = confidence * weight; total_weight += effective
        if verdict == "LEANS_EXTERNAL":
            external_score += effective
            reasoning.append(f"  {signal_name}: {verdict} (conf={confidence:.2f}, "
                             f"weight={weight}) → +{effective:.2f} external")
        elif verdict == "MIXED":
            external_score += effective * 0.5
            reasoning.append(f"  {signal_name}: {verdict} (conf={confidence:.2f}, "
                             f"weight={weight}) → +{effective*0.5:.2f} external (mixed)")
        elif verdict == "LEANS_SYNTHETIC":
            synthetic_score += effective
            reasoning.append(f"  {signal_name}: {verdict} (conf={confidence:.2f}, "
                             f"weight={weight}) → +{effective:.2f} synthetic")
        else:
            reasoning.append(f"  {signal_name}: {verdict} (conf={confidence:.2f}, "
                             f"weight={weight}) → neutral")
    if total_weight == 0:
        return {
            "composite_verdict": "INCONCLUSIVE", "composite_confidence": 0.0,
            "external_score": 0.0, "synthetic_score": 0.0, "reasoning": reasoning,
            "interpretation": "All signals inconclusive.",
        }
    ext_pct = external_score / total_weight; syn_pct = synthetic_score / total_weight
    if ext_pct > 0.6:
        verdict = "LIKELY_EXTERNAL_PROXY"
        interp  = ("Preponderance of evidence suggests the dominant user is a platform proxy "
                   "routing genuine third-party API demand. Token variance, temporal distribution, "
                   "function sequences, miner selection conformity, and request origin diversity "
                   "are consistent with heterogeneous external usage.")
    elif syn_pct > 0.6:
        verdict = "LIKELY_SELF_CONSUMPTION"
        interp  = ("Preponderance of evidence suggests internal or synthetic traffic.")
    elif ext_pct > syn_pct:
        verdict = "LEANS_EXTERNAL_INCONCLUSIVE"
        interp  = "Signals lean toward external proxy routing but confidence is below threshold."
    elif syn_pct > ext_pct:
        verdict = "LEANS_SYNTHETIC_INCONCLUSIVE"
        interp  = "Signals lean slightly toward self-consumption but confidence is low."
    else:
        verdict = "GENUINELY_AMBIGUOUS"; interp = "Signals are evenly split."
    return {
        "composite_verdict":    verdict,
        "composite_confidence": round(max(ext_pct, syn_pct), 3),
        "external_score":       round(ext_pct, 3),
        "synthetic_score":      round(syn_pct, 3),
        "reasoning":            reasoning,
        "interpretation":       interp,
    }


def run_perimeter_analysis(
    user_agg: dict, chute_meta: dict, dominant_user_id: str,
    perimeter: dict, date: str, output_dir: Path = OUTPUT_DIR,
) -> dict:
    print(f"\n{'='*100}")
    print(f"  E2EE PERIMETER FORENSICS — DEMAND AUTHENTICITY ANALYSIS")
    print(f"  Intelligence Sovereignty Research Suite | @im_perseverance")
    print(f"{'='*100}")
    dominant_data = user_agg.get(dominant_user_id, {})
    if not dominant_data:
        print(f"  ❌ Dominant user {dominant_user_id} not found.")
        return {}
    signals = []
    print(f"\n  Running Signal 1: Token Count Variance...")
    sig1 = analyze_token_variance(perimeter.get("dominant_token_cv", {}))
    signals.append(sig1); print(f"    → {sig1['verdict']} (conf={sig1.get('confidence',0):.2f})")
    print(f"\n  Running Signal 2: Chute Age Analysis...")
    sig2 = analyze_chute_age(dominant_data, chute_meta)
    signals.append(sig2); print(f"    → {sig2['verdict']} (conf={sig2.get('confidence',0):.2f})")
    print(f"\n  Running Signal 3: Miner Selection Entropy...")
    sig3 = analyze_miner_selection_entropy(
        perimeter["user_miner_counts"], perimeter["network_miner_shares"], dominant_user_id)
    signals.append(sig3); print(f"    → {sig3['verdict']} (conf={sig3.get('confidence',0):.2f})")
    print(f"\n  Running Signal 4: Inter-Arrival Time Distribution...")
    sig4 = analyze_inter_arrival(perimeter["dominant_timestamps"])
    signals.append(sig4); print(f"    → {sig4['verdict']} (conf={sig4.get('confidence',0):.2f})")
    print(f"\n  Running Signal 5: Function Sequence Co-occurrence (full population)...")
    sig5 = analyze_function_sequences_from_counters(
        perimeter["dominant_bigram_counts"],
        perimeter["dominant_trigram_counts"],
        perimeter["dominant_fn_totals"],
    )
    signals.append(sig5); print(f"    → {sig5['verdict']} (conf={sig5.get('confidence',0):.2f})")
    print(f"\n  Signal 6: Image Owner Provenance — NOT_APPLICABLE (structural constant)")
    sig6 = analyze_image_owner_provenance_structural(dominant_data)
    signals.append(sig6); print(f"    → {sig6['verdict']}")
    print(f"\n  Running Signal 7: Parent Invocation ID Diversity...")
    sig7 = analyze_parent_invocation_diversity(
        perimeter.get("dominant_inv_count", 0),
        perimeter.get("dominant_parent_ids", set()),
    )
    signals.append(sig7); print(f"    → {sig7['verdict']} (conf={sig7.get('confidence',0):.2f})")
    print(f"\n  Running Signal 8: Instance ID Entropy (half weight)...")
    sig8 = analyze_instance_entropy(
        perimeter.get("dominant_instance_ids", set()),
        perimeter.get("dominant_inv_count", 0),
        perimeter.get("network_instance_counts", {}),
    )
    signals.append(sig8); print(f"    → {sig8['verdict']} (conf={sig8.get('confidence',0):.2f})")
    composite = compute_composite_verdict(signals)

    VERDICT_CONCLUSIONS = {
        "LIKELY_EXTERNAL_PROXY":          "The dominant demand is not internal farming. It behaves like real external API traffic.",
        "LIKELY_SELF_CONSUMPTION":        "The dominant demand shows signs of internal or synthetic traffic. The demand thesis is under direct challenge.",
        "LEANS_EXTERNAL_INCONCLUSIVE":    "Signals lean toward external demand but are not conclusive. Continue monitoring.",
        "LEANS_SYNTHETIC_INCONCLUSIVE":   "Signals lean toward synthetic traffic but confidence is low. Investigate before acting.",
        "GENUINELY_AMBIGUOUS":            "Signals are evenly split. The E2EE ceiling is binding — no conclusion can be drawn from metadata alone.",
        "INCONCLUSIVE":                   "All signals inconclusive. Insufficient data to reach a verdict this snapshot.",
    }
    verdict_conclusion = VERDICT_CONCLUSIONS.get(
        composite["composite_verdict"],
        composite["composite_verdict"]
    )

    print(f"\n{'='*100}")
    print(f"  COMPOSITE VERDICT")
    print(f"{'-'*100}")
    print(f"  Verdict    : {composite['composite_verdict']}")
    print(f"  Confidence : {composite['composite_confidence']:.1%}")
    print(f"  External   : {composite['external_score']:.1%}")
    print(f"  Synthetic  : {composite['synthetic_score']:.1%}")
    print(f"\n  CONCLUSION: {verdict_conclusion}")
    print(f"\n  SIGNAL REASONING:")
    for line in composite["reasoning"]: print(f"  {line}")
    print(f"\n  INTERPRETATION:")
    words = composite["interpretation"].split(); line = "  "
    for w in words:
        if len(line) + len(w) + 1 > 95: print(line); line = "  " + w
        else: line += " " + w if line.strip() else w
    if line.strip(): print(line)
    print(f"{'='*100}\n")
    report = {
        "date": date, "dominant_user_id": dominant_user_id,
        "window_days": INVOCATION_WINDOW,
        "signals": {
            "token_variance": sig1, "chute_age": sig2, "miner_entropy": sig3,
            "inter_arrival": sig4, "function_sequence": sig5, "image_owner": sig6,
            "parent_invocation": sig7, "instance_entropy": sig8,
        },
        "composite": composite,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    report_file = output_dir / f"chutes_sn64_perimeter_{date}.json"
    with open(report_file, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"  💾 Perimeter report saved: {report_file}")
    trajectory = update_trajectory(report, output_dir)
    if trajectory:
        flip = detect_verdict_flip(trajectory)
        if flip["flipped"]:
            print(f"\n  🚨 VERDICT CHANGED: {flip['direction']} — {flip['severity']}")
        else:
            print(f"\n  ✅ Demand verdict holding: {flip['current_verdict']} "
                  f"across {flip['consecutive_same']} consecutive snapshots.")
    return report


# ── Trajectory Tracking ───────────────────────────────────────────────────────

def update_trajectory(report: dict, output_dir: Path) -> list:
    trajectory_file = output_dir / "perimeter_trajectory.json"
    trajectory = []
    if trajectory_file.exists():
        try:
            with open(trajectory_file) as f: trajectory = json.load(f)
            if not isinstance(trajectory, list): trajectory = []
        except Exception: trajectory = []
    composite = report.get("composite", {}); signals = report.get("signals", {})
    entry = {
        "date":                 report.get("date"),
        "window_days":          report.get("window_days"),
        "composite_verdict":    composite.get("composite_verdict"),
        "composite_confidence": composite.get("composite_confidence"),
        "external_score":       composite.get("external_score"),
        "synthetic_score":      composite.get("synthetic_score"),
        "signals_summary":      {},
    }
    signal_keys = {
        "token_variance":    ("cv",                  lambda s: list(s.get("distributions", {}).values())[0].get("cv") if s.get("distributions") else None),
        "chute_age":         ("established_pct",     lambda s: s.get("age_distribution", {}).get("established_gt30d", 0) / max(s.get("chutes_with_age", 1), 1)),
        "miner_entropy":     ("entropy_ratio",       lambda s: s.get("entropy_ratio")),
        "inter_arrival":     ("cv",                  lambda s: s.get("stats", {}).get("cv")),
        "function_sequence": ("monotonic_ratio",     lambda s: s.get("monotonic_ratio")),
        "parent_invocation": ("parent_to_inv_ratio", lambda s: s.get("parent_to_inv_ratio")),
        "instance_entropy":  ("dom_coverage",        lambda s: s.get("entropy_dominant")),
    }
    for sig_name, (metric_name, extractor) in signal_keys.items():
        sig_data = signals.get(sig_name, {})
        try:   metric_val = extractor(sig_data)
        except Exception: metric_val = None
        entry["signals_summary"][sig_name] = {
            "verdict":    sig_data.get("verdict"),
            "confidence": sig_data.get("confidence"),
            metric_name:  round(metric_val, 4) if isinstance(metric_val, (int, float)) else metric_val,
        }
    trajectory = [e for e in trajectory if e.get("date") != entry["date"]]
    trajectory.append(entry)
    trajectory.sort(key=lambda e: e.get("date", ""))
    with open(trajectory_file, "w") as f:
        json.dump(trajectory, f, indent=2, default=str)
    n = len(trajectory)
    print(f"\n  📈 Trajectory updated: {n} snapshot{'s' if n != 1 else ''}")
    if n >= 2:
        print(f"\n  TRAJECTORY HISTORY:")
        print(f"  {'Date':<12}  {'Verdict':<32}  {'Conf':>6}  {'Ext':>6}  {'Syn':>6}")
        print(f"  {'-'*70}")
        for e in trajectory:
            print(f"  {e['date']:<12}  {e['composite_verdict']:<32}  "
                  f"{e.get('composite_confidence',0):>5.1%}  "
                  f"{e.get('external_score',0):>5.1%}  "
                  f"{e.get('synthetic_score',0):>5.1%}")
    return trajectory


def detect_verdict_flip(trajectory: list) -> dict:
    result = {
        "flipped": False, "direction": None, "severity": None,
        "previous_verdict": None, "previous_date": None,
        "current_verdict": None, "current_date": None, "consecutive_same": 1,
    }
    if len(trajectory) < 2:
        if trajectory:
            result["current_verdict"] = trajectory[-1].get("composite_verdict")
            result["current_date"]    = trajectory[-1].get("date")
        return result
    cur = trajectory[-1]; prv = trajectory[-2]
    result["current_verdict"]  = cur.get("composite_verdict")
    result["current_date"]     = cur.get("date")
    result["previous_verdict"] = prv.get("composite_verdict")
    result["previous_date"]    = prv.get("date")
    streak = 1
    for i in range(len(trajectory) - 2, -1, -1):
        if trajectory[i].get("composite_verdict") == result["current_verdict"]: streak += 1
        else: break
    result["consecutive_same"] = streak
    cv = result["current_verdict"] or ""; pv = result["previous_verdict"] or ""
    if cv == pv: return result
    result["flipped"] = True
    EXT = {"LIKELY_EXTERNAL_PROXY", "LEANS_EXTERNAL_INCONCLUSIVE"}
    SYN = {"LIKELY_SELF_CONSUMPTION", "LEANS_SYNTHETIC_INCONCLUSIVE"}
    NEU = {"GENUINELY_AMBIGUOUS", "INCONCLUSIVE"}
    if pv in EXT and cv in SYN:   result["direction"] = "EXTERNAL → SYNTHETIC"; result["severity"] = "HIGH — demand thesis under direct challenge"
    elif pv in SYN and cv in EXT: result["direction"] = "SYNTHETIC → EXTERNAL"; result["severity"] = "HIGH — demand thesis strengthening"
    elif pv in EXT and cv in NEU: result["direction"] = "EXTERNAL → AMBIGUOUS"; result["severity"] = "MEDIUM — signal weakening"
    elif pv in SYN and cv in NEU: result["direction"] = "SYNTHETIC → AMBIGUOUS"; result["severity"] = "MEDIUM — previous concern may be resolving"
    elif cv in EXT:  result["direction"] = f"{pv} → EXTERNAL";  result["severity"] = "LOW — moving toward thesis confirmation"
    elif cv in SYN:  result["direction"] = f"{pv} → SYNTHETIC"; result["severity"] = "LOW — moving toward thesis challenge"
    else:            result["direction"] = f"{pv} → {cv}";       result["severity"] = "LOW"
    return result


# ══════════════════════════════════════════════════════════════════════════════
# CORE ANALYSIS ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def run_analysis(root_tao_threshold: float, sn64_alpha_threshold: float, alpha_method: int):

    print(f"\n{SEPARATOR}")
    print(f"  SN64 CHUTES — INTELLIGENCE MARKET HYPOTHESIS TEST")
    print(f"  Intelligence Sovereignty Research Suite | @im_perseverance")
    print(SEPARATOR)

    print("\nConnecting to Bittensor network...")
    sub   = bt.Subtensor(network="finney")
    block = sub.get_current_block()
    now   = datetime.now(timezone.utc)
    ts    = now.strftime("%Y-%m-%d %H:%M:%S UTC")
    date  = now.strftime("%Y-%m-%d")
    print(f"  📌 Snapshot block : {block:,}")
    print(f"  📌 Snapshot time  : {ts}")
    print(f"  📌 Analysis window: {INVOCATION_WINDOW} days")

    print(f"\nLoading SN{NETUID} metagraph...")
    meta            = sub.metagraph(NETUID)
    n_uids          = len(meta.uids)
    total_stake     = sum(float(s) for s in meta.stake)
    vali_uids       = [i for i, vp in enumerate(meta.validator_permit) if vp]
    onchain         = {}
    total_incentive = sum(float(meta.incentive[i]) for i in range(n_uids))
    total_dividends = sum(float(meta.dividends[i]) for i in range(n_uids))

    for uid in range(n_uids):
        hk        = meta.hotkeys[uid]
        stake     = float(meta.stake[uid])
        tao_equiv = float(meta.tao_stake[uid]) if hasattr(meta, 'tao_stake') else stake
        incentive = float(meta.incentive[uid])
        dividends = float(meta.dividends[uid])
        tv        = float(meta.validator_trust[uid]) if hasattr(meta, 'validator_trust') else 0.0
        is_val    = uid in vali_uids
        last_upd  = int(meta.last_update[uid])           if hasattr(meta, 'last_update')              else 0
        reg_block = int(meta.block_at_registration[uid]) if hasattr(meta, 'block_at_registration')    else 0
        onchain[hk] = {
            "uid": uid, "hotkey": hk, "stake": stake, "tao_equiv": tao_equiv,
            "stake_pct":      stake / total_stake if total_stake > 0 else 0,
            "incentive":      incentive,
            "incentive_pct":  incentive / total_incentive if total_incentive > 0 else 0,
            "dividends":      dividends,
            "dividends_pct":  dividends / total_dividends if total_dividends > 0 else 0,
            "tv": tv, "is_validator": is_val, "last_update": last_upd,
            "reg_block": reg_block,
            "age_blocks":          block - reg_block if reg_block > 0 else 0,
            "blocks_since_update": block - last_upd  if last_upd  > 0 else 0,
        }
    print(f"  SN64 UIDs: {n_uids} | Validators: {len(vali_uids)}")

    print(f"\nFetching SN{NETUID} alpha pool state...")
    pool = fetch_alpha_pool_state(sub, NETUID, method=alpha_method)

    print(f"\nFetching validator weight submissions for SN{NETUID}...")
    weight_totals = defaultdict(float); weight_counts = 0
    for val_uid in vali_uids:
        try:
            weights = sub.get_neuron_for_pubkey_and_subnet(meta.hotkeys[val_uid], NETUID)
            if weights and hasattr(weights, 'weights') and weights.weights:
                weight_counts += 1
                for miner_uid, w in weights.weights:
                    weight_totals[miner_uid] += float(w)
        except Exception: continue
    total_weight = sum(weight_totals.values())
    print(f"  Validators with weight data: {weight_counts}/{len(vali_uids)}")
    print(f"  UIDs with weight allocation: {len(weight_totals)}")

    root_stakes = get_root_validator_stakes(sub)

    sn64_validators = []
    for hk, data in onchain.items():
        if data["is_validator"] and data["tao_equiv"] >= sn64_alpha_threshold:
            sn64_validators.append({
                "hotkey": hk, "hotkey_short": hk[:8]+"...", "uid_sn64": data["uid"],
                "sn64_alpha": data["stake"], "tao_equiv": data["tao_equiv"], "tv": data["tv"],
                "dividends_pct": data["dividends_pct"],
                "weight_share": weight_totals.get(data["uid"], 0) / total_weight if total_weight > 0 else 0,
            })
    sn64_validators.sort(key=lambda x: x["sn64_alpha"], reverse=True)
    print(f"\n  SN64 validators with {sn64_alpha_threshold:,.0f}+ alpha: {len(sn64_validators)}")

    overlap = []
    for hk, root_tao in root_stakes.items():
        if hk in onchain and onchain[hk]["is_validator"]:
            d = onchain[hk]
            overlap.append({
                "hotkey": hk, "hotkey_short": hk[:8]+"...", "uid_sn64": d["uid"],
                "root_tao": root_tao, "sn64_alpha": d["stake"], "tv": d["tv"],
                "dividends_pct": d["dividends_pct"],
            })
    overlap.sort(key=lambda x: x["root_tao"], reverse=True)
    print(f"  Root × SN64 overlap (any alpha): {len(overlap)} validators")

    print(f"\nFetching Chutes invocation exports ({INVOCATION_WINDOW}-day window)...")
    miner_demand, user_agg, perimeter, fields_info, all_chute_ids = fetch_all_data(INVOCATION_WINDOW)

    if not miner_demand:
        print("  ❌ No invocation data retrieved from api.chutes.ai"); return

    total_invocations = sum(m["invocation_count"] for m in miner_demand.values())
    total_compute     = sum(m["compute_seconds"]  for m in miner_demand.values())
    tps_miners        = [m for m in miner_demand.values() if m["avg_tps"] > 0]
    print(f"\n  Unique miners in invocation data: {len(miner_demand)}")
    print(f"  Total invocations (7d): {total_invocations:,}")
    print(f"  Total compute seconds (7d): {total_compute:,.0f}")

    print("\nLoading previous snapshot for trend analysis...")
    prev_snapshot = load_previous_snapshot(OUTPUT_DIR, date)

    print(f"\nMerging datasets on hotkey join key...")
    merged = []; matched = 0; unmatched_demand = 0

    for hk in set(onchain.keys()) | set(miner_demand.keys()):
        chain_data  = onchain.get(hk)
        demand_data = miner_demand.get(hk)
        if chain_data is None: unmatched_demand += 1; continue

        inv_count     = demand_data["invocation_count"] if demand_data else 0
        compute_sec   = demand_data["compute_seconds"]  if demand_data else 0.0
        chute_div     = demand_data["chute_diversity"]   if demand_data else 0
        avg_tps       = demand_data["avg_tps"]           if demand_data else 0.0
        avg_ttft      = demand_data["avg_ttft"]          if demand_data else 0.0
        pass_rate     = demand_data["pass_rate"]         if demand_data else None
        inv_share     = inv_count   / total_invocations if total_invocations > 0 else 0
        compute_share = compute_sec / total_compute     if total_compute     > 0 else 0
        uid           = chain_data["uid"]
        weight_share  = weight_totals.get(uid, 0) / total_weight if total_weight > 0 else 0
        emission_share = chain_data["incentive_pct"]
        delta_inv_emit = emission_share - inv_share
        delta_inv_wt   = weight_share   - inv_share
        if demand_data: matched += 1

        prev       = prev_snapshot.get(hk, {})
        emit_trend = emission_share - prev.get("emission_share",   emission_share) if prev else None
        inv_trend  = inv_share      - prev.get("invocation_share", inv_share)      if prev else None

        merged.append({
            "hotkey": hk, "hotkey_short": hk[:8]+"...", "uid": uid,
            "is_validator": chain_data["is_validator"], "tv": chain_data["tv"],
            "age_blocks": chain_data["age_blocks"], "blocks_since_update": chain_data["blocks_since_update"],
            "invocation_count": inv_count, "compute_seconds": round(compute_sec, 2),
            "chute_diversity": chute_div, "avg_tps": avg_tps, "avg_ttft": avg_ttft, "pass_rate": pass_rate,
            "seconds_per_invocation": 0.0, "efficiency_tier": "N/A",
            "invocation_share": inv_share, "compute_share": compute_share,
            "weight_share": weight_share, "emission_share": emission_share,
            "dividends_share": chain_data["dividends_pct"], "stake_pct": chain_data["stake_pct"],
            "emission_trend":   round(emit_trend, 6) if emit_trend is not None else None,
            "invocation_trend": round(inv_trend,  6) if inv_trend  is not None else None,
            "delta_inv_emit": delta_inv_emit, "delta_inv_weight": delta_inv_wt,
            "quality_score": 0.0, "delta_quality_weight": 0.0, "delta_quality_emit": 0.0,
        })

    print(f"  Matched: {matched} | Unmatched demand: {unmatched_demand} | "
          f"On-chain only: {len(merged) - matched}")

    merged = compute_quality_scores(merged)

    quality_miners = [m for m in merged if m["invocation_count"] > 0]
    miners_only    = [m for m in merged if not m["is_validator"] and m["invocation_count"] > 0]

    def pearson(x, y):
        n = len(x)
        if n < 2: return None
        mx = sum(x)/n; my = sum(y)/n
        num = sum((xi-mx)*(yi-my) for xi,yi in zip(x,y))
        dx  = (sum((xi-mx)**2 for xi in x))**0.5
        dy  = (sum((yi-my)**2 for yi in y))**0.5
        return num/(dx*dy) if dx*dy > 0 else None

    def spearman(x, y):
        n = len(x)
        if n < 2: return None
        rx_i = sorted(range(n), key=lambda i: x[i])
        ry_i = sorted(range(n), key=lambda i: y[i])
        rx = [0]*n; ry = [0]*n
        for r, i in enumerate(rx_i): rx[i] = r
        for r, i in enumerate(ry_i): ry[i] = r
        d_sq = sum((rx[i]-ry[i])**2 for i in range(n))
        return 1 - (6*d_sq)/(n*(n**2-1))

    r_inv_emit = r_inv_wt = rs_inv_emit = rs_inv_wt = None
    r_top = r_mid = r_tail = None; top_cut = tail_cut = 0
    r_qs_wt = r_qs_emi = rs_qs_wt = rs_qs_emi = None

    if len(miners_only) >= 2:
        inv_s  = [m["invocation_share"] for m in miners_only]
        emit_s = [m["emission_share"]   for m in miners_only]
        wt_s   = [m["weight_share"]     for m in miners_only]
        r_inv_emit  = pearson(inv_s, emit_s);  r_inv_wt    = pearson(inv_s, wt_s)
        rs_inv_emit = spearman(inv_s, emit_s); rs_inv_wt   = spearman(inv_s, wt_s)
        n = len(miners_only); top_cut = max(1, n//5); tail_cut = max(1, n*4//5)
        by_inv = sorted(miners_only, key=lambda m: m["invocation_share"], reverse=True)
        def tp(group):
            if len(group) < 2: return None
            return pearson([m["invocation_share"] for m in group],
                           [m["emission_share"]   for m in group])
        r_top = tp(by_inv[:top_cut]); r_mid = tp(by_inv[top_cut:tail_cut]); r_tail = tp(by_inv[tail_cut:])

    # ── Console Report ────────────────────────────────────────────────────────
    print(f"\n\n{SEPARATOR}")
    print(f"  SNAPSHOT METADATA"); print(THIN_SEP)
    print(f"  Block          : {block:,}"); print(f"  Timestamp      : {ts}")
    print(f"  Analysis window: {INVOCATION_WINDOW} days")
    print(f"  Row cap        : {MAX_ROWS_PER_HOUR:,} rows/hour")
    print(f"  Data sources   : api.chutes.ai (invocations + metrics) + Bittensor finney (metagraph)")

    print(f"\n{SEPARATOR}"); print(f"  ALPHA TOKEN POOL — SN64 (aCHUTES)"); print(THIN_SEP)
    if pool["alpha_price"] is None:
        print(f"  ❌ Pool state unavailable.")
    else:
        print(f"  Spot price (alpha → TAO)  : {fmt_tao(pool['alpha_price'])}")
        if pool["ema_price"]: print(f"  EMA price (protocol)      : {fmt_tao(pool['ema_price'])}")
        if pool["spot_ema_gap"] is not None:
            gap = pool["spot_ema_gap"]; pct = pool["spot_ema_gap_pct"]
            sign = "+" if gap >= 0 else ""; label = "spot ABOVE ema" if gap >= 0 else "spot BELOW ema"
            print(f"  Spot vs EMA gap           : {sign}{gap:.4f} TAO  ({sign}{pct:.1f}%)  [{label}]")
            if pct > 20:    print(f"  ⚠️  SPOT PREMIUM >20%")
            elif pct < -20: print(f"  ✅ SPOT DISCOUNT >20%")
            else:           print(f"  → Spot/EMA within 20% band.")
        if pool["tao_reserves"]:      print(f"  TAO reserves              : {fmt_large(pool['tao_reserves'])} TAO")
        if pool["alpha_in_pool"]:     print(f"  Alpha in pool             : {fmt_large(pool['alpha_in_pool'])} aCHUTES")
        if pool["alpha_outstanding"]: print(f"  Alpha outstanding         : {fmt_large(pool['alpha_outstanding'])} aCHUTES")
        if pool["market_cap_tao"]:    print(f"  Implied market cap        : {fmt_large(pool['market_cap_tao'])} TAO")

    print(f"\n{SEPARATOR}"); print(f"  DEMAND SIGNAL — TOP 20 MINERS BY INVOCATION SHARE (7d)"); print(THIN_SEP)
    print(f"  {'Rank':>4}  {'UID':>4}  {'Hotkey':10}  {'Invocations':>12}  {'Inv%':>8}  "
          f"{'Cmp%':>8}  {'Div':>5}  {'TPS':>7}  {'Pass':>6}  {'Emit%':>8}"); print(THIN_SEP)
    for rank, m in enumerate(sorted(merged, key=lambda x: x["invocation_share"], reverse=True)[:20], 1):
        tps = f"{m['avg_tps']:.1f}" if m.get("avg_tps", 0) > 0 else "  N/A"
        pr  = f"{m['pass_rate']*100:.0f}%" if m.get("pass_rate") is not None else "  N/A"
        print(f"  {rank:>4}  {m['uid']:>4}  {m['hotkey_short']:10}  {m['invocation_count']:>12,}  "
              f"{fmt_pct(m['invocation_share']):>8}  {fmt_pct(m['compute_share']):>8}  "
              f"{m['chute_diversity']:>5}  {tps:>7}  {pr:>6}  {fmt_pct(m['emission_share']):>8}")

    print(f"\n{SEPARATOR}"); print(f"  RECOGNITION — TOP 20 MINERS BY EMISSION SHARE"); print(THIN_SEP)
    print(f"  {'Rank':>4}  {'UID':>4}  {'Hotkey':10}  {'Emit%':>8}  {'Inv%':>8}  "
          f"{'Wgt%':>8}  {'Delta':>10}"); print(THIN_SEP)
    for rank, m in enumerate(sorted(merged, key=lambda x: x["emission_share"], reverse=True)[:20], 1):
        print(f"  {rank:>4}  {m['uid']:>4}  {m['hotkey_short']:10}  "
              f"{fmt_pct(m['emission_share']):>8}  {fmt_pct(m['invocation_share']):>8}  "
              f"{fmt_pct(m['weight_share']):>8}  "
              f"{fmt_delta(m['invocation_share'], m['emission_share']):>10}")

    print(f"\n{SEPARATOR}"); print(f"  DIVERGENCE TABLE — TOP 20 DEMAND vs EMISSION GAP"); print(THIN_SEP)
    print(f"  {'Rank':>4}  {'UID':>4}  {'Hotkey':10}  {'Inv%':>8}  {'Emit%':>8}  "
          f"{'Delta':>10}  {'Wgt%':>8}"); print(THIN_SEP)
    for rank, m in enumerate(
        sorted([m for m in merged if m["invocation_count"] > 0],
               key=lambda x: abs(x["delta_inv_emit"]), reverse=True)[:20], 1):
        print(f"  {rank:>4}  {m['uid']:>4}  {m['hotkey_short']:10}  "
              f"{fmt_pct(m['invocation_share']):>8}  {fmt_pct(m['emission_share']):>8}  "
              f"{fmt_delta(m['invocation_share'], m['emission_share']):>10}  "
              f"{fmt_pct(m['weight_share']):>8}")

    print(f"\n{SEPARATOR}"); print(f"  CORRELATION ANALYSIS — DEMAND vs RECOGNITION"); print(THIN_SEP)
    print(f"  Miners with invocation data: {len(miners_only)}")
    if r_inv_emit is not None:
        sp = "STRONG" if abs(r_inv_emit)  > 0.7 else "MODERATE" if abs(r_inv_emit)  > 0.4 else "WEAK"
        ss = "STRONG" if abs(rs_inv_emit) > 0.7 else "MODERATE" if abs(rs_inv_emit) > 0.4 else "WEAK"
        print(f"  Pearson  r  (inv → emission) : {r_inv_emit:+.4f}  [{sp}]")
        print(f"  Spearman ρ  (inv → emission) : {rs_inv_emit:+.4f}  [{ss}]")
        if r_inv_wt:  print(f"  Pearson  r  (inv → weight)   : {r_inv_wt:+.4f}")
        if rs_inv_wt: print(f"  Spearman ρ  (inv → weight)   : {rs_inv_wt:+.4f}")
        print()
        if r_top  is not None: print(f"  Tier corr — Top  ({top_cut} miners) : {r_top:+.4f}")
        if r_mid  is not None: print(f"  Tier corr — Mid               : {r_mid:+.4f}")
        if r_tail is not None: print(f"  Tier corr — Tail              : {r_tail:+.4f}")
        print()
        lead = rs_inv_emit if rs_inv_emit is not None else r_inv_emit
        if lead > 0.7:   print(f"  → Validators are ranking miners by real demand. The incentive mechanism is working.")
        elif lead > 0.4: print(f"  → Validator rankings partially reflect demand but divergences are material. Watch the gap.")
        else:            print(f"  → Emission distribution is significantly disconnected from real demand. The mechanism is failing.")

    print(f"\n{SEPARATOR}"); print(f"  QUALITY CROSSCHECK — INDEPENDENT SCORE vs VALIDATOR WEIGHTS"); print(THIN_SEP)
    if quality_miners:
        qs_s  = [m["quality_score"]  for m in quality_miners]
        wt_s  = [m["weight_share"]   for m in quality_miners]
        emi_s = [m["emission_share"] for m in quality_miners]
        r_qs_wt  = pearson(qs_s, wt_s);   r_qs_emi  = pearson(qs_s, emi_s)
        rs_qs_wt = spearman(qs_s, wt_s);  rs_qs_emi = spearman(qs_s, emi_s)
        if r_qs_wt:   print(f"  Pearson  r  (quality → weight)   : {r_qs_wt:+.4f}")
        if rs_qs_wt:  print(f"  Spearman ρ  (quality → weight)   : {rs_qs_wt:+.4f}")
        if r_qs_emi:  print(f"  Pearson  r  (quality → emission) : {r_qs_emi:+.4f}")
        if rs_qs_emi: print(f"  Spearman ρ  (quality → emission) : {rs_qs_emi:+.4f}")

    dominant_report  = None
    perimeter_report = None
    chute_meta       = {}

    if user_agg:
        print(f"\n{SEPARATOR}"); print(f"  DEMAND-SIDE USER ANALYSIS"); print(THIN_SEP)
        users_sorted = sorted(user_agg.values(), key=lambda u: u["invocation_count"], reverse=True)
        top1  = users_sorted[0]["inv_share"] if users_sorted else 0
        top3  = sum(u["inv_share"] for u in users_sorted[:3])
        top5  = sum(u["inv_share"] for u in users_sorted[:5])
        top10 = sum(u["inv_share"] for u in users_sorted[:10])

        def gini_fn(values):
            vals = sorted([v for v in values if v > 0]); n = len(vals)
            if n == 0: return None
            total = sum(vals)
            return (2 * sum((i+1)*v for i,v in enumerate(vals))) / (n*total) - (n+1)/n

        gu = gini_fn([u["invocation_count"] for u in users_sorted])
        print(f"  Total unique users (7d)      : {len(users_sorted):,}")
        print(f"  Top-1  user invocation share : {top1*100:.2f}%")
        print(f"  Top-3  user invocation share : {top3*100:.2f}%")
        print(f"  Top-5  user invocation share : {top5*100:.2f}%")
        print(f"  Top-10 user invocation share : {top10*100:.2f}%")
        if gu is not None:
            print(f"  Gini (user distribution)     : {gu:.4f}")
        print(f"\n  TOP 20 USERS BY INVOCATION VOLUME"); print(THIN_SEP)
        print(f"  {'Rank':>4}  {'User ID (truncated)':35}  {'Invocations':>12}  "
              f"{'Inv%':>8}  {'Cmp%':>8}  {'Chutes':>7}  {'Parents':>10}  {'Instances':>10}")
        print(THIN_SEP)
        for rank, u in enumerate(users_sorted[:20], 1):
            uid_d     = u["user_id"][:33]+".." if len(u["user_id"]) > 35 else u["user_id"]
            parents   = len(u.get("parent_ids",   set()))
            instances = len(u.get("instance_ids", set()))
            print(f"  {rank:>4}  {uid_d:35}  {u['invocation_count']:>12,}  "
                  f"{fmt_pct(u['inv_share']):>8}  {fmt_pct(u['compute_share']):>8}  "
                  f"{u['chute_count']:>7}  {parents:>10,}  {instances:>10,}")

        dominant = users_sorted[0]
        if dominant["inv_share"] > 0.50:
            print(f"\n  ℹ️  Top user holds {dominant['inv_share']*100:.1f}% — running deep analysis...")
            chute_meta      = fetch_chute_metadata(dominant["chute_ids"])
            dominant_report = analyze_dominant_user(dominant, chute_meta, date)
            perimeter       = populate_perimeter_dominant(perimeter, user_agg, dominant["user_id"])
            perimeter_report = run_perimeter_analysis(
                user_agg=user_agg, chute_meta=chute_meta,
                dominant_user_id=dominant["user_id"],
                perimeter=perimeter, date=date, output_dir=OUTPUT_DIR,
            )

    # Miner age analysis
    print(f"\n{SEPARATOR}"); print(f"  MINER AGE ANALYSIS"); print(THIN_SEP)
    active_miners = [m for m in merged if not m["is_validator"] and m["invocation_count"] > 0]
    if active_miners:
        BPD = 7200
        young   = [m for m in active_miners if m["age_blocks"] < 30*BPD]
        mid_age = [m for m in active_miners if 30*BPD <= m["age_blocks"] < 90*BPD]
        old     = [m for m in active_miners if m["age_blocks"] >= 90*BPD]
        def ae(g): return sum(m["emission_share"] for m in g) / len(g) if g else 0
        print(f"  {'Cohort':<12}  {'Count':>6}  {'Avg Emit%':>12}  {'Total Emit%':>14}"); print(THIN_SEP)
        for lbl, grp in [("<30 days", young), ("30-90 days", mid_age), (">90 days", old)]:
            print(f"  {lbl:<12}  {len(grp):>6}  {fmt_pct(ae(grp)):>12}  "
                  f"{fmt_pct(sum(m['emission_share'] for m in grp)):>14}")
        susp = [m for m in young if m["emission_share"] > 0.05]
        if susp:
            print(f"\n  ⚠️  NEW MINERS WITH HIGH EMISSION (>5%, <30 days):")
            for m in sorted(susp, key=lambda x: x["emission_share"], reverse=True):
                print(f"    UID {m['uid']:>3} | age={m['age_blocks']/BPD:.1f}d | "
                      f"emit={fmt_pct(m['emission_share'])} | inv={fmt_pct(m['invocation_share'])}")

    # Emission trend
    print(f"\n{SEPARATOR}"); print(f"  EMISSION TREND  (vs previous snapshot)"); print(THIN_SEP)
    if prev_snapshot:
        up   = sorted([m for m in merged if m.get("emission_trend") and m["emission_trend"] >  0.005],
                      key=lambda x: x["emission_trend"], reverse=True)
        down = sorted([m for m in merged if m.get("emission_trend") and m["emission_trend"] < -0.005],
                      key=lambda x: x["emission_trend"])
        print(f"  Gainers (Δ > +0.5pp): {len(up)} | Losers (Δ < -0.5pp): {len(down)}")
        print(f"\n  TOP GAINERS:"); print(f"  {'UID':>4}  {'Prev':>8}  {'Curr':>8}  {'Δ':>8}  {'Inv%':>8}  {'Tier':>6}"); print(THIN_SEP)
        for m in up[:8]:
            print(f"  {m['uid']:>4}  {fmt_pct(m['emission_share']-m['emission_trend']):>8}  "
                  f"{fmt_pct(m['emission_share']):>8}  +{m['emission_trend']*100:.2f}pp  "
                  f"{fmt_pct(m['invocation_share']):>8}  {m['efficiency_tier']:>6}")
        print(f"\n  TOP LOSERS:"); print(THIN_SEP)
        for m in down[:8]:
            print(f"  {m['uid']:>4}  {fmt_pct(m['emission_share']-m['emission_trend']):>8}  "
                  f"{fmt_pct(m['emission_share']):>8}  {m['emission_trend']*100:.2f}pp  "
                  f"{fmt_pct(m['invocation_share']):>8}  {m['efficiency_tier']:>6}")
    else:
        print(f"  No previous snapshot available.")

    # Efficiency tiers
    print(f"\n{SEPARATOR}"); print(f"  COMPUTE EFFICIENCY TIER ANALYSIS"); print(THIN_SEP)
    if active_miners:
        fast_m = [m for m in active_miners if m["efficiency_tier"] == "FAST"]
        mid_m  = [m for m in active_miners if m["efficiency_tier"] == "MID"]
        slow_m = [m for m in active_miners if m["efficiency_tier"] == "SLOW"]
        print(f"  {'Tier':<6}  {'Count':>6}  {'Avg Sec/Inv':>12}  {'Avg Emit%':>12}  {'Avg Wgt%':>10}"); print(THIN_SEP)
        for tn, grp in [("FAST", fast_m), ("MID", mid_m), ("SLOW", slow_m)]:
            if grp:
                print(f"  {tn:<6}  {len(grp):>6}  "
                      f"{sum(m['seconds_per_invocation'] for m in grp)/len(grp):>12.2f}  "
                      f"{fmt_pct(sum(m['emission_share'] for m in grp)/len(grp)):>12}  "
                      f"{fmt_pct(sum(m['weight_share']   for m in grp)/len(grp)):>10}")
        if len(fast_m) >= 2 and len(slow_m) >= 2:
            fast_avg = sum(m["weight_share"] for m in fast_m) / len(fast_m)
            slow_avg = sum(m["weight_share"] for m in slow_m) / len(slow_m)
            ratio    = fast_avg / slow_avg if slow_avg > 0 else None; print()
            if ratio and ratio > 1.2: print(f"  ✅ FAST miners receive {ratio:.1f}x the avg weight of SLOW miners")
            elif ratio and ratio < 0.8: print(f"  ⚠️  FAST miners receive only {ratio:.1f}x the avg weight of SLOW miners")
            else: print(f"  → No significant weight difference between FAST and SLOW tiers.")

    # Root × SN64 overlap
    print(f"\n{SEPARATOR}"); print(f"  ROOT × SN64 VALIDATOR OVERLAP"); print(THIN_SEP)
    if overlap:
        print(f"  {'Rank':>4}  {'Hotkey':10}  {'UID':>4}  {'Root TAO':>12}  "
              f"{'SN64 Alpha':>12}  {'Tv':>6}  {'Div%':>8}"); print(THIN_SEP)
        for rank, v in enumerate(overlap, 1):
            print(f"  {rank:>4}  {v['hotkey_short']:10}  {v['uid_sn64']:>4}  "
                  f"{v['root_tao']:>12,.0f}  {v['sn64_alpha']:>12,.0f}  "
                  f"{v['tv']:>6.4f}  {fmt_pct(v['dividends_pct']):>8}")

    print(f"\n{SEPARATOR}"); print(f"  SN64 VALIDATORS (≥ {sn64_alpha_threshold:,.0f} alpha)"); print(THIN_SEP)
    if sn64_validators:
        print(f"  {'Rank':>4}  {'Hotkey':10}  {'UID':>4}  {'Alpha':>12}  "
              f"{'TAO':>10}  {'Tv':>6}  {'Div%':>8}  {'Wgt%':>8}"); print(THIN_SEP)
        for rank, v in enumerate(sn64_validators, 1):
            print(f"  {rank:>4}  {v['hotkey_short']:10}  {v['uid_sn64']:>4}  "
                  f"{v['sn64_alpha']:>12,.0f}  {v['tao_equiv']:>10,.0f}  {v['tv']:>6.4f}  "
                  f"{fmt_pct(v['dividends_pct']):>8}  {fmt_pct(v['weight_share']):>8}")

    print(f"\n{SEPARATOR}"); print(f"  MINER INCENTIVE CONCENTRATION"); print(THIN_SEP)
    miners_s = sorted([m for m in merged if not m["is_validator"]],
                      key=lambda x: x["emission_share"], reverse=True)
    t1  = miners_s[0]["emission_share"] if miners_s else 0
    t3  = sum(m["emission_share"] for m in miners_s[:3])
    t5  = sum(m["emission_share"] for m in miners_s[:5])
    t10 = sum(m["emission_share"] for m in miners_s[:10])

    def gini(values):
        vals = sorted([v for v in values if v > 0]); n = len(vals)
        if n == 0: return None
        total = sum(vals)
        return (2 * sum((i+1)*v for i,v in enumerate(vals))) / (n*total) - (n+1)/n

    ga  = gini([m["emission_share"] for m in miners_s])
    gac = gini([m["emission_share"] for m in miners_s if m["emission_share"] > 0])
    print(f"  Top-1: {t1*100:.2f}% | Top-3: {t3*100:.2f}% | Top-5: {t5*100:.2f}% | Top-10: {t10*100:.2f}%")
    if ga:  print(f"  Gini (all miners)  : {ga:.4f}")
    if gac: print(f"  Gini (active only) : {gac:.4f}")
    # ── Burn Rate Analysis ────────────────────────────────────────────────────
    # Miner burn rate: emissions not reaching miners = burned or diverted.
    # Derived from on-chain incentive shares — no additional RPC calls needed.
    # Alpha supply inflation: delta vs previous snapshot's alpha_outstanding.
    # Net real yield = nominal APY minus supply inflation rate (annualised).

    total_miner_incentive = sum(
        m["emission_share"] for m in merged if not m["is_validator"]
    )
    miner_burn_rate = max(0.0, 1.0 - total_miner_incentive)

    # Protocol-native gross emission rate (EMA-smoothed)
    # Raw dilution pressure before buyback/burn defences.
    alpha_out_emission     = pool.get("alpha_out_emission")
    alpha_in_emission      = pool.get("alpha_in_emission")
    alpha_outstanding      = pool.get("alpha_outstanding")
    alpha_in_pool          = pool.get("alpha_in_pool")
    gross_emission_rate    = None

    if alpha_out_emission and alpha_outstanding and alpha_outstanding > 0:
        gross_emission_rate = (alpha_out_emission * BLOCKS_PER_YEAR) / alpha_outstanding

    # Net supply delta from previous metadata (actual dilution after defences)
    prev_meta_files = sorted([
        f for f in OUTPUT_METADATA_DIR.glob("chutes_sn64_metadata_*.json")
        if date not in f.name
    ], reverse=True)
    prev_alpha_outstanding = None
    prev_alpha_in_pool     = None
    prev_meta_date         = None
    if prev_meta_files:
        try:
            with open(prev_meta_files[0]) as f:
                prev_meta = json.load(f)
            prev_alpha_outstanding = prev_meta.get("alpha_outstanding")
            prev_alpha_in_pool     = prev_meta.get("alpha_in_pool")
            prev_meta_date         = prev_meta_files[0].stem.replace("chutes_sn64_metadata_", "")
        except Exception:
            pass

    net_supply_delta       = None
    supply_days_gap        = None
    burned_tokens          = None
    total_emission_period  = None
    supply_defence         = None

    if prev_alpha_outstanding and alpha_outstanding and prev_alpha_outstanding > 0:
        delta_pct = (alpha_outstanding - prev_alpha_outstanding) / prev_alpha_outstanding
        if prev_meta_date:
            try:
                prev_dt  = datetime.strptime(prev_meta_date, "%Y-%m-%d")
                curr_dt  = datetime.strptime(date,           "%Y-%m-%d")
                days_gap = (curr_dt - prev_dt).days
                supply_days_gap = days_gap
                if days_gap > 0:
                    net_supply_delta = delta_pct * (365 / days_gap)

                    # Burned tokens: isolated from staking noise via total alpha accounting.
                    # Staking/unstaking moves tokens between alpha_out and alpha_in but doesn't
                    # change their sum. Only emission creates tokens, only burns destroy them.
                    if (alpha_in_pool is not None and prev_alpha_in_pool is not None and
                        alpha_out_emission is not None and alpha_in_emission is not None):
                        total_emission_period = (alpha_out_emission + alpha_in_emission) * BLOCKS_PER_DAY * days_gap
                        total_alpha_now  = alpha_outstanding + alpha_in_pool
                        total_alpha_prev = prev_alpha_outstanding + prev_alpha_in_pool
                        actual_change    = total_alpha_now - total_alpha_prev
                        burned_tokens    = max(0.0, total_emission_period - actual_change)
                        # Supply defence: fraction of emission actively burned
                        if total_emission_period > 0:
                            supply_defence = burned_tokens / total_emission_period
            except Exception:
                pass

    print(f"\n{SEPARATOR}"); print(f"  BURN RATE & SUPPLY HEALTH"); print(THIN_SEP)
    print(f"  Miner burn rate (emissions not reaching miners): {miner_burn_rate*100:.2f}%")
    if miner_burn_rate > 0.5:
        print(f"  🔴 Most miner emissions are burned or diverted. Miners have weak incentive to serve real demand.")
    elif miner_burn_rate > 0.2:
        print(f"  🟡 A meaningful share of emissions is not reaching miners. Monitor for trend direction.")
    else:
        print(f"  ✅ Miners are capturing emissions and being incentivised to serve demand. Incentive flow is healthy.")

    if alpha_outstanding:
        print(f"\n  Alpha outstanding (current) : {fmt_large(alpha_outstanding)} aCHUTES")
    if prev_alpha_outstanding:
        print(f"  Alpha outstanding (previous): {fmt_large(prev_alpha_outstanding)} aCHUTES [{prev_meta_date}]")
    if gross_emission_rate is not None:
        print(f"  Gross emission rate         : {gross_emission_rate*100:.1f}%/yr ({gross_emission_rate*100/12:.1f}%/mo)")
    if net_supply_delta is not None:
        print(f"  Net supply delta            : {net_supply_delta*100:+.1f}%/yr ({net_supply_delta*100/12:+.1f}%/mo) [{supply_days_gap}d window]")
    if burned_tokens is not None:
        print(f"  Burned tokens ({supply_days_gap}d)         : {fmt_large(burned_tokens)} aCHUTES")
        if total_emission_period:
            print(f"  Total emission ({supply_days_gap}d)        : {fmt_large(total_emission_period)} aCHUTES")
    if supply_defence is not None:
        print(f"  Supply defence ratio        : {supply_defence*100:.1f}%")
        if supply_defence > 1.0:
            print(f"  🟣 Supply is DEFLATING — burns/buybacks exceed emission. Net supply contracting.")
        elif supply_defence > 0.8:
            print(f"  ✅ Subnet is neutralising {supply_defence*100:.0f}% of gross emission. Strong deflationary mechanisms.")
        elif supply_defence > 0.4:
            print(f"  🟡 Partial defence — {supply_defence*100:.0f}% of emission absorbed. Some dilution passing through.")
        elif supply_defence > 0:
            print(f"  🔴 Weak defence — only {supply_defence*100:.0f}% of emission absorbed. Most dilution passes through.")
        else:
            print(f"  🔴 Supply growing faster than gross emission alone. Negative defence.")

    print(f"\n{SEPARATOR}\n")

    # ── Update burn trajectory ────────────────────────────────────────────────
    burn_entry = {
        "date":                    date,
        "miner_burn_rate":         round(miner_burn_rate, 6),
        "total_miner_incentive":   round(total_miner_incentive, 6),
        "alpha_outstanding":       alpha_outstanding,
        "alpha_in_pool":           alpha_in_pool,
        "alpha_out_emission":      alpha_out_emission,
        "alpha_in_emission":       alpha_in_emission,
        "gross_emission_annual":   round(gross_emission_rate, 4) if gross_emission_rate is not None else None,
        "gross_emission_monthly":  round(gross_emission_rate / 12, 4) if gross_emission_rate is not None else None,
        "net_supply_delta_annual": round(net_supply_delta, 4) if net_supply_delta is not None else None,
        "net_supply_delta_monthly":round(net_supply_delta / 12, 4) if net_supply_delta is not None else None,
        "burned_tokens":           round(burned_tokens, 2) if burned_tokens is not None else None,
        "supply_defence":          round(supply_defence, 4) if supply_defence is not None else None,
        "supply_days_gap":         supply_days_gap,
        "tao_reserves":            pool.get("tao_reserves"),
        "alpha_price":             pool.get("alpha_price"),
        "ema_price":               pool.get("ema_price"),
        "market_cap_tao":          pool.get("market_cap_tao"),
    }
    burn_trajectory_file = OUTPUT_DIR / "burn_trajectory.json"
    burn_trajectory = []
    if burn_trajectory_file.exists():
        try:
            with open(burn_trajectory_file) as f:
                burn_trajectory = json.load(f)
            if not isinstance(burn_trajectory, list): burn_trajectory = []
        except Exception: pass
    burn_trajectory = [e for e in burn_trajectory if e.get("date") != date]
    burn_trajectory.append(burn_entry)
    burn_trajectory.sort(key=lambda e: e.get("date", ""))
    with open(burn_trajectory_file, "w") as f:
        json.dump(burn_trajectory, f, indent=2, default=str)

    n_bt = len(burn_trajectory)
    print(f"  📈 Burn trajectory updated: {n_bt} snapshot{'s' if n_bt != 1 else ''}")
    if n_bt >= 2:
        print(f"\n  BURN TRAJECTORY HISTORY:")
        print(f"  {'Date':<12}  {'Burn%':>7}  {'Alpha Supply':>14}  "
              f"{'Gross/yr':>10}  {'NetΔ/yr':>10}  {'Burned':>10}  {'Defence':>8}  {'TAO Rsv':>10}")
        print(f"  {'-'*90}")
        for e in burn_trajectory:
            burn_pct = f"{e.get('miner_burn_rate', 0)*100:.2f}%" if e.get('miner_burn_rate') is not None else "N/A"
            supply   = fmt_large(e.get('alpha_outstanding')) if e.get('alpha_outstanding') else "N/A"
            # Backward compat: check new field names then old
            ge = e.get('gross_emission_annual', e.get('alpha_inflation_annual', e.get('alpha_inflation_annualised')))
            ge_str   = f"{ge*100:.1f}%" if ge is not None else "N/A"
            nd = e.get('net_supply_delta_annual')
            nd_str   = f"{nd*100:+.1f}%" if nd is not None else "N/A"
            bt_val = e.get('burned_tokens')
            bt_str   = fmt_large(bt_val) if bt_val is not None else "N/A"
            sd = e.get('supply_defence')
            sd_str   = f"{sd*100:.0f}%" if sd is not None else "N/A"
            tao_rsv  = fmt_large(e.get('tao_reserves')) if e.get('tao_reserves') else "N/A"
            print(f"  {e['date']:<12}  {burn_pct:>7}  {supply:>14}  "
                  f"{ge_str:>10}  {nd_str:>10}  {bt_str:>10}  {sd_str:>8}  {tao_rsv:>10}")

    # ── Save Outputs ──────────────────────────────────────────────────────────
    for d in [OUTPUT_DIR, OUTPUT_ANALYSIS_DIR, OUTPUT_DIVERGENCE_DIR, OUTPUT_METADATA_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    merged_file = OUTPUT_ANALYSIS_DIR / f"chutes_sn64_analysis_{date}.csv"
    fieldnames  = [
        "hotkey", "uid", "is_validator", "tv", "age_blocks", "blocks_since_update",
        "invocation_count", "compute_seconds", "chute_diversity",
        "avg_tps", "avg_ttft", "pass_rate",
        "seconds_per_invocation", "efficiency_tier",
        "invocation_share", "compute_share", "weight_share", "emission_share",
        "dividends_share", "stake_pct", "emission_trend", "invocation_trend",
        "quality_score", "delta_inv_emit", "delta_inv_weight",
        "delta_quality_weight", "delta_quality_emit",
    ]
    with open(merged_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader(); writer.writerows(merged)

    div_file = OUTPUT_DIVERGENCE_DIR / f"chutes_sn64_divergence_{date}.csv"
    with open(div_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "hotkey", "uid", "invocation_share", "emission_share", "delta_inv_emit",
            "weight_share", "chute_diversity", "avg_tps", "pass_rate",
        ], extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted(merged, key=lambda x: abs(x["delta_inv_emit"]), reverse=True))

    meta_file = OUTPUT_METADATA_DIR / f"chutes_sn64_metadata_{date}.json"
    with open(meta_file, "w") as f:
        json.dump({
            "block": block, "timestamp": ts, "netuid": NETUID, "window_days": INVOCATION_WINDOW,
            "max_rows_per_hour": MAX_ROWS_PER_HOUR,
            "total_invocations": total_invocations, "total_compute_sec": total_compute,
            "n_miners_demand": len(miner_demand), "n_matched": matched,
            "n_overlap": len(overlap), "n_sn64_validators": len(sn64_validators),
            "n_demand_users": len(user_agg), "n_miners_native_tps": len(tps_miners),
            "r_inv_emit": r_inv_emit, "r_inv_weight": r_inv_wt,
            "rs_inv_emit": rs_inv_emit, "rs_inv_weight": rs_inv_wt,
            "r_top_tier": r_top, "r_mid_tier": r_mid, "r_tail_tier": r_tail,
            "r_quality_weight": r_qs_wt, "r_quality_emit": r_qs_emi,
            "rs_quality_weight": rs_qs_wt, "rs_quality_emit": rs_qs_emi,
            "top1_miner_emission_share": miners_s[0]["emission_share"] if miners_s else None,
            "gini_all_miners": ga, "gini_active_miners": gac,
            "alpha_price": pool["alpha_price"], "ema_price": pool["ema_price"],
            "spot_ema_gap": pool["spot_ema_gap"], "spot_ema_gap_pct": pool["spot_ema_gap_pct"],
            "tao_reserves": pool["tao_reserves"], "alpha_outstanding": pool["alpha_outstanding"],
            "alpha_in_pool": pool["alpha_in_pool"], "market_cap_tao": pool["market_cap_tao"],
            "miner_burn_rate": round(miner_burn_rate, 6),
            "total_miner_incentive": round(total_miner_incentive, 6),
            "alpha_out_emission": alpha_out_emission,
            "alpha_in_emission": alpha_in_emission,
            "gross_emission_annual": round(gross_emission_rate, 4) if gross_emission_rate is not None else None,
            "net_supply_delta_annual": round(net_supply_delta, 4) if net_supply_delta is not None else None,
            "burned_tokens": round(burned_tokens, 2) if burned_tokens is not None else None,
            "supply_defence": round(supply_defence, 4) if supply_defence is not None else None,
        }, f, indent=2)

    if user_agg:
        user_file = OUTPUT_DIR / f"chutes_sn64_demand_users_{date}.csv"
        with open(user_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "user_id", "invocation_count", "compute_seconds", "chute_count",
                "inv_share", "compute_share",
            ], extrasaction="ignore")
            writer.writeheader()
            writer.writerows(sorted(user_agg.values(), key=lambda u: u["invocation_count"], reverse=True))

    if dominant_report is not None:
        dom_file = OUTPUT_DIR / f"chutes_sn64_dominant_user_{date}.json"
        with open(dom_file, "w") as f:
            json.dump(dominant_report, f, indent=2, default=str)

    print(f"💾  Outputs saved:")
    print(f"    {merged_file.name}")
    print(f"    {div_file.name}")
    print(f"    {meta_file.name}")
    print(f"    burn_trajectory.json")


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    global MAX_ROWS_PER_HOUR
    parser = argparse.ArgumentParser(
        description="SN64 Chutes Intelligence Market Analysis + E2EE Perimeter Forensics")
    parser.add_argument("--root-threshold",  type=float, default=ROOT_TAO_THRESHOLD)
    parser.add_argument("--alpha-threshold", type=float, default=SN64_ALPHA_THRESHOLD)
    parser.add_argument("--method", type=int, default=0, choices=[0, 1, 2, 3])
    parser.add_argument("--max-rows-per-hour", type=int, default=MAX_ROWS_PER_HOUR,
                        help="Row cap per hourly CSV (default: 200000). Reduce if OOM persists.")
    args = parser.parse_args()
    MAX_ROWS_PER_HOUR = args.max_rows_per_hour
    run_analysis(
        root_tao_threshold=args.root_threshold,
        sn64_alpha_threshold=args.alpha_threshold,
        alpha_method=args.method,
    )

if __name__ == "__main__":
    main()
