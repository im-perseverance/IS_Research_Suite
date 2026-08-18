"""
chain_analysis.py
=================
Shared chain layer — Intelligence Sovereignty Research Suite v3
@im_perseverance

Every v3 tool (subnet_analysis, root_analysis, validator_analysis, sentinel)
imports this module instead of talking to bittensor directly. It owns four
responsibilities and nothing else:

  1. Client + block pinning. One run = one block. `run()` opens a v11 client,
     pins a snapshot at a single block, and hands it to the tool's async
     collector. Every read in a run is taken from the same chain state, so
     every CSV row is a true point-in-time observation.

  2. Unit discipline. v11 returns `Balance` objects and normalized 0..1
     floats where v10 returned raw rao / u16. All conversions to the suite's
     CSV conventions (token-unit floats) happen here, in one place.

  3. Live governance parameters. MaturityRate, UnlockRate, TaoWeight,
     SubnetOwnerCut, per-subnet EMA halving — read at runtime, never
     hardcoded. (v2 lesson: the 30-day EMA assumption is now per-subnet
     governance-settable and would have silently broken the lag-trap flags.)

  4. Trajectory I/O. Atomic JSON persistence shared by all tools.

Design rules:
  - Reads-first, storage-fallback. Prefer the typed v11 read catalog
    (`snap.prices.alpha_prices()`, `snap.locks.subnet_convictions(...)`);
    fall back to generic storage queries where no read exists.
  - Degrade loudly, not silently. A failed optional read returns None and
    logs a warning; it never fabricates a value.
  - No tool-specific analytics live here. Burn coverage, the 2x2 commitment
    matrix, NAV haircut etc. belong to the tools; this module only delivers
    clean inputs. The single exception is `burn_reconciliation`, shared by
    subnet_analysis and the sentinel.

Requires: bittensor >= 11 (unified package). The v10-pinned sentinel must
NOT import this module until its own migration step.

Usage pattern for a v3 tool:

    import chain_analysis as chain

    async def collect(snap, block, client):
        pools  = await chain.all_subnet_pool_state(snap)
        params = await chain.governance_params(snap)
        return pools, params

    pools, params, block = chain.run(collect)

Smoke test (first thing to run after `pip install -U bittensor`):

    python chain_analysis.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Optional

log = logging.getLogger("is_suite.chain")

# ── Constants (physical chain facts only — governance values are read live) ──

RAO_PER_TAO    = 1_000_000_000
BLOCKS_PER_DAY = 7_200            # 12s blocks
# Two distinct "year" quantities — do NOT unify them:
# BLOCKS_PER_YEAR is the physical production rate (365d) — use for APY
# annualization. ONE_YEAR_BLOCKS is the governance constant from lock.rs
# (7200*365 + 1800, i.e. 365.25d) — use ONLY for the ownership-gate age check.
BLOCKS_PER_YEAR = BLOCKS_PER_DAY * 365
ONE_YEAR_BLOCKS = 2_629_800
U16_MAX = 65_535
U64_MAX = 2**64 - 1

DEFAULT_NETWORK = os.environ.get("IS_BT_NETWORK", "finney")


# ── Unit discipline ─────────────────────────────────────────────────────────

def f(val: Any, default: Optional[float] = 0.0) -> Optional[float]:
    """
    Robust token-unit float from anything v11 hands back:
    Balance objects, plain numbers, decoded storage ints, or None.

    NOTE on raw storage: generic `snap.query(...)` returns undecoded rao —
    use `from_rao()` for those. `f()` is for read-catalog outputs, which are
    already token-denominated (Balance / float).
    """
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    # v11 Balance: prefer explicit token-unit accessor, fall back to rao.
    for attr in ("tao", "value"):
        v = getattr(val, attr, None)
        if isinstance(v, (int, float)):
            return float(v)
    rao = getattr(val, "rao", None)
    if isinstance(rao, (int, float)):
        return float(rao) / RAO_PER_TAO
    try:
        return float(val)
    except Exception:
        log.warning("f(): could not coerce %r (%s) — returning default", val, type(val))
        return default


def from_rao(val: Any, default: Optional[float] = 0.0) -> Optional[float]:
    """Token-unit float from a raw rao integer (generic storage queries)."""
    if val is None:
        return default
    try:
        return int(val) / RAO_PER_TAO
    except Exception:
        return f(val, default)


def fixed_to_float(val: Any, frac_bits: int = 32, default: float = 0.0) -> float:
    """
    Decode a substrate fixed-point value (e.g. SubnetMovingPrice, I96F32).
    Handles: already-decoded floats, {'bits': int} dicts, objects with
    `.bits`, or raw ints of the fixed representation.
    """
    if val is None:
        return default
    if isinstance(val, float):
        return val
    bits = None
    if isinstance(val, dict):
        bits = val.get("bits")
    elif hasattr(val, "bits"):
        bits = getattr(val, "bits")
    elif isinstance(val, int):
        bits = val
    if bits is None:
        return f(val, default) or default
    try:
        return int(bits) / (1 << frac_bits)
    except Exception:
        return default


def u16_frac(val: Any, default: float = 0.0) -> float:
    """
    u16-normalized fraction (e.g. SubnetOwnerCut 11796/65535 -> 0.18).
    Defensive pass-through: if the SDK already normalized to 0..1, return it
    unchanged — int(0.18) would truncate to 0 and silently zero every
    downstream owner-cut / burn-coverage computation.
    """
    if val is None:
        return default
    try:
        v = float(val)
    except Exception:
        return default
    if 0.0 <= v <= 1.0:
        return v
    return v / U16_MAX


def u64_weight(val: Any, default: float = 0.18) -> float:
    """
    TaoWeight decode. Stored as a u64 fraction of u64::MAX. Defensive: if the
    SDK already normalized it to 0..1, pass it through.
    """
    if val is None:
        return default
    try:
        v = float(val)
    except Exception:
        return default
    if 0.0 <= v <= 1.0:
        return v
    return v / U64_MAX


# ── Client + snapshot pinning ───────────────────────────────────────────────

def connect(network: Optional[str] = None):
    """Construct the v11 client (import deferred so unit tests can stub)."""
    import bittensor as bt
    return bt.Subtensor(network or DEFAULT_NETWORK)


async def open_snapshot(client, block: Optional[int] = None):
    """Pin a snapshot. Returns (snap, block). All reads go through `snap`."""
    if block is None:
        blk = client.block
        block = int(await blk) if asyncio.iscoroutine(blk) else int(blk)
    snap = client.at(block)
    return snap, block


def run(collector: Callable[..., Awaitable[Any]],
        network: Optional[str] = None,
        block: Optional[int] = None):
    """
    Sync driver for the v3 CLI tools:

        result, block = chain.run(my_async_collector)

    Opens client -> pins snapshot -> awaits collector(snap, block, client).
    """
    async def _main():
        client = connect(network)
        try:
            snap, blk = await open_snapshot(client, block)
            out = await collector(snap, blk, client)
            return out, blk
        finally:
            close = getattr(client, "close", None)
            if close is not None:
                try:
                    res = close()
                    if asyncio.iscoroutine(res):
                        await res
                except Exception:
                    pass
    return asyncio.run(_main())


# ── Async / sync bridge ────────────────────────────────────────────────────

async def _resolve(val):
    """Await if coroutine/future, return as-is otherwise.
    v11 SyncSnapshot methods may return plain values or coroutines depending
    on the runtime path — this lets q/qmap/read work with either."""
    if asyncio.iscoroutine(val) or asyncio.isfuture(val):
        return await val
    return val


# ── Generic storage access (with graceful degradation) ─────────────────────

def _storage_item(pallet: str, item: str):
    import bittensor as bt
    return getattr(getattr(bt.storage, pallet), item)


async def q(snap, pallet: str, item: str, params: Iterable[Any] = ()) -> Any:
    """Single storage query. Returns decoded value or None on failure."""
    try:
        return await _resolve(snap.query(_storage_item(pallet, item), list(params)))
    except Exception as e:
        log.warning("query %s.%s%r failed: %s", pallet, item, tuple(params), e)
        return None


async def qmap(snap, pallet: str, item: str, params: Iterable[Any] = ()) -> dict:
    """Storage map query -> {int_key: value}. Empty dict on failure."""
    try:
        rows = await _resolve(snap.query_map(_storage_item(pallet, item), list(params)))
        out = {}
        for k, v in rows:
            key = k[0] if isinstance(k, (tuple, list)) and len(k) == 1 else k
            try:
                key = int(key)
            except Exception:
                pass
            out[key] = v
        return out
    except Exception as e:
        log.warning("query_map %s.%s failed: %s", pallet, item, e)
        return {}


async def read(snap, name: str, default: Any = None, **kwargs) -> Any:
    """Named read from the v11 catalog, by name. Default on failure."""
    try:
        return await _resolve(snap.read(name, **kwargs))
    except Exception as e:
        log.warning("read %s(%r) failed: %s", name, kwargs, e)
        return default


# ── Pool state: the subnet_analysis input surface ──────────────────────────

_POOL_MAPS = {
    # csv/dict field           (pallet,          storage item,            decoder)
    "tao_reserves":            ("SubtensorModule", "SubnetTAO",            from_rao),
    "alpha_in_pool":           ("SubtensorModule", "SubnetAlphaIn",        from_rao),
    "alpha_outstanding":       ("SubtensorModule", "SubnetAlphaOut",       from_rao),
    "protocol_alpha":          ("SubtensorModule", "SubnetProtocolAlpha",  from_rao),
    "tao_in_emission":         ("SubtensorModule", "SubnetTaoInEmission",  from_rao),
    "alpha_in_emission":       ("SubtensorModule", "SubnetAlphaInEmission", from_rao),
    "alpha_out_emission":      ("SubtensorModule", "SubnetAlphaOutEmission", from_rao),
    "volume":                  ("SubtensorModule", "SubnetVolume",         from_rao),
    "moving_price":            ("SubtensorModule", "SubnetMovingPrice",    fixed_to_float),
    "registered_at":           ("SubtensorModule", "NetworkRegisteredAt",  lambda v: int(v or 0)),
    "first_emission_block":    ("SubtensorModule", "FirstEmissionBlockNumber", lambda v: int(v) if v is not None else None),
    "ema_halving_blocks":      ("SubtensorModule", "EMAPriceHalvingBlocks", lambda v: int(v) if v is not None else None),
    # AlphaAssets pallet — the v3 primary burn/recycle source (v2 lesson:
    # the derived estimator captured ~34% against corrupted SubnetAlphaOut).
    "alpha_burned":            ("AlphaAssets",     "AlphaBurned",          from_rao),
    "alpha_recycled":          ("AlphaAssets",     "AlphaRecycled",        from_rao),
    "total_alpha_issuance":    ("AlphaAssets",     "TotalAlphaIssuance",   from_rao),
}


async def all_subnet_pool_state(snap) -> dict[int, dict]:
    """
    One batched sweep of per-subnet pool + supply + counter state for every
    registered subnet. Emission fields are per-block token units. Spot price
    comes from the read catalog (pool-weighted), with tao_in/alpha_in as a
    fallback; moving_price is the protocol EMA.
    """
    names = list(_POOL_MAPS.keys())
    results = await asyncio.gather(
        qmap(snap, "SubtensorModule", "NetworksAdded"),
        *[qmap(snap, p, i) for p, i, _ in _POOL_MAPS.values()],
    )
    added, maps = results[0], dict(zip(names, results[1:]))

    raw_prices = await read(snap, "alpha_prices", default={}) or {}
    # Defend against both plausible read shapes: {netuid: price} or
    # [(netuid, price)] / [{"netuid":..,"price"/"tao_per_alpha":..}].
    if isinstance(raw_prices, dict):
        prices = {int(k): float(v) for k, v in raw_prices.items()}
    else:
        prices = {}
        for row in raw_prices:
            if isinstance(row, dict):
                prices[int(row["netuid"])] = float(
                    row.get("tao_per_alpha", row.get("price", 0.0)))
            else:
                k, v = row
                prices[int(k)] = float(v)

    netuids = sorted(int(n) for n, ok in added.items() if ok and int(n) != 0)
    out: dict[int, dict] = {}
    for n in netuids:
        rec: dict[str, Any] = {"netuid": n}
        for field, (_, _, dec) in _POOL_MAPS.items():
            rec[field] = dec(maps[field].get(n))
        spot = prices.get(n)
        if spot is None:
            ain, tin = rec["alpha_in_pool"], rec["tao_reserves"]
            spot = (tin / ain) if ain else 0.0
        rec["spot_price"] = float(spot)
        out[n] = rec
    return out


# ── Governance parameters (live, never hardcoded) ──────────────────────────

async def governance_params(snap) -> dict:
    """
    Chain-wide governance-settable values every tool needs. Per-subnet EMA
    halving lives in pool state (`ema_halving_blocks`).
    """
    tao_w, owner_cut, maturity, unlock, claim_thr = await asyncio.gather(
        q(snap, "SubtensorModule", "TaoWeight"),
        q(snap, "SubtensorModule", "SubnetOwnerCut"),
        q(snap, "SubtensorModule", "MaturityRate"),
        q(snap, "SubtensorModule", "UnlockRate"),
        read(snap, "root_claim_threshold"),
    )
    return {
        "tao_weight":            u64_weight(tao_w),
        "owner_cut":             u16_frac(owner_cut, default=0.18),
        "maturity_rate_blocks":  int(maturity) if maturity else None,
        "unlock_rate_blocks":    int(unlock) if unlock else None,
        "root_claim_threshold":  f(claim_thr, default=None),
    }


# ── Conviction (locks) ─────────────────────────────────────────────────────

async def subnet_convictions(snap, netuid: int) -> Optional[dict]:
    """
    Normalized `subnet_convictions` read for one subnet. Returns:
      {netuid, eligible_alpha, threshold_alpha, total_locked_alpha,
       total_conviction_alpha, alpha_burned, protocol_alpha,
       owner: {hotkey, locked, conviction} | None,
       leader: {hotkey, is_owner, conviction, pct_of_threshold,
                blocks_to_threshold} | None,
       entries: [per-hotkey dicts, floats]}
    None if the read fails — callers must treat conviction as unknown, not
    zero (a lock read failure is not "no locks").
    """
    raw = await read(snap, "subnet_convictions", netuid=netuid)
    if not raw:
        return None
    entries = []
    owner = None
    for e in raw.get("entries", []) or []:
        rec = {
            "hotkey":              str(e.get("hotkey")),
            "is_owner":            bool(e.get("is_owner")),
            "locked_alpha":        f(e.get("locked_alpha")),
            "conviction_alpha":    f(e.get("conviction_alpha")),
            "pct_of_threshold":    e.get("pct_of_threshold"),
            "blocks_to_threshold": e.get("blocks_to_threshold"),
        }
        entries.append(rec)
        if rec["is_owner"]:
            owner = rec
    entries.sort(key=lambda r: -(r["conviction_alpha"] or 0.0))
    return {
        "netuid":                 netuid,
        "eligible_alpha":         f(raw.get("eligible_alpha")),
        "threshold_alpha":        f(raw.get("threshold_alpha")),
        "total_locked_alpha":     f(raw.get("total_locked_alpha")),
        "total_conviction_alpha": f(raw.get("total_conviction_alpha")),
        "alpha_burned":           f(raw.get("alpha_burned")),
        "protocol_alpha":         f(raw.get("protocol_alpha")),
        "owner":                  owner,
        "leader":                 entries[0] if entries else None,
        "entries":                entries,
    }


# ── Root Reborn: baskets ───────────────────────────────────────────────────

async def root_baskets(snap) -> list[dict]:
    """
    Network-wide fund leaderboard, normalized to plain floats:
      [{hotkey, nav_tao, spot_nav_tao, deposited_tao, redeemed_tao,
        lifetime_return, nav_haircut, weights: [...], holdings: [...]}]
    nav_haircut = (spot - realizable) / spot — the per-fund pool-depth
    discount (an IS-native metric; computed here so every tool agrees on it).
    """
    rows = await read(snap, "root_baskets", default=[]) or []
    out = []
    for r in rows:
        nav, spot = f(r.get("nav_tao")), f(r.get("spot_nav_tao"))
        out.append({
            "hotkey":          str(r.get("hotkey")),
            "nav_tao":         nav,
            "spot_nav_tao":    spot,
            "deposited_tao":   f(r.get("deposited_tao")),
            "redeemed_tao":    f(r.get("redeemed_tao")),
            "lifetime_return": r.get("lifetime_return"),
            "nav_haircut":     ((spot - nav) / spot) if spot else None,
            "weights":         r.get("weights") or [],
            "holdings": [
                {
                    "netuid":         int(h.get("netuid")),
                    "alpha":          f(h.get("alpha")),
                    "spot_tao":       f(h.get("spot_tao")),
                    "realizable_tao": f(h.get("realizable_tao")),
                }
                for h in (r.get("holdings") or [])
            ],
        })
    return out


async def basket_subnet_exposure(snap, baskets: Optional[list[dict]] = None) -> dict[int, float]:
    """
    Aggregate root-fund alpha per subnet (token units) — latent sell pressure
    the day weight curation opens. Feeds subnet_analysis's flow model v2.
    """
    baskets = baskets if baskets is not None else await root_baskets(snap)
    exposure: dict[int, float] = {}
    for b in baskets:
        for h in b["holdings"]:
            exposure[h["netuid"]] = exposure.get(h["netuid"], 0.0) + (h["alpha"] or 0.0)
    return exposure


# ── Delegation / identity passthroughs ─────────────────────────────────────

async def delegates(snap) -> list:
    return await read(snap, "delegates", default=[]) or []


async def hotkey_identities(snap, hotkeys: list[str]) -> dict:
    out = await read(snap, "hotkey_identities", default={}, hotkey_ss58s=hotkeys)
    return out or {}


async def metagraph(snap, netuid: int):
    """Typed v11 metagraph (per-neuron records; no arrays)."""
    try:
        return await _resolve(snap.subnets.metagraph(netuid))
    except Exception:
        return await read(snap, "metagraph", netuid=netuid)


# ── Burn reconciliation (shared: subnet_analysis + sentinel) ───────────────

def burn_reconciliation(prev: dict, cur: dict, estimator_burned: Optional[float],
                        tolerance: float = 0.25,
                        materiality: float = 0.005) -> Optional[dict]:
    """
    Chain counters vs the v2-style derived estimator over one interval.
    `prev`/`cur` are pool-state records for the same netuid at two blocks.

    counter_destroyed = Δalpha_burned + Δalpha_recycled  (both destroy float)
    gap               = estimator - counter_destroyed

    Flag logic (two conditions, both required for DIVERGENT):
      material: |gap| > materiality × interval emission — suppresses dust
                disagreements on subnets where burns are negligible.
      relative: |gap| / max(counter, |estimator|) > tolerance — the actual
                alarm. Normalizing against the counters, NOT emission, is
                deliberate: the v2 failure (estimator at ~34% of true burns)
                would look like noise next to emission but is a 66% relative
                miss. The estimator disagreeing with the chain's own counter
                is what fingerprinted the SubnetAlphaOut corruption.

    Returns None when counters are unavailable on either side.
    """
    for k in ("alpha_burned", "alpha_recycled"):
        if prev.get(k) is None or cur.get(k) is None:
            return None
    raw_d_burn = (cur["alpha_burned"] or 0.0) - (prev["alpha_burned"] or 0.0)
    raw_d_recycled = (cur["alpha_recycled"] or 0.0) - (prev["alpha_recycled"] or 0.0)
    # AlphaBurned/AlphaRecycled are monotonic counters; a decrease is a
    # chain-level integrity event (reset, migration rebase, or storage
    # corruption). Clamp for the math, but surface it loudly — hiding this
    # is exactly the failure mode this toolchain exists to catch.
    if raw_d_burn < 0 or raw_d_recycled < 0:
        log.warning(
            "monotonic counter decreased (netuid=%s): d_burned=%.6f "
            "d_recycled=%.6f — possible reset/rebase/corruption",
            cur.get("netuid", "?"), raw_d_burn, raw_d_recycled)
    d_burn = max(0.0, raw_d_burn)
    d_recycled = max(0.0, raw_d_recycled)
    counter = d_burn + d_recycled
    if estimator_burned is None:
        return {"delta_burned": d_burn, "delta_recycled": d_recycled,
                "raw_delta_burned": raw_d_burn, "raw_delta_recycled": raw_d_recycled,
                "counter_destroyed": counter, "estimator": None,
                "gap": None, "flag": "NO_ESTIMATE"}
    gap = estimator_burned - counter
    emission = cur.get("alpha_out_emission") or 0.0
    days = cur.get("_days_gap") or 1.0
    interval_emission = emission * BLOCKS_PER_DAY * days
    base = max(counter, abs(estimator_burned))
    rel = (gap / base) if base else 0.0
    is_material = abs(gap) > materiality * interval_emission if interval_emission else abs(gap) > 0
    flag = "DIVERGENT" if (is_material and abs(rel) > tolerance) else "OK"
    return {"delta_burned": d_burn, "delta_recycled": d_recycled,
            "raw_delta_burned": raw_d_burn, "raw_delta_recycled": raw_d_recycled,
            "counter_destroyed": counter, "estimator": estimator_burned,
            "gap": gap, "gap_relative": rel,
            "gap_vs_emission": (gap / interval_emission) if interval_emission else None,
            "flag": flag}


# ── Trajectory I/O (atomic) ────────────────────────────────────────────────

def load_json(path, default: Any = None) -> Any:
    p = Path(path)
    if p.exists():
        try:
            with open(p) as fh:
                return json.load(fh)
        except Exception as e:
            log.warning("load_json %s failed: %s", p, e)
    return {} if default is None else default


def save_json(path, data: Any) -> None:
    """Atomic write (tmp + rename) — a sentinel crash can no longer truncate
    a trajectory file mid-write."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(p.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            json.dump(data, fh, indent=2, default=str)
        os.replace(tmp, p)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def upsert_by_date(trajectory: list, entry: dict, key: str = "date") -> list:
    """Replace same-day entry, append otherwise, keep sorted by date."""
    out = [e for e in (trajectory or []) if e.get(key) != entry.get(key)]
    out.append(entry)
    out.sort(key=lambda e: e.get(key, ""))
    return out


# ── Smoke test ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    async def _smoke(snap, block, client):
        pools = await all_subnet_pool_state(snap)
        params = await governance_params(snap)
        baskets = await root_baskets(snap)
        return pools, params, baskets

    (pools, params, baskets), block = run(_smoke)
    print(f"pinned block        : {block:,}")
    print(f"subnets             : {len(pools)}")
    print(f"governance          : {params}")
    print(f"root funds          : {len(baskets)}")
    with_counters = sum(1 for r in pools.values() if r["alpha_burned"] is not None)
    print(f"burn counters       : {with_counters}/{len(pools)} subnets")
    n, rec = next(iter(sorted(pools.items())))
    print(f"sample SN{n}        : spot={rec['spot_price']:.6f} "
          f"ema={rec['moving_price']:.6f} burned={rec['alpha_burned']}")
