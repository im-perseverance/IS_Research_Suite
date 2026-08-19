"""Offline end-to-end test for subnet_analysis.py v3 (pure layers, no chain)."""
import json, tempfile, io, contextlib
from pathlib import Path
from datetime import datetime, timezone, timedelta
import subnet_analysis as sa

now = datetime.now(timezone.utc)
today = now.strftime("%Y-%m-%d")
yday  = (now - timedelta(days=1)).strftime("%Y-%m-%d")
BLOCK = 9_000_000
OLD_REG = BLOCK - sa.ONE_YEAR_BLOCKS - 1000   # gate-eligible
NEW_REG = BLOCK - 100_000                     # immune, not gate-eligible

def pool(n, **kw):
    base = dict(netuid=n, tao_reserves=5000.0, alpha_in_pool=5000.0,
                alpha_outstanding=10000.0, protocol_alpha=100.0,
                tao_in_emission=0.005, alpha_in_emission=0.002,
                alpha_out_emission=0.01, volume=1000.0, moving_price=0.5,
                registered_at=OLD_REG, first_emission_block=1, ema_halving_blocks=216000,
                alpha_burned=1000.0, alpha_recycled=50.0,
                total_alpha_issuance=15000.0, spot_price=0.5)
    base.update(kw); return base

def conv(owner_locked, leader_is_owner, leader_pct, total_locked):
    entries = []
    owner = None
    if owner_locked:
        owner = {"hotkey": "5Own", "is_owner": True, "locked_alpha": owner_locked,
                 "conviction_alpha": owner_locked, "pct_of_threshold": 0.2,
                 "blocks_to_threshold": None}
        entries.append(owner)
    leader = {"hotkey": "5Own" if leader_is_owner else "5Foe",
              "is_owner": leader_is_owner,
              "locked_alpha": total_locked, "conviction_alpha": total_locked + 1,
              "pct_of_threshold": leader_pct, "blocks_to_threshold": 14400}
    entries.insert(0, leader) if not leader_is_owner else None
    ec = sorted(entries + ([leader] if leader_is_owner and not owner else []),
                key=lambda r: -(r["conviction_alpha"] or 0))
    return {"netuid": 0, "eligible_alpha": 9000.0, "threshold_alpha": 1620.0,
            "total_locked_alpha": total_locked, "total_conviction_alpha": total_locked,
            "alpha_burned": 1000.0, "protocol_alpha": 100.0,
            "owner": owner, "leader": ec[0], "entries": ec}

def mg():
    return {"hotkeys": ["hkA", "hkB"], "coldkeys": ["ckOwner", "ckB"],
            "total_stake": [5000.0, 2000.0], "dividends": [40000, 25535],
            "emission": [0.004, 0.002], "validator_permit": [True, True],
            "incentives": [0.5, 0.5], "validator_trust": [60000, 30000]}

def prev_entry(alpha_out, alpha_in, burned_cum, recycled_cum):
    return {"date": yday, "block": BLOCK - 7200, "spot_price": 0.48,
            "moving_price": 0.5, "alpha_outstanding": alpha_out,
            "alpha_in_pool": alpha_in, "alpha_out_emission": 0.01,
            "alpha_in_emission": 0.002, "tao_reserves": 4900.0,
            "emission_apy": 0.05, "ema_tao_inflow": 1.5, "owner_stake": 4000.0,
            "alpha_burned_cum": burned_cum, "alpha_recycled_cum": recycled_cum,
            "pool_growth_real": 1.0}

# SN10 FORTRESS: counters engage, recon OK, owner locked, leader=owner
# est raw burn 95 vs counter 100+0 → rel -5% OK
p10 = pool(10, alpha_outstanding=9991.4, alpha_burned=1000.0, alpha_recycled=50.0)
t10 = prev_entry(10000.0, 5000.0, 900.0, 50.0)
c10 = conv(owner_locked=300.0, leader_is_owner=True, leader_pct=0.19, total_locked=800.0)

# SN20 EXPOSED + TAKEOVER_IMMINENT + recon DIVERGENT
# counter burned 300; estimator ~90 → rel ≈ -70% DIVERGENT
p20 = pool(20, alpha_outstanding=9996.4, alpha_burned=800.0, alpha_recycled=0.0,
           alpha_in_pool=5000.0)
t20 = prev_entry(10000.0, 5000.0, 500.0, 0.0)
c20 = conv(owner_locked=0.0, leader_is_owner=False, leader_pct=0.85, total_locked=600.0)

# SN30 UNDEFENDED_BURNER: burning, no owner lock, leader is owner (low pct)
p30 = pool(30, alpha_outstanding=9991.4, alpha_burned=1200.0, alpha_recycled=10.0)
t30 = prev_entry(10000.0, 5000.0, 1100.0, 10.0)
c30 = conv(owner_locked=0.0, leader_is_owner=True, leader_pct=0.10, total_locked=50.0)

# SN40 first run: no trajectory, young subnet
p40 = pool(40, registered_at=NEW_REG)
c40 = conv(owner_locked=100.0, leader_is_owner=True, leader_pct=0.05, total_locked=100.0)

data = {
    "block": BLOCK,
    "pools": {10: p10, 20: p20, 30: p30, 40: p40},
    "params": {"tao_weight": 0.18, "owner_cut": 0.18,
               "maturity_rate_blocks": 311622, "unlock_rate_blocks": 934866,
               "root_claim_threshold": 0.0005},
    "names": {10: "fortress", 20: "exposed", 30: "siege", 40: "newborn"},
    "owners": {10: "ckOwner", 20: "ckOwner", 30: "ckOwner", 40: "ckOwner"},
    "ema_inflow": {10: 2.0, 20: 1.0, 30: 0.5, 40: 0.1},
    "basket_exposure": {10: 500.0},
    "take_map": {"hkA": 0.09, "hkB": 0.18},
    "convictions": {10: c10, 20: c20, 30: c30, 40: c40},
    "metagraphs": {10: mg(), 20: mg(), 30: mg(), 40: mg()},
}
traj_90d = {"10": [t10], "20": [t20], "30": [t30]}

results, traj_entries = sa.analyse(data, traj_90d, now)
R = {r["netuid"]: r for r in results}

# burn source + counters
assert R[10]["burned_source"] == "COUNTER" and abs(R[10]["burned_tokens"] - 100.0) < 1e-6
assert abs(R[10]["recycled_tokens"] - 0.0) < 1e-6
assert R[40]["burned_source"] is None and R[40]["burned_tokens"] is None
# reconciliation
assert R[10]["recon_flag"] == "OK", R[10]["recon_flag"]
assert R[20]["recon_flag"] == "DIVERGENT", R[20]["recon_flag"]
# commitment matrix
assert R[10]["commitment_class"] == "FORTRESS", R[10]["commitment_class"]
assert R[20]["commitment_class"] == "EXPOSED", R[20]["commitment_class"]
assert R[10]["owner_lock_ratio"] > 0.005 and R[20]["owner_lock_ratio"] == 0.0
assert R[10]["burn_coverage"] > sa.BURN_COVERAGE_ACTIVE
# governance
assert R[20]["governance_flag"] == "TAKEOVER_IMMINENT", R[20]["governance_flag"]
assert R[30]["governance_flag"] == "UNDEFENDED_BURNER", R[30]["governance_flag"]
assert R[10]["governance_flag"] is None
assert R[40]["gate_eligible"] is False and R[40]["immune"] is True
# basket + live EMA basis
assert abs(R[10]["basket_share"] - 500.0/9991.4) < 1e-9
assert abs(R[10]["ema_halving_days"] - 30.0) < 1e-9
# best validator: emission is alpha/TEMPO (diagnose_units 2026-08-19);
# hkB still wins on per-stake emission after tempo conversion + 18% take
raw_b = (0.002/360)/2000*sa.BLOCKS_PER_YEAR
assert abs(R[10]["best_validator_apy"] - raw_b*0.82) < 1e-12
assert R[10]["best_validator_take"] == 0.18
assert R[10]["best_validator_hotkey"] == "hkB..."
# COUNTER-source decomposition: incentives sum 1.0 -> no mechanical
# deduction -> manual burn equals the counter delta (100a on SN10)
assert abs(R[10]["manual_burn"] - 100.0) < 1e-9
assert abs(R[10]["burn_coverage"] - 100.0/(0.01*0.18*7200)) < 1e-9
# trajectory entries carry v3 fields
assert traj_entries["10"]["alpha_burned_cum"] == 1000.0
assert traj_entries["20"]["commitment_class"] == "EXPOSED"
print("analyse() assertions OK")

eco = sa.ecosystem_stats(results)
assert eco["counter_coverage"] == 3 and eco["recon_divergent"] == 1
assert eco["matrix_counts"]["FORTRESS"] == 1
assert eco["governance_flags"]["TAKEOVER_IMMINENT"] == 1
assert eco["governance_flags"]["UNDEFENDED_BURNER"] == 1
print("ecosystem_stats OK")

# report renders without error
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    sa.print_report(results, eco)
out = buf.getvalue()
assert "COMMITMENT MATRIX" in out and "GOVERNANCE EXPOSURE" in out
assert "BURN RECONCILIATION DIVERGENCE" in out and "TAKEOVER_IMMINENT" in out
print("print_report OK")

# persistence round-trip in tmpdir
tmp = Path(tempfile.mkdtemp())
sa.OUTPUT_DIR = tmp / "subnet_analysis"
sa.OUTPUT_METADATA_DIR = sa.OUTPUT_DIR / "metadata"
sa.OUTPUT_SNAPSHOT_DIR = sa.OUTPUT_DIR / "snapshots"
sa.OUTPUT_DIR.mkdir(parents=True)
sa.save_json(sa.OUTPUT_DIR / "trajectory_90d.json", traj_90d)
with contextlib.redirect_stdout(io.StringIO()):
    csv_path = sa.persist(results, traj_entries, eco, now, BLOCK)
import csv as _csv
rows = list(_csv.DictReader(open(csv_path)))
assert len(rows) == 4
assert rows[0].keys().__contains__("commitment_class")
byid = {r["netuid"]: r for r in rows}
assert byid["10"]["commitment_class"] == "FORTRESS"
assert byid["20"]["governance_flag"] == "TAKEOVER_IMMINENT"
assert byid["10"]["burned_source"] == "COUNTER"
t90 = json.load(open(sa.OUTPUT_DIR / "trajectory_90d.json"))
assert t90["10"][-1]["date"] == today and t90["10"][-1]["alpha_burned_cum"] == 1000.0
meta = json.load(open(sa.OUTPUT_METADATA_DIR / f"staking_metadata_{today}.json"))
assert meta["ecosystem_conviction"]["commitment_matrix"]["FORTRESS"] == 1
assert meta["burn_accounting"]["reconciliation_divergent"] == 1
eco_t = json.load(open(sa.OUTPUT_DIR / "trajectory_ecosystem.json"))
assert eco_t[-1]["counter_coverage"] == 3
print("persist round-trip OK")
print("ALL SUBNET v3 TESTS PASSED")
