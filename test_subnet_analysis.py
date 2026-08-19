"""Offline structural test for chain_analysis.py with a stubbed snapshot."""
import asyncio, sys, types, json, tempfile, os
import chain_analysis as ca

# ── unit helpers ──
class Bal:
    def __init__(self, rao): self.rao = rao
    @property
    def tao(self): return self.rao / 1e9
assert ca.f(Bal(1_500_000_000)) == 1.5
assert ca.f(2.5) == 2.5 and ca.f(None, None) is None
assert ca.from_rao(1_000_000_000) == 1.0
assert abs(ca.fixed_to_float({'bits': 3 << 32}) - 3.0) < 1e-9
assert abs(ca.fixed_to_float(0.0125) - 0.0125) < 1e-12
assert abs(ca.u16_frac(11796) - 0.18) < 0.001
assert abs(ca.u64_weight(int(0.18 * (2**64 - 1))) - 0.18) < 1e-6
assert ca.u64_weight(0.18) == 0.18
print("units OK")

# ── stub bittensor module so storage getattr resolves ──
bt = types.ModuleType("bittensor")
class _Item:
    def __init__(s, p, i): s.p, s.i = p, i
class _Pallet:
    def __init__(s, name): s._n = name
    def __getattr__(s, item): return _Item(s._n, item)
class _Storage:
    def __getattr__(s, p): return _Pallet(p)
bt.storage = _Storage()
sys.modules["bittensor"] = bt

# ── stub snapshot ──
NETS = [1, 64]
RAO = 10**9
class Snap:
    async def query(self, item, params):
        return {"TaoWeight": int(0.18*(2**64-1)), "SubnetOwnerCut": 11796,
                "MaturityRate": 311622, "UnlockRate": 934866}.get(item.i)
    async def query_map(self, item, params):
        if item.i == "NetworksAdded": return [((0,), True)] + [((n,), True) for n in NETS]
        if item.i == "SubnetMovingPrice": return [((n,), {"bits": (1*n) << 32}) for n in NETS]
        if item.i in ("NetworkRegisteredAt",): return [((n,), 1000+n) for n in NETS]
        if item.i in ("FirstEmissionBlockNumber","EMAPriceHalvingBlocks"): return [((n,), 7200) for n in NETS]
        return [((n,), n * RAO) for n in NETS]   # everything else: n tokens in rao
    async def read(self, name, **kw):
        if name == "alpha_prices": return {1: 0.5, 64: 0.9}
        if name == "root_claim_threshold": return Bal(500_000)
        if name == "root_baskets":
            return [{"hotkey":"5F...","nav_tao":Bal(90*RAO),"spot_nav_tao":Bal(100*RAO),
                     "deposited_tao":Bal(80*RAO),"redeemed_tao":Bal(10*RAO),
                     "lifetime_return":1.25,"weights":[],
                     "holdings":[{"netuid":64,"alpha":Bal(5*RAO),"spot_tao":Bal(4*RAO),"realizable_tao":Bal(3*RAO)}]}]
        if name == "subnet_convictions":
            # real v11 read shape: per-hotkey records under "hotkeys",
            # canonical owner via top-level "owner_hotkey" (locks.py)
            return {"eligible_alpha":Bal(1000*RAO),"threshold_alpha":Bal(180*RAO),
                    "total_locked_alpha":Bal(300*RAO),"total_conviction_alpha":Bal(250*RAO),
                    "alpha_burned":Bal(50*RAO),"protocol_alpha":Bal(20*RAO),
                    "owner_hotkey":"5Own","ownership_changeable_at_block":777,
                    "hotkeys":[{"hotkey":"5Own","is_owner":False,"locked_alpha":Bal(200*RAO),
                                "conviction_alpha":Bal(200*RAO),"pct_of_threshold":1.11,"blocks_to_threshold":0},
                               {"hotkey":"5Gen","is_owner":False,"locked_alpha":Bal(100*RAO),
                                "conviction_alpha":Bal(50*RAO),"pct_of_threshold":0.28,"blocks_to_threshold":None}]}
        return None

async def main():
    s = Snap()
    pools = await ca.all_subnet_pool_state(s)
    assert set(pools) == {1, 64}, pools.keys()
    assert pools[64]["alpha_burned"] == 64.0 and pools[1]["spot_price"] == 0.5
    assert pools[64]["moving_price"] == 64.0 and pools[1]["registered_at"] == 1001
    params = await ca.governance_params(s)
    assert abs(params["tao_weight"]-0.18) < 1e-6 and params["unlock_rate_blocks"] == 934866
    assert params["root_claim_threshold"] == 0.0005
    b = await ca.root_baskets(s)
    assert abs(b[0]["nav_haircut"] - 0.10) < 1e-9
    exp = await ca.basket_subnet_exposure(s, b)
    assert exp == {64: 5.0}
    c = await ca.subnet_convictions(s, 64)
    assert c["owner"]["hotkey"] == "5Own" and c["leader"]["is_owner"] is True
    assert c["total_locked_alpha"] == 300.0
    assert c["owner_hotkey"] == "5Own"          # crosscheck path fixed is_owner
    assert c["owner"]["locked_alpha"] == 200.0  # per-hotkey records survive
    print("pool/params/baskets/convictions OK")

asyncio.run(main())

# ── reconciliation ──
prev = {"alpha_burned": 100.0, "alpha_recycled": 10.0}
cur  = {"alpha_burned": 130.0, "alpha_recycled": 15.0, "alpha_out_emission": 0.1, "_days_gap": 1.0}
r = ca.burn_reconciliation(prev, cur, estimator_burned=34.0)
assert r["counter_destroyed"] == 35.0 and r["flag"] == "OK", r
r2 = ca.burn_reconciliation(prev, cur, estimator_burned=12.0)
assert r2["flag"] == "DIVERGENT", r2
assert ca.burn_reconciliation({}, cur, 1.0) is None
print("reconciliation OK")

# ── trajectory I/O ──
d = tempfile.mkdtemp()
p = os.path.join(d, "traj.json")
t = ca.upsert_by_date([], {"date": "2026-08-17", "x": 1})
t = ca.upsert_by_date(t, {"date": "2026-08-16", "x": 0})
t = ca.upsert_by_date(t, {"date": "2026-08-17", "x": 2})
assert [e["x"] for e in t] == [0, 2]
ca.save_json(p, t)
assert ca.load_json(p) == t
print("trajectory OK")
print("ALL STRUCTURAL TESTS PASSED")

# ── feedback-round regression cases ──
assert ca.u16_frac(0.18) == 0.18                      # pre-normalized pass-through
assert abs(ca.u16_frac(11796) - 0.18) < 0.001         # raw u16 still decodes
# counter decrease: clamped math + raw deltas surfaced via warning path
import logging as _lg
rec = ca.burn_reconciliation(
    {"alpha_burned": 100.0, "alpha_recycled": 10.0},
    {"alpha_burned": 90.0, "alpha_recycled": 15.0, "alpha_out_emission": 0.1,
     "_days_gap": 1.0, "netuid": 7},
    estimator_burned=5.0)
assert rec["delta_burned"] == 0.0 and rec["delta_recycled"] == 5.0
assert rec["raw_delta_burned"] == -10.0 and rec["raw_delta_recycled"] == 5.0
# alpha_prices list shapes
async def _shapes():
    class S(Snap):
        async def read(self, name, **kw):
            if name == "alpha_prices":
                return [{"netuid": 1, "tao_per_alpha": 0.5}, (64, 0.9)]
            return await Snap.read(self, name, **kw)
    pools = await ca.all_subnet_pool_state(S())
    assert pools[1]["spot_price"] == 0.5 and pools[64]["spot_price"] == 0.9
asyncio.run(_shapes())
print("FEEDBACK-ROUND CASES PASSED")
