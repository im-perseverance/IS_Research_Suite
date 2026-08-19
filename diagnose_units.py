"""
diagnose_units.py — one-shot v11 metagraph unit probe
=====================================================
IS Research Suite v3 — @im_perseverance

The 2026-08-18 live run printed validator APYs up to 19,590,654%, meaning
the v11 neuron `emission` field is not in the unit the APY math assumes
(alpha per block). This probe pins the unit EMPIRICALLY, using a ground
truth the chain itself provides:

    sum(neuron.emission over a subnet) ≈ distributed share of
    alpha_out_emission (α/block) — i.e. 0.41 (validators) + up to 0.41
    (miners, if paid) + possibly 0.18 (owner) of the per-block alpha_out
    emission, which we already read correctly from pool storage.

So the ratio  sum(neuron.emission) / alpha_out_emission  reveals the unit:
    ~0.4–1.0        -> alpha per block (no conversion needed)
    ~4e8–1e9        -> rao per block            (divide by 1e9)
    ~3e3–7e3 x0.8   -> alpha per day/tempo      (divide by 7200 / 360)
    ~3e12–7e12      -> rao per day              (divide by 7200e9)

Also prints raw types + sample values for stake, dividends, incentive,
trust fields so every adapter assumption gets checked in one run.

Usage: python diagnose_units.py [netuid ...]   (default: 64 9 19)
"""

import sys
import chain_analysis as chain

PROBE_DEFAULT = [64, 9, 19]


def describe(label, seq, n=3):
    vals = list(seq)[:n]
    types = {type(v).__name__ for v in seq}
    print(f"    {label:<18} types={sorted(types)} sample={vals}")


async def probe(snap, block, client):
    netuids = [int(a) for a in sys.argv[1:]] or PROBE_DEFAULT
    pools = await chain.all_subnet_pool_state(snap)
    print(f"  pinned block {block:,}\n")
    for n in netuids:
        pool = pools.get(n)
        if pool is None:
            print(f"  SN{n}: not found\n"); continue
        aoe = pool["alpha_out_emission"]
        print(f"  ── SN{n} ─ alpha_out_emission = {aoe:.9f} α/block (pool storage, known-good)")
        graph = await chain.metagraph(snap, n)
        if graph is None:
            print("    metagraph: None\n"); continue

        # Normalize to per-neuron records regardless of shape
        if isinstance(graph, dict):
            neurons = None
            print(f"    shape: dict, keys={sorted(graph.keys())[:12]}...")
            emissions = graph.get("emission") or graph.get("emissions") or []
            stakes = graph.get("total_stake") or graph.get("stake") or []
            divs = graph.get("dividends") or []
            incs = graph.get("incentives") or graph.get("incentive") or []
            vtr = graph.get("validator_trust") or []
        else:
            neurons = getattr(graph, "neurons", None) or []
            print(f"    shape: {type(graph).__name__} with {len(neurons)} neurons; "
                  f"neuron attrs={sorted(a for a in dir(neurons[0]) if not a.startswith('_'))[:18]}")
            emissions = [getattr(x, "emission", None) for x in neurons]
            stakes = [getattr(x, "total_stake", getattr(x, "stake", None)) for x in neurons]
            divs = [getattr(x, "dividends", None) for x in neurons]
            incs = [getattr(x, "incentive", None) for x in neurons]
            vtr = [getattr(x, "validator_trust", None) for x in neurons]

        describe("emission", emissions)
        describe("stake", stakes)
        describe("dividends", divs)
        describe("incentive", incs)
        describe("validator_trust", vtr)

        em_sum_raw = sum(float(v) if isinstance(v, (int, float)) else chain.f(v, 0.0)
                         for v in emissions if v is not None)
        em_sum_f   = sum(chain.f(v, 0.0) for v in emissions if v is not None)
        stake_sum  = sum(chain.f(v, 0.0) for v in stakes if v is not None)
        inc_sum    = sum(chain.f(v, 0.0) for v in incs if v is not None)
        print(f"    sum(emission) raw={em_sum_raw:.6g}  via chain.f={em_sum_f:.6g}")
        print(f"    sum(stake) via chain.f = {stake_sum:,.1f}  (α — compare taostats)")
        print(f"    sum(incentive)         = {inc_sum:.4f}  (~1.0 if miners paid, u16-scaled if ~65535)")
        if aoe and aoe > 0:
            for name, s in (("raw", em_sum_raw), ("chain.f", em_sum_f)):
                ratio = s / aoe
                if 0.3 <= ratio <= 1.2:        verdict = "α/block — NO conversion"
                elif 3e8 <= ratio <= 1.3e9:    verdict = "rao/block — divide by 1e9"
                elif 2.4e3 <= ratio <= 8e3:    verdict = "α/day or α/tempo — divide by 7200 or 360"
                elif 2e12 <= ratio <= 9e12:    verdict = "rao/day — divide by 7.2e12"
                elif 100 <= ratio <= 500:      verdict = "α/tempo — divide by tempo (360)"
                else:                           verdict = "UNRECOGNISED — paste this output back"
                print(f"    ratio sum(emission {name})/alpha_out_emission = {ratio:,.4g}  ->  {verdict}")
        print()


if __name__ == "__main__":
    chain.run(probe)
