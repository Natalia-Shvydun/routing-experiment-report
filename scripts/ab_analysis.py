#!/usr/bin/env python3
"""
A/B Experiment Analysis: Payment Orchestration Routing
Control (Primer) vs Test (In-house Statsig)
Segment: DE, eu5-jpm-currency, pay_now, CIT
"""

import json
import math
from collections import defaultdict

DATA_PATH = "/Users/natalia.shvydun@getyourguide.com/Desktop/Work/Cursor/public/data.json"

def z_test_proportions(s1, n1, s2, n2):
    """Two-proportion z-test. Returns (z_stat, p_value_two_sided)."""
    if n1 == 0 or n2 == 0:
        return (0.0, 1.0)
    p1 = s1 / n1
    p2 = s2 / n2
    p_pool = (s1 + s2) / (n1 + n2)
    if p_pool == 0 or p_pool == 1:
        return (0.0, 1.0)
    se = math.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))
    if se == 0:
        return (0.0, 1.0)
    z = (p1 - p2) / se
    p_value = 2 * (1 - normal_cdf(abs(z)))
    return (z, p_value)

def normal_cdf(x):
    """Approximation of the standard normal CDF."""
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))

def pct(num, den):
    return 100 * num / den if den > 0 else 0.0

def pp_diff(r1, r2):
    return r1 - r2

def load_and_filter():
    with open(DATA_PATH) as f:
        raw = json.load(f)

    data = raw["data"]
    filtered = [
        r for r in data
        if r["payment_flow"] == "pay_now"
        and r["bin_issuer_country_code"] == "DE"
        and r["routing_rule"] == "eu5-jpm-currency"
        and r["payment_initiator_type"] == "CIT"
    ]
    return filtered, raw.get("samples", [])

def agg(rows, keys=None):
    """Aggregate attempts/successful/sent_to_issuer/three_ds_passed, optionally by keys."""
    if keys is None:
        totals = {"attempts": 0, "successful": 0, "sent_to_issuer": 0, "three_ds_passed": 0}
        for r in rows:
            totals["attempts"] += r["attempts"]
            totals["successful"] += r["successful"]
            totals["sent_to_issuer"] += r["sent_to_issuer"]
            totals["three_ds_passed"] += r["three_ds_passed"]
        return totals

    buckets = defaultdict(lambda: {"attempts": 0, "successful": 0, "sent_to_issuer": 0, "three_ds_passed": 0})
    for r in rows:
        if isinstance(keys, str):
            k = r[keys]
        else:
            k = tuple(r[kk] for kk in keys)
        buckets[k]["attempts"] += r["attempts"]
        buckets[k]["successful"] += r["successful"]
        buckets[k]["sent_to_issuer"] += r["sent_to_issuer"]
        buckets[k]["three_ds_passed"] += r["three_ds_passed"]
    return dict(buckets)


# ── MAIN ──────────────────────────────────────────────
rows, samples = load_and_filter()
ctrl = [r for r in rows if r["group_name"] == "Control"]
test = [r for r in rows if r["group_name"] == "Test"]

ctrl_tot = agg(ctrl)
test_tot = agg(test)

print("=" * 90)
print("SECTION 0: OVERALL SUMMARY")
print("=" * 90)
for label, t in [("Control", ctrl_tot), ("Test", test_tot)]:
    sr = pct(t["successful"], t["attempts"])
    print(f"  {label}: {t['attempts']:,} attempts, {t['successful']:,} successful, SR={sr:.2f}%")

overall_z, overall_p = z_test_proportions(
    ctrl_tot["successful"], ctrl_tot["attempts"],
    test_tot["successful"], test_tot["attempts"],
)
print(f"  Δ SR = {pct(ctrl_tot['successful'], ctrl_tot['attempts']) - pct(test_tot['successful'], test_tot['attempts']):+.2f}pp")
print(f"  z={overall_z:.4f}, p={overall_p:.6f}")

# ═══════════════════════════════════════════════════════
# SECTION 1: Oaxaca-Blinder Decomposition
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 1: OAXACA-BLINDER STYLE SR GAP DECOMPOSITION")
print("=" * 90)

# Decompose by processor × card_network
dim = ["payment_processor", "payment_method_variant"]
ctrl_by = agg(ctrl, dim)
test_by = agg(test, dim)

all_keys = set(ctrl_by.keys()) | set(test_by.keys())
ctrl_total_att = ctrl_tot["attempts"]
test_total_att = test_tot["attempts"]
ctrl_sr_overall = pct(ctrl_tot["successful"], ctrl_tot["attempts"])
test_sr_overall = pct(test_tot["successful"], test_tot["attempts"])

composition_effect = 0.0
within_effect = 0.0
interaction_effect = 0.0

print(f"\n{'Processor × Network':<35} {'Ctrl%':>7} {'Test%':>7} {'CtrlSR':>8} {'TestSR':>8} {'Δshare':>8} {'ΔSR':>8} {'Comp.':>8} {'Within':>8}")
print("-" * 120)

decomp_rows = []
for k in sorted(all_keys):
    c = ctrl_by.get(k, {"attempts": 0, "successful": 0})
    t = test_by.get(k, {"attempts": 0, "successful": 0})
    c_share = c["attempts"] / ctrl_total_att if ctrl_total_att else 0
    t_share = t["attempts"] / test_total_att if test_total_att else 0
    c_sr = pct(c["successful"], c["attempts"])
    t_sr = pct(t["successful"], t["attempts"])
    d_share = t_share - c_share
    d_sr = t_sr - c_sr

    comp = d_share * c_sr  # composition (routing) effect
    within = c_share * d_sr  # within-cell (performance) effect
    inter = d_share * d_sr  # interaction

    composition_effect += comp
    within_effect += within
    interaction_effect += inter

    label = f"{k[0]} × {k[1]}"
    decomp_rows.append((label, c_share, t_share, c_sr, t_sr, d_share, d_sr, comp, within))
    print(f"  {label:<33} {c_share*100:>6.2f}% {t_share*100:>6.2f}% {c_sr:>7.2f}% {t_sr:>7.2f}% {d_share*100:>+7.2f}pp {d_sr:>+7.2f}pp {comp:>+7.3f} {within:>+7.3f}")

print("-" * 120)
total_gap = test_sr_overall - ctrl_sr_overall
print(f"\n  Total SR gap (Test − Control): {total_gap:+.4f}pp")
print(f"  Composition (routing) effect:  {composition_effect:+.4f}pp")
print(f"  Within-cell (perf.) effect:    {within_effect:+.4f}pp")
print(f"  Interaction effect:            {interaction_effect:+.4f}pp")
print(f"  Sum of components:             {composition_effect + within_effect + interaction_effect:+.4f}pp")

# ═══════════════════════════════════════════════════════
# Decomposition by fraud_pre_auth_result
# ═══════════════════════════════════════════════════════
print("\n--- Decomposition by fraud_pre_auth_result ---")
ctrl_fraud = agg(ctrl, "fraud_pre_auth_result")
test_fraud = agg(test, "fraud_pre_auth_result")
all_fraud = sorted(set(ctrl_fraud.keys()) | set(test_fraud.keys()))

comp2 = 0.0
within2 = 0.0
inter2 = 0.0

print(f"\n{'Fraud Result':<30} {'Ctrl%':>7} {'Test%':>7} {'CtrlSR':>8} {'TestSR':>8} {'Comp.':>8} {'Within':>8}")
print("-" * 100)
for k in all_fraud:
    c = ctrl_fraud.get(k, {"attempts": 0, "successful": 0})
    t = test_fraud.get(k, {"attempts": 0, "successful": 0})
    c_share = c["attempts"] / ctrl_total_att if ctrl_total_att else 0
    t_share = t["attempts"] / test_total_att if test_total_att else 0
    c_sr = pct(c["successful"], c["attempts"])
    t_sr = pct(t["successful"], t["attempts"])
    d_share = t_share - c_share
    d_sr = t_sr - c_sr
    c_comp = d_share * c_sr
    c_within = c_share * d_sr
    c_inter = d_share * d_sr
    comp2 += c_comp
    within2 += c_within
    inter2 += c_inter
    print(f"  {k:<28} {c_share*100:>6.2f}% {t_share*100:>6.2f}% {c_sr:>7.2f}% {t_sr:>7.2f}% {c_comp:>+7.3f} {c_within:>+7.3f}")

print(f"\n  Composition (3DS routing) effect: {comp2:+.4f}pp")
print(f"  Within-cell effect:               {within2:+.4f}pp")
print(f"  Interaction:                      {inter2:+.4f}pp")

# ═══════════════════════════════════════════════════════
# SECTION 2: TIME SERIES
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 2: TIME SERIES ANALYSIS")
print("=" * 90)

ctrl_ts = agg(ctrl, "payment_attempt_date")
test_ts = agg(test, "payment_attempt_date")
all_dates = sorted(set(ctrl_ts.keys()) | set(test_ts.keys()))

print(f"\n{'Date':<14} {'Ctrl Att':>9} {'Ctrl SR':>8} {'Test Att':>9} {'Test SR':>8} {'Δ SR':>8} {'z':>8} {'p':>8}")
print("-" * 85)

gaps = []
for d in all_dates:
    c = ctrl_ts.get(d, {"attempts": 0, "successful": 0})
    t = test_ts.get(d, {"attempts": 0, "successful": 0})
    c_sr = pct(c["successful"], c["attempts"])
    t_sr = pct(t["successful"], t["attempts"])
    gap = c_sr - t_sr
    z, p = z_test_proportions(c["successful"], c["attempts"], t["successful"], t["attempts"])
    gaps.append(gap)
    print(f"  {d:<12} {c['attempts']:>9,} {c_sr:>7.2f}% {t['attempts']:>9,} {t_sr:>7.2f}% {gap:>+7.2f}pp {z:>7.3f}  {p:>7.4f}")

if len(gaps) > 2:
    mean_gap = sum(gaps) / len(gaps)
    std_gap = math.sqrt(sum((g - mean_gap)**2 for g in gaps) / (len(gaps) - 1))
    first_half = gaps[:len(gaps)//2]
    second_half = gaps[len(gaps)//2:]
    first_avg = sum(first_half) / len(first_half)
    second_avg = sum(second_half) / len(second_half)
    print(f"\n  Mean daily gap: {mean_gap:+.2f}pp (std={std_gap:.2f}pp)")
    print(f"  First half avg gap: {first_avg:+.2f}pp")
    print(f"  Second half avg gap: {second_avg:+.2f}pp")
    print(f"  Trend direction: {'WIDENING' if second_avg > first_avg else 'NARROWING' if second_avg < first_avg else 'STABLE'}")

# ═══════════════════════════════════════════════════════
# SECTION 3: CARD NETWORK DEEP DIVE
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 3: CARD NETWORK DEEP DIVE")
print("=" * 90)

ctrl_net = agg(ctrl, "payment_method_variant")
test_net = agg(test, "payment_method_variant")

print(f"\n{'Network':<15} {'Ctrl Att':>9} {'Ctrl SR':>8} {'Test Att':>9} {'Test SR':>8} {'Δ SR':>8} {'Ctrl%':>7} {'Test%':>7} {'z':>7} {'p':>8}")
print("-" * 100)
for net in sorted(set(ctrl_net.keys()) | set(test_net.keys())):
    c = ctrl_net.get(net, {"attempts": 0, "successful": 0})
    t = test_net.get(net, {"attempts": 0, "successful": 0})
    c_sr = pct(c["successful"], c["attempts"])
    t_sr = pct(t["successful"], t["attempts"])
    gap = c_sr - t_sr
    z, p = z_test_proportions(c["successful"], c["attempts"], t["successful"], t["attempts"])
    print(f"  {net:<13} {c['attempts']:>9,} {c_sr:>7.2f}% {t['attempts']:>9,} {t_sr:>7.2f}% {gap:>+7.2f}pp {pct(c['attempts'], ctrl_total_att):>6.1f}% {pct(t['attempts'], test_total_att):>6.1f}% {z:>6.3f}  {p:>7.4f}")

# By network × processor
print("\n--- By Network × Processor ---")
ctrl_np = agg(ctrl, ["payment_method_variant", "payment_processor"])
test_np = agg(test, ["payment_method_variant", "payment_processor"])
all_np = sorted(set(ctrl_np.keys()) | set(test_np.keys()))

print(f"\n{'Network × Processor':<35} {'Ctrl Att':>9} {'Ctrl SR':>8} {'Test Att':>9} {'Test SR':>8} {'Δ SR':>8} {'z':>7} {'p':>8}")
print("-" * 100)
for k in all_np:
    c = ctrl_np.get(k, {"attempts": 0, "successful": 0})
    t = test_np.get(k, {"attempts": 0, "successful": 0})
    c_sr = pct(c["successful"], c["attempts"])
    t_sr = pct(t["successful"], t["attempts"])
    gap = c_sr - t_sr
    z, p = z_test_proportions(c["successful"], c["attempts"], t["successful"], t["attempts"])
    label = f"{k[0]} × {k[1]}"
    print(f"  {label:<33} {c['attempts']:>9,} {c_sr:>7.2f}% {t['attempts']:>9,} {t_sr:>7.2f}% {gap:>+7.2f}pp {z:>6.3f}  {p:>7.4f}")

# ═══════════════════════════════════════════════════════
# SECTION 4: PROCESSOR "none" INVESTIGATION
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 4: PROCESSOR 'NONE' INVESTIGATION")
print("=" * 90)

ctrl_proc = agg(ctrl, "payment_processor")
test_proc = agg(test, "payment_processor")

print(f"\n{'Processor':<20} {'Ctrl Att':>9} {'Ctrl SR':>8} {'Test Att':>9} {'Test SR':>8} {'Ctrl%':>7} {'Test%':>7}")
print("-" * 80)
for proc in sorted(set(ctrl_proc.keys()) | set(test_proc.keys())):
    c = ctrl_proc.get(proc, {"attempts": 0, "successful": 0})
    t = test_proc.get(proc, {"attempts": 0, "successful": 0})
    print(f"  {proc:<18} {c['attempts']:>9,} {pct(c['successful'], c['attempts']):>7.2f}% {t['attempts']:>9,} {pct(t['successful'], t['attempts']):>7.2f}% {pct(c['attempts'], ctrl_total_att):>6.1f}% {pct(t['attempts'], test_total_att):>6.1f}%")

# None processor breakdown
none_ctrl = [r for r in ctrl if r["payment_processor"] == "NONE"]
none_test = [r for r in test if r["payment_processor"] == "NONE"]

print("\n--- Processor=NONE by fraud_pre_auth_result ---")
none_ctrl_fraud = agg(none_ctrl, "fraud_pre_auth_result")
none_test_fraud = agg(none_test, "fraud_pre_auth_result")
for k in sorted(set(none_ctrl_fraud.keys()) | set(none_test_fraud.keys())):
    c = none_ctrl_fraud.get(k, {"attempts": 0, "successful": 0})
    t = none_test_fraud.get(k, {"attempts": 0, "successful": 0})
    print(f"  {k:<25} Ctrl: {c['attempts']:>6,} att, {c['successful']:>6,} succ | Test: {t['attempts']:>6,} att, {t['successful']:>6,} succ")

print("\n--- Processor=NONE by card network ---")
none_ctrl_net = agg(none_ctrl, "payment_method_variant")
none_test_net = agg(none_test, "payment_method_variant")
for k in sorted(set(none_ctrl_net.keys()) | set(none_test_net.keys())):
    c = none_ctrl_net.get(k, {"attempts": 0, "successful": 0})
    t = none_test_net.get(k, {"attempts": 0, "successful": 0})
    print(f"  {k:<15} Ctrl: {c['attempts']:>6,} | Test: {t['attempts']:>6,}")

print("\n--- Processor=NONE by challenge_issued ---")
none_ctrl_ch = agg(none_ctrl, "challenge_issued")
none_test_ch = agg(none_test, "challenge_issued")
for k in sorted(set(none_ctrl_ch.keys()) | set(none_test_ch.keys())):
    c = none_ctrl_ch.get(k, {"attempts": 0, "successful": 0})
    t = none_test_ch.get(k, {"attempts": 0, "successful": 0})
    print(f"  challenge_issued={k:<8} Ctrl: {c['attempts']:>6,} | Test: {t['attempts']:>6,}")

print("\n--- Processor=NONE time series ---")
none_ctrl_ts = agg(none_ctrl, "payment_attempt_date")
none_test_ts = agg(none_test, "payment_attempt_date")
for d in all_dates:
    c = none_ctrl_ts.get(d, {"attempts": 0})
    t = none_test_ts.get(d, {"attempts": 0})
    print(f"  {d}  Ctrl: {c['attempts']:>5,} | Test: {t['attempts']:>5,}")

# ═══════════════════════════════════════════════════════
# SECTION 5: FIRST ATTEMPT VS RETRY
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 5: FIRST ATTEMPT VS RETRY")
print("=" * 90)

ctrl_first = agg([r for r in ctrl if r["is_first_attempt"]])
ctrl_retry = agg([r for r in ctrl if not r["is_first_attempt"]])
test_first = agg([r for r in test if r["is_first_attempt"]])
test_retry = agg([r for r in test if not r["is_first_attempt"]])

for label, c, t in [("First attempt", ctrl_first, test_first), ("Retry", ctrl_retry, test_retry)]:
    c_sr = pct(c["successful"], c["attempts"])
    t_sr = pct(t["successful"], t["attempts"])
    z, p = z_test_proportions(c["successful"], c["attempts"], t["successful"], t["attempts"])
    print(f"  {label:<15} Ctrl: {c['attempts']:>8,} att, SR={c_sr:.2f}% | Test: {t['attempts']:>8,} att, SR={t_sr:.2f}% | Δ={c_sr - t_sr:+.2f}pp  z={z:.3f} p={p:.4f}")

# Retry rate
ctrl_retry_rate = pct(ctrl_retry["attempts"], ctrl_tot["attempts"])
test_retry_rate = pct(test_retry["attempts"], test_tot["attempts"])
print(f"\n  Retry rate: Ctrl={ctrl_retry_rate:.2f}% | Test={test_retry_rate:.2f}%")

# By system_attempt_rank
print("\n--- By system_attempt_rank ---")
ctrl_rank = agg(ctrl, "system_attempt_rank")
test_rank = agg(test, "system_attempt_rank")
for rank in sorted(set(ctrl_rank.keys()) | set(test_rank.keys())):
    c = ctrl_rank.get(rank, {"attempts": 0, "successful": 0})
    t = test_rank.get(rank, {"attempts": 0, "successful": 0})
    c_sr = pct(c["successful"], c["attempts"])
    t_sr = pct(t["successful"], t["attempts"])
    z, p = z_test_proportions(c["successful"], c["attempts"], t["successful"], t["attempts"])
    print(f"  Rank {rank}: Ctrl {c['attempts']:>7,} SR={c_sr:.2f}% | Test {t['attempts']:>7,} SR={t_sr:.2f}% | Δ={c_sr - t_sr:+.2f}pp z={z:.3f} p={p:.4f}")

# First attempt by network × processor
print("\n--- First attempt: by network × processor ---")
ctrl_first_rows = [r for r in ctrl if r["is_first_attempt"]]
test_first_rows = [r for r in test if r["is_first_attempt"]]
ctrl_first_np = agg(ctrl_first_rows, ["payment_method_variant", "payment_processor"])
test_first_np = agg(test_first_rows, ["payment_method_variant", "payment_processor"])
all_np_first = sorted(set(ctrl_first_np.keys()) | set(test_first_np.keys()))

for k in all_np_first:
    c = ctrl_first_np.get(k, {"attempts": 0, "successful": 0})
    t = test_first_np.get(k, {"attempts": 0, "successful": 0})
    c_sr = pct(c["successful"], c["attempts"])
    t_sr = pct(t["successful"], t["attempts"])
    gap = c_sr - t_sr
    z, p = z_test_proportions(c["successful"], c["attempts"], t["successful"], t["attempts"])
    label = f"{k[0]} × {k[1]}"
    if c["attempts"] >= 50 or t["attempts"] >= 50:
        print(f"  {label:<33} Ctrl: {c['attempts']:>7,} SR={c_sr:.2f}% | Test: {t['attempts']:>7,} SR={t_sr:.2f}% | Δ={gap:+.2f}pp z={z:.3f} p={p:.4f}")

# ═══════════════════════════════════════════════════════
# SECTION 6: COUNTERFACTUAL EXEMPTION ANALYSIS
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 6: COUNTERFACTUAL EXEMPTION ANALYSIS")
print("=" * 90)

# Get SR by fraud_pre_auth_result for test
test_fraud = agg(test, "fraud_pre_auth_result")
ctrl_fraud = agg(ctrl, "fraud_pre_auth_result")

print("\n--- Current fraud_pre_auth_result distribution ---")
print(f"{'Result':<30} {'Ctrl Att':>9} {'Ctrl%':>7} {'Ctrl SR':>8} {'Test Att':>9} {'Test%':>7} {'Test SR':>8}")
print("-" * 90)
for k in sorted(set(ctrl_fraud.keys()) | set(test_fraud.keys())):
    c = ctrl_fraud.get(k, {"attempts": 0, "successful": 0})
    t = test_fraud.get(k, {"attempts": 0, "successful": 0})
    print(f"  {k:<28} {c['attempts']:>9,} {pct(c['attempts'], ctrl_total_att):>6.1f}% {pct(c['successful'], c['attempts']):>7.2f}% {t['attempts']:>9,} {pct(t['attempts'], test_total_att):>6.1f}% {pct(t['successful'], t['attempts']):>7.2f}%")

# Counterfactual: what if test had control's fraud result shares?
cf_successful = 0
for k in sorted(set(ctrl_fraud.keys()) | set(test_fraud.keys())):
    c = ctrl_fraud.get(k, {"attempts": 0, "successful": 0})
    t = test_fraud.get(k, {"attempts": 0, "successful": 0})
    ctrl_share = c["attempts"] / ctrl_total_att if ctrl_total_att else 0
    test_sr_k = pct(t["successful"], t["attempts"]) / 100 if t["attempts"] > 0 else 0
    cf_successful += ctrl_share * test_total_att * test_sr_k

cf_sr = pct(cf_successful, test_total_att)
print(f"\n  Counterfactual: If test had control's fraud result SHARES but test's within-category SR:")
print(f"    Actual test SR:         {pct(test_tot['successful'], test_tot['attempts']):.4f}%")
print(f"    Counterfactual test SR: {cf_sr:.4f}%")
print(f"    Actual gap (Ctrl−Test): {pct(ctrl_tot['successful'], ctrl_tot['attempts']) - pct(test_tot['successful'], test_tot['attempts']):+.4f}pp")
print(f"    CF gap (Ctrl−CF_Test):  {pct(ctrl_tot['successful'], ctrl_tot['attempts']) - cf_sr:+.4f}pp")
print(f"    Gap EXPLAINED by exemption composition: {(cf_sr - pct(test_tot['successful'], test_tot['attempts'])):+.4f}pp")

# Counterfactual 2: what if test had control's within-fraud-category SR?
cf2_successful = 0
for k in sorted(set(ctrl_fraud.keys()) | set(test_fraud.keys())):
    c = ctrl_fraud.get(k, {"attempts": 0, "successful": 0})
    t = test_fraud.get(k, {"attempts": 0, "successful": 0})
    test_share = t["attempts"] / test_total_att if test_total_att else 0
    ctrl_sr_k = pct(c["successful"], c["attempts"]) / 100 if c["attempts"] > 0 else 0
    cf2_successful += test_share * test_total_att * ctrl_sr_k

cf2_sr = pct(cf2_successful, test_total_att)
print(f"\n  Counterfactual 2: If test had control's within-category SR but test's shares:")
print(f"    CF2 test SR:            {cf2_sr:.4f}%")
print(f"    Gap EXPLAINED by within-category SR diff: {(cf2_sr - pct(test_tot['successful'], test_tot['attempts'])):+.4f}pp")

# 3DS specific deep dive
print("\n--- 3DS specific analysis ---")
three_ds_ctrl = [r for r in ctrl if r["fraud_pre_auth_result"] == "THREE_DS"]
three_ds_test = [r for r in test if r["fraud_pre_auth_result"] == "THREE_DS"]

# By challenge_issued within 3DS
print("\n  Within THREE_DS, by challenge_issued:")
ctrl_ch = agg(three_ds_ctrl, "challenge_issued")
test_ch = agg(three_ds_test, "challenge_issued")
for k in sorted(set(ctrl_ch.keys()) | set(test_ch.keys())):
    c = ctrl_ch.get(k, {"attempts": 0, "successful": 0})
    t = test_ch.get(k, {"attempts": 0, "successful": 0})
    c_sr = pct(c["successful"], c["attempts"])
    t_sr = pct(t["successful"], t["attempts"])
    z, p = z_test_proportions(c["successful"], c["attempts"], t["successful"], t["attempts"])
    print(f"    challenge={k:<8} Ctrl: {c['attempts']:>6,} SR={c_sr:.2f}% | Test: {t['attempts']:>6,} SR={t_sr:.2f}% | Δ={c_sr-t_sr:+.2f}pp z={z:.3f} p={p:.4f}")

# 3DS exemption by challenge
exemption_ctrl = [r for r in ctrl if r["fraud_pre_auth_result"] == "THREE_DS_EXEMPTION"]
exemption_test = [r for r in test if r["fraud_pre_auth_result"] == "THREE_DS_EXEMPTION"]

print("\n  Within THREE_DS_EXEMPTION, by challenge_issued:")
ctrl_exc = agg(exemption_ctrl, "challenge_issued")
test_exc = agg(exemption_test, "challenge_issued")
for k in sorted(set(ctrl_exc.keys()) | set(test_exc.keys())):
    c = ctrl_exc.get(k, {"attempts": 0, "successful": 0})
    t = test_exc.get(k, {"attempts": 0, "successful": 0})
    c_sr = pct(c["successful"], c["attempts"])
    t_sr = pct(t["successful"], t["attempts"])
    z, p = z_test_proportions(c["successful"], c["attempts"], t["successful"], t["attempts"])
    print(f"    challenge={k:<8} Ctrl: {c['attempts']:>6,} SR={c_sr:.2f}% | Test: {t['attempts']:>6,} SR={t_sr:.2f}% | Δ={c_sr-t_sr:+.2f}pp z={z:.3f} p={p:.4f}")

# ═══════════════════════════════════════════════════════
# SECTION 7: ADDITIONAL DEEP DIVES
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 7: ADDITIONAL DEEP DIVES")
print("=" * 90)

# Frictionless (challenge_issued = false) by processor × network
print("\n--- Frictionless (challenge_issued=false) by processor × network ---")
ctrl_fl = [r for r in ctrl if r["challenge_issued"] == "false"]
test_fl = [r for r in test if r["challenge_issued"] == "false"]
ctrl_fl_np = agg(ctrl_fl, ["payment_method_variant", "payment_processor"])
test_fl_np = agg(test_fl, ["payment_method_variant", "payment_processor"])

print(f"\n{'Network × Processor':<35} {'Ctrl Att':>9} {'Ctrl SR':>8} {'Test Att':>9} {'Test SR':>8} {'Δ SR':>8} {'z':>7} {'p':>8}")
print("-" * 100)
for k in sorted(set(ctrl_fl_np.keys()) | set(test_fl_np.keys())):
    c = ctrl_fl_np.get(k, {"attempts": 0, "successful": 0})
    t = test_fl_np.get(k, {"attempts": 0, "successful": 0})
    if c["attempts"] >= 20 or t["attempts"] >= 20:
        c_sr = pct(c["successful"], c["attempts"])
        t_sr = pct(t["successful"], t["attempts"])
        gap = c_sr - t_sr
        z, p = z_test_proportions(c["successful"], c["attempts"], t["successful"], t["attempts"])
        label = f"{k[0]} × {k[1]}"
        print(f"  {label:<33} {c['attempts']:>9,} {c_sr:>7.2f}% {t['attempts']:>9,} {t_sr:>7.2f}% {gap:>+7.2f}pp {z:>6.3f}  {p:>7.4f}")

# JPMC vs Checkout volume shift analysis
print("\n--- JPMC vs Checkout volume shift ---")
for proc in ["CHECKOUT", "JPMORGAN"]:
    c = ctrl_proc.get(proc, {"attempts": 0, "successful": 0})
    t = test_proc.get(proc, {"attempts": 0, "successful": 0})
    c_sr = pct(c["successful"], c["attempts"])
    t_sr = pct(t["successful"], t["attempts"])
    print(f"  {proc}: Ctrl {c['attempts']:>7,} ({pct(c['attempts'], ctrl_total_att):.1f}%) SR={c_sr:.2f}% | Test {t['attempts']:>7,} ({pct(t['attempts'], test_total_att):.1f}%) SR={t_sr:.2f}%")

# Detailed 3-way: fraud × processor × network (top combinations)
print("\n--- Top fraud × processor × network combinations ---")
ctrl_3way = agg(ctrl, ["fraud_pre_auth_result", "payment_processor", "payment_method_variant"])
test_3way = agg(test, ["fraud_pre_auth_result", "payment_processor", "payment_method_variant"])

all_3way = sorted(set(ctrl_3way.keys()) | set(test_3way.keys()))
big_combos = []
for k in all_3way:
    c = ctrl_3way.get(k, {"attempts": 0, "successful": 0})
    t = test_3way.get(k, {"attempts": 0, "successful": 0})
    total = c["attempts"] + t["attempts"]
    if total >= 100:
        c_sr = pct(c["successful"], c["attempts"])
        t_sr = pct(t["successful"], t["attempts"])
        gap = c_sr - t_sr
        z, p_val = z_test_proportions(c["successful"], c["attempts"], t["successful"], t["attempts"])
        big_combos.append((k, c, t, c_sr, t_sr, gap, z, p_val))

big_combos.sort(key=lambda x: abs(x[5]), reverse=True)
print(f"\n{'Fraud × Proc × Net':<50} {'Ctrl':>7} {'CtrlSR':>7} {'Test':>7} {'TestSR':>7} {'Δ':>7} {'z':>7} {'p':>7}")
print("-" * 110)
for k, c, t, c_sr, t_sr, gap, z, p_val in big_combos[:25]:
    label = f"{k[0]} × {k[1]} × {k[2]}"
    print(f"  {label:<48} {c['attempts']:>7,} {c_sr:>6.1f}% {t['attempts']:>7,} {t_sr:>6.1f}% {gap:>+6.1f}pp {z:>6.2f}  {p_val:>6.4f}")

# ═══════════════════════════════════════════════════════
# SECTION 8: QUANTIFY EACH CONTRIBUTING FACTOR
# ═══════════════════════════════════════════════════════
print("\n" + "=" * 90)
print("SECTION 8: SUMMARY WATERFALL — WHAT EXPLAINS THE GAP?")
print("=" * 90)

total_gap_pp = pct(ctrl_tot["successful"], ctrl_tot["attempts"]) - pct(test_tot["successful"], test_tot["attempts"])
print(f"\n  Total observed SR gap (Control − Test): {total_gap_pp:+.4f}pp")
print(f"  = Composition (routing) effect:          {composition_effect:+.4f}pp")
print(f"  + Within-cell (performance) effect:       {within_effect:+.4f}pp")
print(f"  + Interaction:                            {interaction_effect:+.4f}pp")

# Processor NONE contribution
none_c = ctrl_proc.get("NONE", {"attempts": 0, "successful": 0})
none_t = test_proc.get("NONE", {"attempts": 0, "successful": 0})
none_extra = none_t["attempts"] - none_c["attempts"]
# If those extra "NONE" attempts had the same SR as the overall control:
if none_extra > 0:
    ctrl_overall_sr = ctrl_tot["successful"] / ctrl_tot["attempts"]
    lost_succ = none_extra * ctrl_overall_sr
    none_pp_impact = lost_succ / test_tot["attempts"] * 100
    print(f"\n  Excess 'NONE' processor attempts in test: {none_extra}")
    print(f"  If those had ctrl-average SR, recovered: ~{none_pp_impact:.3f}pp")

print("\n  DONE.")
