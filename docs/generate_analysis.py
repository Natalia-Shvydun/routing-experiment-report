#!/usr/bin/env python3
"""Generate stored cards experiment analysis: charts + markdown + HTML."""

import os, math, json, base64, glob
import numpy as np
from scipy import stats
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

CHART_DIR = os.path.join(os.path.dirname(__file__), 'charts')
os.makedirs(CHART_DIR, exist_ok=True)

COLORS = {
    'control': '#5B8FF9', 'test': '#5AD8A6',
    'storedcard': '#F6BD16', 'payment_card': '#5B8FF9', 'creditcard': '#5B8FF9',
    'apple_pay': '#333333', 'google_pay': '#4285F4', 'paypal': '#003087',
    'klarna': '#FFB3C7', 'adyen_ideal': '#CC0066', 'adyen_twint': '#000000',
    'adyen_blik': '#E30613', 'adyen_mobilepay': '#5A78FF', 'adyen_mbway': '#D4213D',
    'dlocal_pix': '#32BCAD', 'adyen_vipps': '#FF5B24', 'adyen_bancontact_payconiq': '#005498',
    'adyen_swish': '#008CD2', 'other': '#AAAAAA',
}

def fmt(v, is_pct=True, threshold=1.0):
    """Format: >threshold% → 1 decimal, <=threshold% → 2 decimals."""
    if v is None: return '—'
    if is_pct:
        if abs(v) > threshold:
            return f"{v:.1f}%"
        else:
            return f"{v:.2f}%"
    return str(v)

def fmt_pp(v, threshold=1.0):
    if v is None: return '—'
    if abs(v) > threshold:
        return f"{v:+.1f}"
    return f"{v:+.2f}"

def fmt_plain(v, threshold=1.0):
    if abs(v) > threshold:
        return f"{v:.1f}"
    return f"{v:.2f}"

def z_test(n1, s1, n2, s2):
    p1, p2 = s1/n1, s2/n2
    p_pool = (s1+s2)/(n1+n2)
    se = math.sqrt(p_pool*(1-p_pool)*(1/n1+1/n2))
    if se == 0: return 0, 1.0
    z = (p2-p1)/se
    pval = 2*(1-stats.norm.cdf(abs(z)))
    return z, pval

def sig_stars(p):
    if p < 0.001: return '***'
    if p < 0.01: return '**'
    if p < 0.05: return '*'
    return ''

def required_n(p_base, delta, alpha=0.05, power=0.8):
    z_a = stats.norm.ppf(1 - alpha/2)
    z_b = stats.norm.ppf(power)
    p2 = p_base + delta
    n = ((z_a*math.sqrt(2*p_base*(1-p_base)) + z_b*math.sqrt(p_base*(1-p_base)+p2*(1-p2)))**2) / (delta**2)
    return int(math.ceil(n))

# ============================================================
# DATA (from query results)
# ============================================================

# Main segmented metrics (from MAIN query)
main = {
    ('ios','0','with_stored_cards'):    {'pp':37575, 'init':35881, 'booked':35071, 'used_sc':0},
    ('ios','1','with_stored_cards'):    {'pp':37760, 'init':36197, 'booked':35509, 'used_sc':20923},
    ('ios','0','without_stored_cards'): {'pp':378418, 'init':359365, 'booked':350382, 'used_sc':0},
    ('ios','1','without_stored_cards'): {'pp':379294, 'init':360223, 'booked':351407, 'used_sc':9543},
    ('android','0','with_stored_cards'):    {'pp':25006, 'init':24060, 'booked':23040, 'used_sc':0},
    ('android','1','with_stored_cards'):    {'pp':24740, 'init':23890, 'booked':23150, 'used_sc':18328},
    ('android','0','without_stored_cards'): {'pp':129773, 'init':123983, 'booked':118422, 'used_sc':0},
    ('android','1','without_stored_cards'): {'pp':129740, 'init':123945, 'booked':118432, 'used_sc':7401},
}

# SRM
srm = {
    ('ios','0'): 1380765, ('ios','1'): 1383521,
    ('android','0'): 331721, ('android','1'): 331777,
}

# GCR
gcr_custs = {
    'ios': {'distinct_customers_pp': 84059, 'distinct_customers_booked': 78946},
    'android': {'distinct_customers_pp': 41570, 'distinct_customers_booked': 38762},
}
global_booked = 2908727

# Journey
journey = {
    ('ios','0','with_stored_cards'):     {'gained_sc': 0, 'gained_more': 777, 'returned': 29795, 'total': 37575},
    ('ios','1','with_stored_cards'):     {'gained_sc': 0, 'gained_more': 1422, 'returned': 29916, 'total': 37760},
    ('ios','0','without_stored_cards'):  {'gained_sc': 5182, 'gained_more': 0, 'returned': 212105, 'total': 378418},
    ('ios','1','without_stored_cards'):  {'gained_sc': 10477, 'gained_more': 0, 'returned': 212453, 'total': 379294},
    ('android','0','with_stored_cards'):    {'gained_sc': 0, 'gained_more': 1862, 'returned': 11606, 'total': 25006},
    ('android','1','with_stored_cards'):    {'gained_sc': 0, 'gained_more': 1201, 'returned': 11571, 'total': 24740},
    ('android','0','without_stored_cards'): {'gained_sc': 7525, 'gained_more': 0, 'returned': 48018, 'total': 129773},
    ('android','1','without_stored_cards'): {'gained_sc': 6686, 'gained_more': 0, 'returned': 48054, 'total': 129740},
}

# Retry analysis (from RETRY query)
retry = {
    ('ios','0','with_stored_cards'):    {'total_carts':68340, 'single_carts':58984, 'single_success':58751, 'multi_carts':9356, 'multi_success':8975, 'multi_started_card':4178, 'multi_card_success':3915},
    ('ios','1','with_stored_cards'):    {'total_carts':69742, 'single_carts':59186, 'single_success':58880, 'multi_carts':10556, 'multi_success':10164, 'multi_started_card':7271, 'multi_card_success':6971},
    ('android','0','with_stored_cards'):    {'total_carts':43737, 'single_carts':37261, 'single_success':36963, 'multi_carts':6476, 'multi_success':6157, 'multi_started_card':4189, 'multi_card_success':3953},
    ('android','1','with_stored_cards'):    {'total_carts':44806, 'single_carts':37694, 'single_success':37431, 'multi_carts':7112, 'multi_success':6772, 'multi_started_card':6099, 'multi_card_success':5791},
}

# Opt-in (from OPTIN3 query)
optin = {
    ('ios','0','with_stored_cards'):     {'card_sub': 14483, 'opted': 0},
    ('ios','1','with_stored_cards'):     {'card_sub': 6016, 'opted': 2924},
    ('ios','0','without_stored_cards'):  {'card_sub': 102418, 'opted': 0},
    ('ios','1','without_stored_cards'):  {'card_sub': 99830, 'opted': 18045},
    ('android','0','with_stored_cards'):    {'card_sub': 17538, 'opted': 13035},
    ('android','1','with_stored_cards'):    {'card_sub': 5610, 'opted': 3448},
    ('android','0','without_stored_cards'): {'card_sub': 75591, 'opted': 59669},
    ('android','1','without_stored_cards'): {'card_sub': 74813, 'opted': 54953},
}

# Payment method mix (% of initiated visitors) - top methods from PMIX2
pmix_data_raw = """ios|0|with_stored_cards|apple_pay|21423|35881|59.71
ios|0|with_stored_cards|payment_card|14262|35881|39.75
ios|0|with_stored_cards|paypal|1923|35881|5.36
ios|0|with_stored_cards|adyen_ideal|237|35881|0.66
ios|0|with_stored_cards|adyen_twint|230|35881|0.64
ios|0|with_stored_cards|adyen_blik|110|35881|0.31
ios|0|with_stored_cards|klarna|110|35881|0.31
ios|0|with_stored_cards|adyen_mobilepay|98|35881|0.27
ios|0|with_stored_cards|adyen_mbway|95|35881|0.26
ios|0|with_stored_cards|dlocal_pix|79|35881|0.22
ios|0|with_stored_cards|adyen_vipps|52|35881|0.14
ios|0|with_stored_cards|adyen_bancontact_payconiq|50|35881|0.14
ios|0|with_stored_cards|adyen_swish|26|35881|0.07
ios|1|with_stored_cards|payment_card|22852|36197|63.13
ios|1|with_stored_cards|apple_pay|14496|36197|40.05
ios|1|with_stored_cards|paypal|1460|36197|4.03
ios|1|with_stored_cards|adyen_twint|220|36197|0.61
ios|1|with_stored_cards|adyen_ideal|202|36197|0.56
ios|1|with_stored_cards|adyen_mbway|124|36197|0.34
ios|1|with_stored_cards|klarna|116|36197|0.32
ios|1|with_stored_cards|adyen_blik|88|36197|0.24
ios|1|with_stored_cards|adyen_mobilepay|83|36197|0.23
ios|1|with_stored_cards|dlocal_pix|61|36197|0.17
ios|1|with_stored_cards|adyen_vipps|44|36197|0.12
ios|1|with_stored_cards|adyen_bancontact_payconiq|39|36197|0.11
ios|1|with_stored_cards|adyen_swish|20|36197|0.06
ios|0|without_stored_cards|apple_pay|222881|359365|62.02
ios|0|without_stored_cards|payment_card|97885|359365|27.24
ios|0|without_stored_cards|paypal|37663|359365|10.48
ios|0|without_stored_cards|adyen_ideal|5558|359365|1.55
ios|0|without_stored_cards|adyen_twint|3533|359365|0.98
ios|0|without_stored_cards|adyen_blik|1637|359365|0.46
ios|0|without_stored_cards|klarna|1635|359365|0.45
ios|0|without_stored_cards|adyen_mobilepay|1438|359365|0.40
ios|0|without_stored_cards|adyen_mbway|1038|359365|0.29
ios|0|without_stored_cards|adyen_vipps|719|359365|0.20
ios|0|without_stored_cards|dlocal_pix|596|359365|0.17
ios|0|without_stored_cards|adyen_bancontact_payconiq|545|359365|0.15
ios|0|without_stored_cards|adyen_swish|416|359365|0.12
ios|1|without_stored_cards|apple_pay|222524|360223|61.77
ios|1|without_stored_cards|payment_card|98399|360223|27.32
ios|1|without_stored_cards|paypal|37795|360223|10.49
ios|1|without_stored_cards|adyen_ideal|5526|360223|1.53
ios|1|without_stored_cards|adyen_twint|3420|360223|0.95
ios|1|without_stored_cards|klarna|1641|360223|0.46
ios|1|without_stored_cards|adyen_blik|1542|360223|0.43
ios|1|without_stored_cards|adyen_mobilepay|1384|360223|0.38
ios|1|without_stored_cards|adyen_mbway|1059|360223|0.29
ios|1|without_stored_cards|adyen_vipps|690|360223|0.19
ios|1|without_stored_cards|dlocal_pix|628|360223|0.17
ios|1|without_stored_cards|adyen_bancontact_payconiq|561|360223|0.16
ios|1|without_stored_cards|adyen_swish|449|360223|0.12
android|0|with_stored_cards|payment_card|16184|24060|67.27
android|0|with_stored_cards|google_pay|6107|24060|25.38
android|0|with_stored_cards|paypal|2161|24060|8.98
android|0|with_stored_cards|klarna|373|24060|1.55
android|0|with_stored_cards|adyen_ideal|255|24060|1.06
android|0|with_stored_cards|adyen_blik|254|24060|1.06
android|0|with_stored_cards|adyen_twint|244|24060|1.01
android|0|with_stored_cards|dlocal_pix|161|24060|0.67
android|0|with_stored_cards|adyen_mobilepay|139|24060|0.58
android|0|with_stored_cards|adyen_mbway|122|24060|0.51
android|0|with_stored_cards|adyen_bancontact_payconiq|84|24060|0.35
android|0|with_stored_cards|adyen_swish|54|24060|0.22
android|0|with_stored_cards|adyen_vipps|44|24060|0.18
android|1|with_stored_cards|payment_card|19839|23890|83.04
android|1|with_stored_cards|google_pay|3078|23890|12.88
android|1|with_stored_cards|paypal|1493|23890|6.25
android|1|with_stored_cards|klarna|326|23890|1.36
android|1|with_stored_cards|adyen_ideal|166|23890|0.69
android|1|with_stored_cards|adyen_blik|165|23890|0.69
android|1|with_stored_cards|adyen_twint|159|23890|0.67
android|1|with_stored_cards|adyen_mbway|100|23890|0.42
android|1|with_stored_cards|adyen_mobilepay|62|23890|0.26
android|1|with_stored_cards|dlocal_pix|61|23890|0.26
android|1|with_stored_cards|adyen_bancontact_payconiq|33|23890|0.14
android|1|with_stored_cards|adyen_vipps|25|23890|0.10
android|1|with_stored_cards|adyen_swish|12|23890|0.05
android|0|without_stored_cards|payment_card|68657|123983|55.38
android|0|without_stored_cards|paypal|23840|123983|19.23
android|0|without_stored_cards|google_pay|23794|123983|19.19
android|0|without_stored_cards|klarna|3361|123983|2.71
android|0|without_stored_cards|adyen_ideal|3311|123983|2.67
android|0|without_stored_cards|adyen_blik|2430|123983|1.96
android|0|without_stored_cards|adyen_twint|1666|123983|1.34
android|0|without_stored_cards|dlocal_pix|906|123983|0.73
android|0|without_stored_cards|adyen_mobilepay|904|123983|0.73
android|0|without_stored_cards|adyen_mbway|857|123983|0.69
android|0|without_stored_cards|adyen_bancontact_payconiq|450|123983|0.36
android|0|without_stored_cards|adyen_vipps|268|123983|0.22
android|0|without_stored_cards|adyen_swish|236|123983|0.19
android|1|without_stored_cards|payment_card|68315|123945|55.12
android|1|without_stored_cards|paypal|23955|123945|19.33
android|1|without_stored_cards|google_pay|23108|123945|18.64
android|1|without_stored_cards|klarna|3387|123945|2.73
android|1|without_stored_cards|adyen_ideal|3281|123945|2.65
android|1|without_stored_cards|adyen_blik|2414|123945|1.95
android|1|without_stored_cards|adyen_twint|1681|123945|1.36
android|1|without_stored_cards|adyen_mbway|877|123945|0.71
android|1|without_stored_cards|adyen_mobilepay|827|123945|0.67
android|1|without_stored_cards|dlocal_pix|800|123945|0.65
android|1|without_stored_cards|adyen_bancontact_payconiq|444|123945|0.36
android|1|without_stored_cards|adyen_vipps|283|123945|0.23
android|1|without_stored_cards|adyen_swish|261|123945|0.21"""

pmix = {}
for line in pmix_data_raw.strip().split('\n'):
    parts = line.split('|')
    plat, var, seg, method = parts[0], parts[1], parts[2], parts[3]
    visitors, total_init, pct = int(parts[4]), int(parts[5]), float(parts[6])
    key = (plat, var, seg)
    if key not in pmix: pmix[key] = {}
    pmix[key][method] = {'visitors': visitors, 'total_init': total_init, 'pct': pct}

# Attempt SR by method (from ATTSR2) — for with_stored_cards only
attsr_raw = """ios|0|with_stored_cards|apple_pay|45137|42222|93.54
ios|0|with_stored_cards|payment_card|32036|26973|84.20
ios|0|with_stored_cards|paypal|2989|2935|98.19
ios|1|with_stored_cards|payment_card|52949|44712|84.44
ios|1|with_stored_cards|apple_pay|28964|26963|93.09
ios|1|with_stored_cards|paypal|2192|2158|98.45
android|0|with_stored_cards|payment_card|34075|29838|87.57
android|0|with_stored_cards|google_pay|12768|11280|88.35
android|0|with_stored_cards|paypal|3293|3240|98.39
android|1|with_stored_cards|payment_card|45563|38746|85.04
android|1|with_stored_cards|google_pay|6015|5355|89.03
android|1|with_stored_cards|paypal|2311|2277|98.53"""

attsr = {}
for line in attsr_raw.strip().split('\n'):
    parts = line.split('|')
    plat, var, seg, method = parts[0], parts[1], parts[2], parts[3]
    attempts, successes, sr = int(parts[4]), int(parts[5]), float(parts[6])
    key = (plat, var, seg)
    if key not in attsr: attsr[key] = {}
    attsr[key][method] = {'attempts': attempts, 'successes': successes, 'sr': sr}

# CR by method (from CRBYMETHOD - for with_stored_cards only)
crmethod_raw = """android|0|with_stored_cards|payment_card|16217|15797
android|0|with_stored_cards|google_pay|6114|5971
android|0|with_stored_cards|paypal|2166|2136
android|0|with_stored_cards|klarna|373|367
android|0|with_stored_cards|adyen_ideal|256|251
android|0|with_stored_cards|adyen_blik|254|253
android|0|with_stored_cards|adyen_twint|244|239
android|1|with_stored_cards|payment_card|19877|19428
android|1|with_stored_cards|google_pay|3084|3020
android|1|with_stored_cards|paypal|1493|1477
android|1|with_stored_cards|klarna|326|325
android|1|with_stored_cards|adyen_ideal|166|161
android|1|with_stored_cards|adyen_blik|165|164
android|1|with_stored_cards|adyen_twint|159|159
ios|0|with_stored_cards|apple_pay|21488|21305
ios|0|with_stored_cards|payment_card|14319|13996
ios|0|with_stored_cards|paypal|1926|1916
ios|0|with_stored_cards|adyen_ideal|237|235
ios|0|with_stored_cards|adyen_twint|231|229
ios|1|with_stored_cards|payment_card|22913|22528
ios|1|with_stored_cards|apple_pay|14536|14392
ios|1|with_stored_cards|paypal|1462|1448
ios|1|with_stored_cards|adyen_twint|221|216
ios|1|with_stored_cards|adyen_ideal|202|200"""

crmethod = {}
for line in crmethod_raw.strip().split('\n'):
    parts = line.split('|')
    plat, var, seg, method = parts[0], parts[1], parts[2], parts[3]
    attempted, booked = int(parts[4]), int(parts[5])
    key = (plat, var, seg)
    if key not in crmethod: crmethod[key] = {}
    crmethod[key][method] = {'attempted': attempted, 'booked': booked, 'cr': booked/attempted*100}

# ============================================================
# COMPUTATIONS
# ============================================================

def compute_metrics(plat, seg):
    c = main[(plat, '0', seg)]
    t = main[(plat, '1', seg)]
    cr_c, cr_t = c['booked']/c['pp']*100, t['booked']/t['pp']*100
    ir_c, ir_t = c['init']/c['pp']*100, t['init']/t['pp']*100
    psr_c = c['booked']/c['init']*100 if c['init'] else 0
    psr_t = t['booked']/t['init']*100 if t['init'] else 0
    _, p_cr = z_test(c['pp'], c['booked'], t['pp'], t['booked'])
    _, p_ir = z_test(c['pp'], c['init'], t['pp'], t['init'])
    _, p_psr = z_test(c['init'], c['booked'], t['init'], t['booked'])
    return {
        'cr_c': cr_c, 'cr_t': cr_t, 'cr_d': cr_t-cr_c, 'cr_rel': (cr_t-cr_c)/cr_c*100, 'cr_p': p_cr,
        'ir_c': ir_c, 'ir_t': ir_t, 'ir_d': ir_t-ir_c, 'ir_rel': (ir_t-ir_c)/ir_c*100, 'ir_p': p_ir,
        'psr_c': psr_c, 'psr_t': psr_t, 'psr_d': psr_t-psr_c, 'psr_rel': (psr_t-psr_c)/psr_c*100, 'psr_p': p_psr,
        'n_c': c['pp'], 'n_t': t['pp'],
    }

results = {}
for plat in ['ios', 'android']:
    for seg in ['with_stored_cards', 'without_stored_cards']:
        results[(plat, seg)] = compute_metrics(plat, seg)

# ============================================================
# CHART 1: Conversion by Segment (bar chart)
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
for idx, plat in enumerate(['ios', 'android']):
    ax = axes[idx]
    ws = results[(plat, 'with_stored_cards')]
    wos = results[(plat, 'without_stored_cards')]
    x = np.arange(2)
    w = 0.3
    bars_c = [ws['cr_c'], wos['cr_c']]
    bars_t = [ws['cr_t'], wos['cr_t']]
    ax.bar(x - w/2, bars_c, w, label='Control', color=COLORS['control'], zorder=3)
    ax.bar(x + w/2, bars_t, w, label='Test', color=COLORS['test'], zorder=3)
    for i, (vc, vt) in enumerate(zip(bars_c, bars_t)):
        ax.text(i-w/2, vc+0.15, fmt(vc), ha='center', va='bottom', fontsize=8)
        ax.text(i+w/2, vt+0.15, fmt(vt), ha='center', va='bottom', fontsize=8)
        delta = vt - vc
        sig = sig_stars(ws['cr_p'] if i == 0 else wos['cr_p'])
        if sig:
            ax.text(i, max(vc, vt)+0.6, f"{fmt_pp(delta)}pp {sig}", ha='center', fontsize=8, color='green' if delta > 0 else 'red')
    ax.set_xticks(x)
    ax.set_xticklabels(['With Stored Cards', 'Without Stored Cards'])
    ax.set_ylim(88, 100)
    ax.set_ylabel('Conversion Rate (%)')
    ax.set_title(f'{plat.upper()} — Conversion Rate by Segment')
    ax.legend()
    ax.grid(axis='y', alpha=0.3, zorder=0)
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, '01_conversion_by_segment.png'), dpi=150, bbox_inches='tight')
plt.close()

# ============================================================
# CHART 2: Payment Method Mix — Stacked Bar
# ============================================================
top_methods_ios = ['apple_pay', 'payment_card', 'paypal', 'klarna', 'adyen_ideal', 'adyen_twint', 'adyen_blik', 'adyen_mobilepay', 'adyen_mbway', 'dlocal_pix', 'adyen_vipps', 'adyen_bancontact_payconiq', 'adyen_swish']
top_methods_android = ['payment_card', 'google_pay', 'paypal', 'klarna', 'adyen_ideal', 'adyen_blik', 'adyen_twint', 'dlocal_pix', 'adyen_mobilepay', 'adyen_mbway', 'adyen_bancontact_payconiq', 'adyen_vipps', 'adyen_swish']

def make_stacked_bar(plat, seg, methods, ax, title):
    c_data = pmix.get((plat, '0', seg), {})
    t_data = pmix.get((plat, '1', seg), {})
    method_names = []
    c_pcts = []
    t_pcts = []
    for m in methods:
        if m in c_data or m in t_data:
            method_names.append(m)
            c_pcts.append(c_data.get(m, {}).get('pct', 0))
            t_pcts.append(t_data.get(m, {}).get('pct', 0))

    y = np.arange(2)
    left_c = np.zeros(1)
    left_t = np.zeros(1)
    for i, m in enumerate(method_names):
        color = COLORS.get(m, COLORS['other'])
        ax.barh(1, c_pcts[i], left=left_c[0], height=0.5, color=color, edgecolor='white', linewidth=0.5)
        ax.barh(0, t_pcts[i], left=left_t[0], height=0.5, color=color, edgecolor='white', linewidth=0.5,
                label=m.replace('adyen_', '').replace('dlocal_', '').replace('_', ' ').title() if i < len(method_names) else '')
        if c_pcts[i] > 3:
            ax.text(left_c[0] + c_pcts[i]/2, 1, f"{c_pcts[i]:.0f}%", ha='center', va='center', fontsize=6, color='white', fontweight='bold')
        if t_pcts[i] > 3:
            ax.text(left_t[0] + t_pcts[i]/2, 0, f"{t_pcts[i]:.0f}%", ha='center', va='center', fontsize=6, color='white', fontweight='bold')
        left_c[0] += c_pcts[i]
        left_t[0] += t_pcts[i]
    ax.set_yticks([0, 1])
    ax.set_yticklabels(['Test', 'Control'])
    ax.set_xlabel('% of Visitors with Initiated Payments')
    ax.set_title(title)
    ax.set_xlim(0, max(left_c[0], left_t[0]) * 1.02)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[:len(method_names)], [m.replace('adyen_', '').replace('dlocal_', '').replace('_', ' ').title() for m in method_names],
              loc='upper right', fontsize=5, ncol=2)

fig, axes = plt.subplots(2, 2, figsize=(16, 8))
make_stacked_bar('ios', 'with_stored_cards', top_methods_ios, axes[0,0], 'iOS — With Stored Cards')
make_stacked_bar('ios', 'without_stored_cards', top_methods_ios, axes[0,1], 'iOS — Without Stored Cards')
make_stacked_bar('android', 'with_stored_cards', top_methods_android, axes[1,0], 'Android — With Stored Cards')
make_stacked_bar('android', 'without_stored_cards', top_methods_android, axes[1,1], 'Android — Without Stored Cards')
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, '02_payment_method_mix.png'), dpi=150, bbox_inches='tight')
plt.close()

# ============================================================
# CHART 3: Cannibalization vs Incremental (initiation rates)
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
for idx, plat in enumerate(['ios', 'android']):
    ax = axes[idx]
    c_data = pmix.get((plat, '0', 'with_stored_cards'), {})
    t_data = pmix.get((plat, '1', 'with_stored_cards'), {})
    methods_order = top_methods_ios if plat == 'ios' else top_methods_android
    methods_present = [m for m in methods_order if m in c_data or m in t_data]

    c_rates = [c_data.get(m, {}).get('pct', 0) for m in methods_present]
    t_rates = [t_data.get(m, {}).get('pct', 0) for m in methods_present]
    deltas = [t - c for t, c in zip(t_rates, c_rates)]

    total_init_c = main[(plat, '0', 'with_stored_cards')]['init']
    total_init_t = main[(plat, '1', 'with_stored_cards')]['init']
    total_pp_c = main[(plat, '0', 'with_stored_cards')]['pp']
    total_pp_t = main[(plat, '1', 'with_stored_cards')]['pp']
    init_rate_c = total_init_c / total_pp_c * 100
    init_rate_t = total_init_t / total_pp_t * 100
    init_delta = init_rate_t - init_rate_c

    pc_sum = sum(t_rates)
    method_labels = [m.replace('adyen_', '').replace('dlocal_', '').replace('_', ' ').title() for m in methods_present]
    colors = ['green' if d > 0 else '#CC3333' for d in deltas]
    y = np.arange(len(methods_present))
    ax.barh(y, deltas, color=colors, height=0.6)
    for i, d in enumerate(deltas):
        if abs(d) > 0.3:
            ax.text(d + (0.3 if d > 0 else -0.3), i, f"{d:+.1f}pp", va='center', fontsize=7)
    ax.set_yticks(y)
    ax.set_yticklabels(method_labels, fontsize=8)
    ax.axvline(0, color='black', linewidth=0.5)
    ax.set_xlabel('Change in Initiation Rate (pp)')
    ax.set_title(f'{plat.upper()} — With SC: Change in Method Initiation Rate\n(Total initiation rate change: {init_delta:+.2f}pp = incremental)')
    ax.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, '03_cannibalization.png'), dpi=150, bbox_inches='tight')
plt.close()

# ============================================================
# CHART 4: CR by Payment Method
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
for idx, plat in enumerate(['ios', 'android']):
    ax = axes[idx]
    c_data = crmethod.get((plat, '0', 'with_stored_cards'), {})
    t_data = crmethod.get((plat, '1', 'with_stored_cards'), {})
    methods_order = ['apple_pay', 'payment_card', 'paypal', 'adyen_ideal', 'adyen_twint'] if plat == 'ios' else ['payment_card', 'google_pay', 'paypal', 'klarna', 'adyen_ideal', 'adyen_blik', 'adyen_twint']
    methods_present = [m for m in methods_order if m in c_data or m in t_data]
    labels = [m.replace('adyen_', '').replace('_', ' ').title() for m in methods_present]
    x = np.arange(len(methods_present))
    w = 0.3
    c_crs = [c_data.get(m, {}).get('cr', 0) for m in methods_present]
    t_crs = [t_data.get(m, {}).get('cr', 0) for m in methods_present]
    ax.bar(x - w/2, c_crs, w, label='Control', color=COLORS['control'], zorder=3)
    ax.bar(x + w/2, t_crs, w, label='Test', color=COLORS['test'], zorder=3)
    for i in range(len(methods_present)):
        ax.text(i-w/2, c_crs[i]+0.3, f"{c_crs[i]:.1f}%", ha='center', fontsize=7, rotation=0)
        ax.text(i+w/2, t_crs[i]+0.3, f"{t_crs[i]:.1f}%", ha='center', fontsize=7, rotation=0)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylim(90, 102)
    ax.set_ylabel('Conversion Rate (%)')
    ax.set_title(f'{plat.upper()} — CR by Payment Method (With SC)')
    ax.legend()
    ax.grid(axis='y', alpha=0.3, zorder=0)
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, '04_cr_by_method.png'), dpi=150, bbox_inches='tight')
plt.close()

# ============================================================
# CHART 5: Attempt-Level Deep Dive
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
for idx, plat in enumerate(['ios', 'android']):
    ax = axes[idx]
    r_c = retry[(plat, '0', 'with_stored_cards')]
    r_t = retry[(plat, '1', 'with_stored_cards')]
    labels_r = ['Single Attempt\nCart SR', 'Multi Attempt\nCart SR', 'Multi (Card First)\nCart SR']
    c_vals = [r_c['single_success']/r_c['single_carts']*100, r_c['multi_success']/r_c['multi_carts']*100, r_c['multi_card_success']/r_c['multi_started_card']*100]
    t_vals = [r_t['single_success']/r_t['single_carts']*100, r_t['multi_success']/r_t['multi_carts']*100, r_t['multi_card_success']/r_t['multi_started_card']*100]
    x = np.arange(len(labels_r))
    w = 0.3
    ax.bar(x - w/2, c_vals, w, label='Control', color=COLORS['control'], zorder=3)
    ax.bar(x + w/2, t_vals, w, label='Test', color=COLORS['test'], zorder=3)
    for i in range(len(labels_r)):
        ax.text(i-w/2, c_vals[i]+0.15, f"{c_vals[i]:.1f}%", ha='center', fontsize=8)
        ax.text(i+w/2, t_vals[i]+0.15, f"{t_vals[i]:.1f}%", ha='center', fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels_r, fontsize=8)
    ax.set_ylim(88, 102)
    ax.set_ylabel('Cart Success Rate (%)')
    ax.set_title(f'{plat.upper()} — Cart SR by Attempt Pattern (With SC)')
    ax.legend()
    ax.grid(axis='y', alpha=0.3, zorder=0)
    # Add cart counts
    for i, (cc, ct) in enumerate(zip(
        [r_c['single_carts'], r_c['multi_carts'], r_c['multi_started_card']],
        [r_t['single_carts'], r_t['multi_carts'], r_t['multi_started_card']])):
        ax.text(i, 88.5, f"n={cc:,}/{ct:,}", ha='center', fontsize=6, color='gray')
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, '05_attempt_deep_dive.png'), dpi=150, bbox_inches='tight')
plt.close()

# ============================================================
# CHART 6a: Gained Stored Cards
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
for idx, plat in enumerate(['ios', 'android']):
    ax = axes[idx]
    j_c = journey[(plat, '0', 'without_stored_cards')]
    j_t = journey[(plat, '1', 'without_stored_cards')]
    c_pct = j_c['gained_sc']/j_c['total']*100
    t_pct = j_t['gained_sc']/j_t['total']*100
    bars = ax.bar(['Control', 'Test'], [c_pct, t_pct], color=[COLORS['control'], COLORS['test']], width=0.5)
    ax.bar_label(bars, [f"{c_pct:.1f}%\n({j_c['gained_sc']:,})", f"{t_pct:.1f}%\n({j_t['gained_sc']:,})"], fontsize=9)
    ax.set_ylabel('% of Without-SC Visitors')
    ax.set_title(f'{plat.upper()} — Visitors Who Gained Stored Cards')
    ymax = max(c_pct, t_pct) * 1.4
    ax.set_ylim(0, ymax)
    ax.grid(axis='y', alpha=0.3)
    if plat == 'android':
        ax.text(0.5, 0.02, 'Note: For control, stored cards are those saved on web', ha='center', transform=ax.transAxes, fontsize=7, style='italic', color='gray')
    else:
        ax.text(0.5, 0.02, 'Note: For control, stored cards are those saved on web', ha='center', transform=ax.transAxes, fontsize=7, style='italic', color='gray')
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, '06a_gained_stored_cards.png'), dpi=150, bbox_inches='tight')
plt.close()

# ============================================================
# CHART 6b: Multiple Stored Cards
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
for idx, plat in enumerate(['ios', 'android']):
    ax = axes[idx]
    j_c = journey[(plat, '0', 'with_stored_cards')]
    j_t = journey[(plat, '1', 'with_stored_cards')]
    c_pct = j_c['gained_more']/j_c['total']*100
    t_pct = j_t['gained_more']/j_t['total']*100
    bars = ax.bar(['Control', 'Test'], [c_pct, t_pct], color=[COLORS['control'], COLORS['test']], width=0.5)
    ax.bar_label(bars, [f"{c_pct:.1f}%\n({j_c['gained_more']:,})", f"{t_pct:.1f}%\n({j_t['gained_more']:,})"], fontsize=9)
    ax.set_ylabel('% of With-SC Visitors')
    ax.set_title(f'{plat.upper()} — Visitors Who Gained Additional Stored Cards')
    ymax = max(c_pct, t_pct) * 1.4
    ax.set_ylim(0, ymax)
    ax.grid(axis='y', alpha=0.3)
    ax.text(0.5, 0.02, 'Note: For control, stored cards are those saved on web', ha='center', transform=ax.transAxes, fontsize=7, style='italic', color='gray')
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, '06b_multiple_stored_cards.png'), dpi=150, bbox_inches='tight')
plt.close()

# ============================================================
# CHART 7: Attempt SR by Method
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
for idx, plat in enumerate(['ios', 'android']):
    ax = axes[idx]
    c_data = attsr.get((plat, '0', 'with_stored_cards'), {})
    t_data = attsr.get((plat, '1', 'with_stored_cards'), {})
    methods_order = ['apple_pay', 'payment_card', 'paypal'] if plat == 'ios' else ['payment_card', 'google_pay', 'paypal']
    labels = [m.replace('_', ' ').title() for m in methods_order]
    x = np.arange(len(methods_order))
    w = 0.3
    c_srs = [c_data.get(m, {}).get('sr', 0) for m in methods_order]
    t_srs = [t_data.get(m, {}).get('sr', 0) for m in methods_order]
    ax.bar(x - w/2, c_srs, w, label='Control', color=COLORS['control'], zorder=3)
    ax.bar(x + w/2, t_srs, w, label='Test', color=COLORS['test'], zorder=3)
    for i in range(len(methods_order)):
        ax.text(i-w/2, c_srs[i]+0.3, f"{c_srs[i]:.1f}%", ha='center', fontsize=8)
        ax.text(i+w/2, t_srs[i]+0.3, f"{t_srs[i]:.1f}%", ha='center', fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylim(75, 102)
    ax.set_ylabel('Customer Attempt SR (%)')
    ax.set_title(f'{plat.upper()} — Attempt SR by Method (With SC)')
    ax.legend()
    ax.grid(axis='y', alpha=0.3, zorder=0)
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, '07_attempt_sr_by_method.png'), dpi=150, bbox_inches='tight')
plt.close()

print("All charts generated successfully.")

# ============================================================
# GCR CALCULATION
# ============================================================
for plat in ['ios', 'android']:
    ws = results[(plat, 'with_stored_cards')]
    cr_uplift_pp = ws['cr_d'] / 100  # as proportion
    exp_custs = gcr_custs[plat]['distinct_customers_booked']
    gcr_val = cr_uplift_pp * exp_custs / global_booked * 100
    print(f"GCR {plat}: CR_uplift={ws['cr_d']:.3f}pp, exp_customers_booked={exp_custs}, global={global_booked}, GCR={gcr_val:.4f}%")

# ============================================================
# SAMPLE SIZE ADEQUACY (with stored cards segment)
# ============================================================
for plat in ['ios', 'android']:
    ws = results[(plat, 'with_stored_cards')]
    baseline = ws['cr_c'] / 100
    delta = ws['cr_d'] / 100
    req = required_n(baseline, delta)
    current = min(ws['n_c'], ws['n_t'])
    print(f"Sample size {plat}: baseline={baseline:.4f}, delta={delta:.4f}pp, required={req:,}, current={current:,}, sufficient={'Yes' if current >= req else 'No'}")

# ============================================================
# INCREMENTALITY CALCULATION
# ============================================================
for plat in ['ios', 'android']:
    c_data = pmix.get((plat, '0', 'with_stored_cards'), {})
    t_data = pmix.get((plat, '1', 'with_stored_cards'), {})
    c_total_init = main[(plat, '0', 'with_stored_cards')]['init']
    t_total_init = main[(plat, '1', 'with_stored_cards')]['init']
    c_pp = main[(plat, '0', 'with_stored_cards')]['pp']
    t_pp = main[(plat, '1', 'with_stored_cards')]['pp']
    init_rate_c = c_total_init / c_pp * 100
    init_rate_t = t_total_init / t_pp * 100
    init_delta = init_rate_t - init_rate_c

    # payment_card initiation rate (includes stored cards in test)
    pc_c = c_data.get('payment_card', {}).get('pct', 0)
    pc_t = t_data.get('payment_card', {}).get('pct', 0)
    pc_delta = pc_t - pc_c

    print(f"\nIncrementality {plat}:")
    print(f"  Init rate: control={init_rate_c:.2f}%, test={init_rate_t:.2f}%, delta={init_delta:+.2f}pp")
    print(f"  Payment card init rate: control={pc_c:.2f}%, test={pc_t:.2f}%, delta={pc_delta:+.2f}pp")
    print(f"  Incremental init rate (total delta): {init_delta:+.2f}pp")
    print(f"  Payment card cannibalized other methods by {-pc_delta:+.2f}pp (cannibalization from other methods)")

    # Compute how much of total payment_card share increase is incremental vs cannibalization
    # Incremental portion = total init delta / payment_card share increase
    if pc_delta > 0:
        incr_pct = init_delta / pc_delta * 100 if pc_delta != 0 else 0
        print(f"  % incremental of payment_card increase: {incr_pct:.1f}% incremental, {100-incr_pct:.1f}% cannibalized")

# ============================================================
# RETRY ANALYSIS SUMMARY
# ============================================================
for plat in ['ios', 'android']:
    r_c = retry[(plat, '0', 'with_stored_cards')]
    r_t = retry[(plat, '1', 'with_stored_cards')]
    print(f"\nRetry Analysis {plat} (With SC):")
    print(f"  Control: {r_c['single_carts']:,} single / {r_c['multi_carts']:,} multi ({r_c['multi_carts']/r_c['total_carts']*100:.1f}%)")
    print(f"  Test:    {r_t['single_carts']:,} single / {r_t['multi_carts']:,} multi ({r_t['multi_carts']/r_t['total_carts']*100:.1f}%)")
    print(f"  Single SR: {r_c['single_success']/r_c['single_carts']*100:.2f}% -> {r_t['single_success']/r_t['single_carts']*100:.2f}%")
    print(f"  Multi SR:  {r_c['multi_success']/r_c['multi_carts']*100:.2f}% -> {r_t['multi_success']/r_t['multi_carts']*100:.2f}%")
    print(f"  Multi(card-first): {r_c['multi_started_card']:,} -> {r_t['multi_started_card']:,} ({r_t['multi_started_card']/r_t['multi_carts']*100:.1f}% of test multi)")
    print(f"  Multi(card-first) SR: {r_c['multi_card_success']/r_c['multi_started_card']*100:.1f}% -> {r_t['multi_card_success']/r_t['multi_started_card']*100:.1f}%")
