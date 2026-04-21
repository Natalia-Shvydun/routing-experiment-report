export const EXPERIMENT_NAME = 'Payment Orchestration Routing — In-House vs Primer';
export const EXPERIMENT_ID = 'pay-payment-orchestration-routing-in-house';

export const ACQUIRER_COLORS = {
  JPMC: '#003087',
  ADYEN: '#0abf53',
  CHECKOUT: '#ff6b35',
  none: '#9e9e9e',
};

export const VARIANT_COLORS = {
  control: '#4285f4',
  test: '#fbbc04',
};

export const SIG_COLORS = {
  significant: '#ea4335',
  notSignificant: '#34a853',
  insufficientData: '#9e9e9e',
};

export const FRAUD_RESULT_ORDER = [
  'ACCEPT',
  'THREE_DS',
  'THREE_DS_EXEMPTION',
  'REFUSE',
  'NULL',
];

export const FRAUD_RESULT_COLORS = {
  ACCEPT: '#34a853',
  THREE_DS: '#fbbc04',
  THREE_DS_EXEMPTION: '#4285f4',
  REFUSE: '#ea4335',
  NULL: '#9e9e9e',
};

export const ROUTING_RULES = [
  { name: 'non-visa-mc-adyen-only', label: 'Non-Visa/MC (Adyen only)' },
  { name: 'adyen-primary-jpm-currency', label: 'Adyen primary + JPM currency (AU/CA/CH)' },
  { name: 'adyen-primary', label: 'Adyen primary (AU/CA/CH)' },
  { name: 'us-jpm-primary', label: 'US — JPM primary' },
  { name: 'us-no-jpm', label: 'US — no JPM' },
  { name: 'eu5-jpm-currency', label: 'EU5 + JPM currency' },
  { name: 'eu5', label: 'EU5' },
  { name: 'latam-plus-jpm-currency', label: 'LATAM+ JPM currency' },
  { name: 'latam-plus', label: 'LATAM+' },
  { name: 'catch-all-jpm-currency', label: 'Catch-all + JPM currency' },
  { name: 'catch-all', label: 'Catch-all' },
];

export const ACQUIRER_ORDER = ['JPMC', 'ADYEN', 'CHECKOUT', 'none'];

export const MIN_SAMPLE_SIZE = 100;

export const formatPct = (value) => {
  if (value == null || isNaN(value)) return '—';
  return `${(value * 100).toFixed(2)}%`;
};

export const formatNumber = (n) => {
  if (n == null) return '—';
  return n.toLocaleString();
};

export const formatPValue = (p) => {
  if (p == null) return '—';
  if (p < 0.001) return '< 0.001';
  return p.toFixed(4);
};
