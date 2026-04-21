function normalCDF(x) {
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const p = 0.3275911;

  const sign = x < 0 ? -1 : 1;
  const absX = Math.abs(x);
  const t = 1.0 / (1.0 + p * absX);
  const y = 1.0 - ((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * Math.exp(-absX * absX / 2);

  return 0.5 * (1.0 + sign * y);
}

export function twoProportionZTest(successes1, n1, successes2, n2) {
  if (n1 === 0 || n2 === 0) {
    return { delta: null, zScore: null, pValue: null, significant: false };
  }

  const p1 = successes1 / n1;
  const p2 = successes2 / n2;
  const delta = p2 - p1;

  const pooled = (successes1 + successes2) / (n1 + n2);
  if (pooled === 0 || pooled === 1) {
    return { delta, zScore: null, pValue: null, significant: false };
  }

  const se = Math.sqrt(pooled * (1 - pooled) * (1 / n1 + 1 / n2));
  const zScore = delta / se;
  const pValue = 2 * (1 - normalCDF(Math.abs(zScore)));

  return {
    delta,
    zScore: Math.round(zScore * 1000) / 1000,
    pValue: Math.round(pValue * 10000) / 10000,
    significant: pValue < 0.05,
  };
}
