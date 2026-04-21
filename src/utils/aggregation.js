const MEASURE_KEYS = ['attempts', 'successful', 'sent_to_issuer', 'three_ds_passed'];

export function filterCube(data, filters) {
  return data.filter((row) => {
    if (filters.countries.length > 0 && !filters.countries.includes(row.bin_issuer_country_code)) {
      return false;
    }
    if (filters.currencies.length > 0 && !filters.currencies.includes(row.currency)) {
      return false;
    }
    if (filters.routingRules.length > 0 && !filters.routingRules.includes(row.routing_rule)) {
      return false;
    }
    if (filters.paymentFlows.length > 0 && !filters.paymentFlows.includes(row.payment_flow)) {
      return false;
    }
    if (filters.initiatorType !== 'ALL' && row.payment_initiator_type !== filters.initiatorType) {
      return false;
    }
    if (filters.firstAttemptOnly && !row.is_first_attempt) {
      return false;
    }
    if (filters.challengeIssued?.length > 0 && !filters.challengeIssued.includes(row.challenge_issued)) {
      return false;
    }
    if (filters.systemAttemptRanks?.length > 0 && !filters.systemAttemptRanks.includes(row.system_attempt_rank)) {
      return false;
    }
    return true;
  });
}

export function groupAndSum(rows, groupKeys, measureKeys = MEASURE_KEYS) {
  const map = new Map();

  for (const row of rows) {
    const key = groupKeys.map((k) => row[k] ?? 'null').join('|');
    if (!map.has(key)) {
      const group = {};
      for (const k of groupKeys) group[k] = row[k];
      for (const m of measureKeys) group[m] = 0;
      map.set(key, group);
    }
    const group = map.get(key);
    for (const m of measureKeys) {
      group[m] += row[m] || 0;
    }
  }

  return Array.from(map.values());
}

export function sumMeasures(rows) {
  const result = { attempts: 0, successful: 0, sent_to_issuer: 0, three_ds_passed: 0 };
  for (const row of rows) {
    result.attempts += row.attempts || 0;
    result.successful += row.successful || 0;
    result.sent_to_issuer += row.sent_to_issuer || 0;
    result.three_ds_passed += row.three_ds_passed || 0;
  }
  return result;
}

export function splitByVariant(rows) {
  const control = rows.filter((r) => r.group_name?.toLowerCase() === 'control');
  const test = rows.filter((r) => r.group_name?.toLowerCase() === 'test');
  return { control, test };
}

export function computeRate(numerator, denominator) {
  if (!denominator || denominator === 0) return null;
  return numerator / denominator;
}
