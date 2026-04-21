import React from 'react';
import { formatNumber } from '../../utils/constants';
import { sumMeasures, splitByVariant } from '../../utils/aggregation';
import './HeaderBar.css';

export default function HeaderBar({ metadata, filteredData, filters }) {
  const { control, test } = splitByVariant(filteredData);
  const controlTotals = sumMeasures(control);
  const testTotals = sumMeasures(test);

  const activeFilters = [];
  if (filters.countries.length > 0) activeFilters.push(`Country: ${filters.countries.join(', ')}`);
  if (filters.currencies.length > 0) activeFilters.push(`Currency: ${filters.currencies.join(', ')}`);
  if (filters.routingRules.length > 0) activeFilters.push(`Rule: ${filters.routingRules.join(', ')}`);
  if (filters.paymentFlows.length > 0) activeFilters.push(`Flow: ${filters.paymentFlows.join(', ')}`);
  if (filters.initiatorType !== 'CIT') activeFilters.push(`Initiator: ${filters.initiatorType}`);
  if (!filters.firstAttemptOnly) activeFilters.push('All attempts');

  return (
    <header className="header-bar">
      <div className="header-bar__left">
        <h1 className="header-bar__title">Routing Experiment Deep Dive</h1>
        <div className="header-bar__meta">
          <span className="header-bar__tag">{metadata?.experiment_id}</span>
          <span className="header-bar__separator" />
          <span>From {metadata?.assignment_start}</span>
          <span className="header-bar__separator" />
          <span>Data as of {metadata?.data_as_of}</span>
        </div>
      </div>
      <div className="header-bar__right">
        <div className="header-bar__counts">
          <div className="header-bar__count">
            <span className="header-bar__count-label">Control</span>
            <span className="header-bar__count-value">{formatNumber(controlTotals.attempts)}</span>
          </div>
          <div className="header-bar__count">
            <span className="header-bar__count-label">Test</span>
            <span className="header-bar__count-value">{formatNumber(testTotals.attempts)}</span>
          </div>
        </div>
      </div>
      {activeFilters.length > 0 && (
        <div className="header-bar__filters">
          {activeFilters.map((f, i) => (
            <span key={i} className="header-bar__chip">{f}</span>
          ))}
        </div>
      )}
    </header>
  );
}
