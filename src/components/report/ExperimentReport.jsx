import React, { useState, useMemo } from 'react';
import { useExperimentData } from '../../hooks/useExperimentData';
import { filterCube } from '../../utils/aggregation';
import HeaderBar from './HeaderBar';
import FilterPanel from './FilterPanel';
import KPISummaryPanel from './panels/KPISummaryPanel';
import AcquirerSplitPanel from './panels/AcquirerSplitPanel';
import RoutingRulePanel from './panels/RoutingRulePanel';
import CountrySRPanel from './panels/CountrySRPanel';
import SuccessRatePanel from './panels/SuccessRatePanel';
import FraudPanel from './panels/FraudPanel';
import ThreeDSPanel from './panels/ThreeDSPanel';
import SignificanceSummary from './panels/SignificanceSummary';
import './ExperimentReport.css';

const DEFAULT_FILTERS = {
  countries: [],
  currencies: [],
  routingRules: [],
  paymentFlows: ['pay_now', 'rnpl_pay_early'],
  challengeIssued: [],
  initiatorType: 'CIT',
  firstAttemptOnly: true,
  systemAttemptRanks: [1],
};

export default function ExperimentReport() {
  const { data, metadata, samples, loading, error } = useExperimentData();
  const [filters, setFilters] = useState(DEFAULT_FILTERS);

  const filteredData = useMemo(() => {
    if (!data) return [];
    return filterCube(data, filters);
  }, [data, filters]);

  if (loading) {
    return (
      <div className="report-loading">
        <div className="report-loading__spinner" />
        <p>Loading experiment data...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="report-error">
        <h2>Failed to load data</h2>
        <p>{error}</p>
        <p>Make sure <code>public/data.json</code> exists.</p>
      </div>
    );
  }

  return (
    <div className="report">
      <HeaderBar metadata={metadata} filteredData={filteredData} filters={filters} />
      <div className="report__body">
        <FilterPanel
          filters={filters}
          dimensions={metadata?.dimensions}
          onChange={setFilters}
        />
        <main className="report__panels">
          {filteredData.length === 0 ? (
            <div className="report__empty">
              <h3>No data matches the current filters</h3>
              <p>Try broadening your filter selection.</p>
            </div>
          ) : (
            <>
              <KPISummaryPanel data={filteredData} metadata={metadata} samples={samples} />
              <AcquirerSplitPanel data={filteredData} samples={samples} />
              <RoutingRulePanel data={filteredData} samples={samples} />
              <CountrySRPanel data={filteredData} samples={samples} />
              <SuccessRatePanel data={filteredData} samples={samples} />
              <FraudPanel data={filteredData} samples={samples} />
              <ThreeDSPanel data={filteredData} samples={samples} />
              <SignificanceSummary data={filteredData} samples={samples} />
            </>
          )}
        </main>
      </div>
    </div>
  );
}
