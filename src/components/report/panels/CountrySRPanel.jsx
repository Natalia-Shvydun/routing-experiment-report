import React, { useMemo, useState } from 'react';
import PanelCard from './PanelCard';
import SignificanceBadge from './SignificanceBadge';
import SampleReferences from './SampleReferences';
import { groupAndSum, splitByVariant, computeRate } from '../../../utils/aggregation';
import { formatNumber, formatPct } from '../../../utils/constants';
import { twoProportionZTest } from '../../../utils/stats';

export default function CountrySRPanel({ data, samples }) {
  const [sortBy, setSortBy] = useState('controlAttempts');
  const [sortDir, setSortDir] = useState('desc');
  const [sigOnly, setSigOnly] = useState(false);

  const tableRows = useMemo(() => {
    const byCountry = groupAndSum(data, ['group_name', 'bin_issuer_country_code']);
    const { control, test } = splitByVariant(byCountry);

    const countries = [...new Set(data.map((r) => r.bin_issuer_country_code))];

    return countries.map((cc) => {
      const cRow = control.find((r) => r.bin_issuer_country_code === cc) || { attempts: 0, successful: 0 };
      const tRow = test.find((r) => r.bin_issuer_country_code === cc) || { attempts: 0, successful: 0 };
      const z = twoProportionZTest(cRow.successful, cRow.attempts, tRow.successful, tRow.attempts);
      return {
        country: cc,
        controlAttempts: cRow.attempts,
        testAttempts: tRow.attempts,
        controlSR: computeRate(cRow.successful, cRow.attempts),
        testSR: computeRate(tRow.successful, tRow.attempts),
        ...z,
        nControl: cRow.attempts,
        nTest: tRow.attempts,
      };
    });
  }, [data]);

  const filtered = useMemo(() => {
    if (!sigOnly) return tableRows;
    return tableRows.filter((r) => r.significant);
  }, [tableRows, sigOnly]);

  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      const av = a[sortBy] ?? -1;
      const bv = b[sortBy] ?? -1;
      return sortDir === 'desc' ? bv - av : av - bv;
    });
  }, [filtered, sortBy, sortDir]);

  const toggleSort = (col) => {
    if (sortBy === col) {
      setSortDir(sortDir === 'desc' ? 'asc' : 'desc');
    } else {
      setSortBy(col);
      setSortDir('desc');
    }
  };

  const sortIcon = (col) => {
    if (sortBy !== col) return '';
    return sortDir === 'desc' ? ' ↓' : ' ↑';
  };

  return (
    <PanelCard title="Country-Level Success Rate" subtitle="Attempt SR by issuer country, control vs test">
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 10 }}>
        <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13, cursor: 'pointer' }}>
          <input type="checkbox" checked={sigOnly} onChange={(e) => setSigOnly(e.target.checked)} />
          Significant only
        </label>
        <span style={{ fontSize: 12, color: '#888' }}>
          {sorted.length} of {tableRows.length} countries
        </span>
      </div>
      <div style={{ overflowX: 'auto', maxHeight: 500, overflowY: 'auto' }}>
        <table className="metric-table">
          <thead>
            <tr>
              <th style={{ cursor: 'pointer' }} onClick={() => toggleSort('country')}>
                Country{sortIcon('country')}
              </th>
              <th className="num" style={{ cursor: 'pointer' }} onClick={() => toggleSort('controlAttempts')}>
                Ctrl #{sortIcon('controlAttempts')}
              </th>
              <th className="num" style={{ cursor: 'pointer' }} onClick={() => toggleSort('testAttempts')}>
                Test #{sortIcon('testAttempts')}
              </th>
              <th className="num" style={{ cursor: 'pointer' }} onClick={() => toggleSort('controlSR')}>
                Ctrl SR{sortIcon('controlSR')}
              </th>
              <th className="num" style={{ cursor: 'pointer' }} onClick={() => toggleSort('testSR')}>
                Test SR{sortIcon('testSR')}
              </th>
              <th className="num" style={{ cursor: 'pointer' }} onClick={() => toggleSort('delta')}>
                Delta{sortIcon('delta')}
              </th>
              <th className="center">Sig</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r) => (
              <tr key={r.country} style={r.significant ? { background: '#fef0f0' } : undefined}>
                <td>{r.country}</td>
                <td className="num">{formatNumber(r.controlAttempts)}</td>
                <td className="num">{formatNumber(r.testAttempts)}</td>
                <td className="num">{formatPct(r.controlSR)}</td>
                <td className="num">{formatPct(r.testSR)}</td>
                <td className="num">{formatDeltaPP(r.delta)}</td>
                <td className="center">
                  <SignificanceBadge pValue={r.pValue} nControl={r.nControl} nTest={r.nTest} delta={r.delta} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <SampleReferences samples={samples} />
    </PanelCard>
  );
}

function formatDeltaPP(d) {
  if (d == null || isNaN(d)) return '—';
  const sign = d > 0 ? '+' : '';
  return `${sign}${(d * 100).toFixed(2)}pp`;
}
