import React from 'react';
import SignificanceBadge from './SignificanceBadge';
import { formatPct, formatNumber } from '../../../utils/constants';
import './MetricTable.css';

export default function MetricTable({ rows }) {
  if (!rows || rows.length === 0) return <p className="metric-table__empty">No data</p>;

  return (
    <div className="metric-table__wrapper">
      <table className="metric-table">
        <thead>
          <tr>
            <th>Metric</th>
            <th className="num">Control</th>
            <th className="num">Test</th>
            <th className="num">Delta</th>
            <th className="center">Sig</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              <td>{row.label}</td>
              <td className="num">
                {row.isRate ? formatPct(row.controlValue) : formatNumber(row.controlValue)}
              </td>
              <td className="num">
                {row.isRate ? formatPct(row.testValue) : formatNumber(row.testValue)}
              </td>
              <td className="num">
                {row.isRate ? formatDelta(row.delta) : formatNumber(row.delta)}
              </td>
              <td className="center">
                <SignificanceBadge
                  pValue={row.pValue}
                  nControl={row.nControl}
                  nTest={row.nTest}
                  delta={row.delta}
                />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function formatDelta(d) {
  if (d == null || isNaN(d)) return '—';
  const sign = d > 0 ? '+' : '';
  return `${sign}${(d * 100).toFixed(2)}pp`;
}
