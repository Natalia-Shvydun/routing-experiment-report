import React, { useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import PanelCard from './PanelCard';
import { groupAndSum, splitByVariant, sumMeasures } from '../../../utils/aggregation';
import { ACQUIRER_COLORS, ACQUIRER_ORDER, formatNumber, formatPct } from '../../../utils/constants';
import { twoProportionZTest } from '../../../utils/stats';
import SignificanceBadge from './SignificanceBadge';
import SampleReferences from './SampleReferences';

export default function AcquirerSplitPanel({ data, samples }) {
  const { chartData, tableRows, controlTotal, testTotal } = useMemo(() => {
    const byAcquirer = groupAndSum(data, ['group_name', 'payment_processor']);
    const { control, test } = splitByVariant(byAcquirer);

    const cTotal = sumMeasures(control).attempts;
    const tTotal = sumMeasures(test).attempts;

    const acquirers = [...new Set(data.map((r) => r.payment_processor))].sort((a, b) => {
      const ai = ACQUIRER_ORDER.indexOf(a);
      const bi = ACQUIRER_ORDER.indexOf(b);
      return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
    });

    const chart = [
      { variant: 'Control', ...Object.fromEntries(acquirers.map((acq) => {
        const row = control.find((r) => r.payment_processor === acq);
        return [acq, cTotal > 0 && row ? (row.attempts / cTotal) * 100 : 0];
      }))},
      { variant: 'Test', ...Object.fromEntries(acquirers.map((acq) => {
        const row = test.find((r) => r.payment_processor === acq);
        return [acq, tTotal > 0 && row ? (row.attempts / tTotal) * 100 : 0];
      }))},
    ];

    const rows = acquirers.map((acq) => {
      const cRow = control.find((r) => r.payment_processor === acq);
      const tRow = test.find((r) => r.payment_processor === acq);
      const cAttempts = cRow ? cRow.attempts : 0;
      const tAttempts = tRow ? tRow.attempts : 0;
      const controlPct = cTotal > 0 ? cAttempts / cTotal : 0;
      const testPct = tTotal > 0 ? tAttempts / tTotal : 0;
      const zResult = twoProportionZTest(
        Math.round(controlPct * cTotal), cTotal,
        Math.round(testPct * tTotal), tTotal
      );
      return {
        acquirer: acq,
        control: cAttempts,
        test: tAttempts,
        controlPct,
        testPct,
        ...zResult,
        nControl: cTotal,
        nTest: tTotal,
      };
    });

    return { chartData: chart, tableRows: rows, controlTotal: cTotal, testTotal: tTotal, acquirers };
  }, [data]);

  const acquirers = useMemo(() => {
    return [...new Set(data.map((r) => r.payment_processor))].sort((a, b) => {
      const ai = ACQUIRER_ORDER.indexOf(a);
      const bi = ACQUIRER_ORDER.indexOf(b);
      return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
    });
  }, [data]);

  return (
    <PanelCard title="Panel 1 — Acquirer Split" subtitle="Volume distribution by payment processor">
      <div style={{ width: '100%', height: 160 }}>
        <ResponsiveContainer>
          <BarChart data={chartData} layout="vertical" barCategoryGap="20%" margin={{ left: 60 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis type="number" tick={{ fontSize: 11 }} tickFormatter={(v) => `${v}%`} domain={[0, 100]} />
            <YAxis dataKey="variant" type="category" tick={{ fontSize: 12 }} width={60} />
            <Tooltip formatter={(v, name) => [`${v.toFixed(1)}%`, name]} />
            <Legend />
            {acquirers.map((acq) => (
              <Bar key={acq} dataKey={acq} stackId="stack" fill={ACQUIRER_COLORS[acq] || '#ccc'} name={acq} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div style={{ overflowX: 'auto', marginTop: 16 }}>
        <table className="metric-table">
          <thead>
            <tr>
              <th>Acquirer</th>
              <th className="num">Control #</th>
              <th className="num">Control %</th>
              <th className="num">Test #</th>
              <th className="num">Test %</th>
              <th className="num">Delta %</th>
              <th className="center">Sig</th>
            </tr>
          </thead>
          <tbody>
            {tableRows.map((r) => (
              <tr key={r.acquirer}>
                <td>
                  <span
                    style={{
                      display: 'inline-block',
                      width: 10,
                      height: 10,
                      borderRadius: 2,
                      background: ACQUIRER_COLORS[r.acquirer] || '#ccc',
                      marginRight: 6,
                      verticalAlign: 'middle',
                    }}
                  />
                  {r.acquirer}
                </td>
                <td className="num">{formatNumber(r.control)}</td>
                <td className="num">{formatPct(r.controlPct)}</td>
                <td className="num">{formatNumber(r.test)}</td>
                <td className="num">{formatPct(r.testPct)}</td>
                <td className="num">{formatDeltaPP(r.delta)}</td>
                <td className="center">
                  <SignificanceBadge pValue={r.pValue} nControl={r.nControl} nTest={r.nTest} delta={r.delta} />
                </td>
              </tr>
            ))}
            <tr style={{ fontWeight: 600, borderTop: '2px solid #ddd' }}>
              <td>Total</td>
              <td className="num">{formatNumber(controlTotal)}</td>
              <td className="num">100%</td>
              <td className="num">{formatNumber(testTotal)}</td>
              <td className="num">100%</td>
              <td className="num" />
              <td />
            </tr>
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
