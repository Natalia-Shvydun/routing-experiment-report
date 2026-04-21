import React, { useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import PanelCard from './PanelCard';
import MetricTable from './MetricTable';
import SampleReferences from './SampleReferences';
import { groupAndSum, splitByVariant, sumMeasures, computeRate } from '../../../utils/aggregation';
import { ACQUIRER_ORDER, formatPct } from '../../../utils/constants';
import { twoProportionZTest } from '../../../utils/stats';

export default function SuccessRatePanel({ data, samples }) {
  const { chartData, tableRows } = useMemo(() => {
    const { control: allCtrl, test: allTest } = splitByVariant(data);
    const cAll = sumMeasures(allCtrl);
    const tAll = sumMeasures(allTest);
    const overallZ = twoProportionZTest(cAll.successful, cAll.attempts, tAll.successful, tAll.attempts);

    const byAcquirer = groupAndSum(data, ['group_name', 'payment_processor']);
    const { control, test } = splitByVariant(byAcquirer);

    const acquirers = [...new Set(data.map((r) => r.payment_processor))].sort((a, b) => {
      const ai = ACQUIRER_ORDER.indexOf(a);
      const bi = ACQUIRER_ORDER.indexOf(b);
      return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
    });

    const chart = acquirers.map((acq) => {
      const cRow = control.find((r) => r.payment_processor === acq) || { successful: 0, attempts: 0 };
      const tRow = test.find((r) => r.payment_processor === acq) || { successful: 0, attempts: 0 };
      return {
        acquirer: acq,
        control: computeRate(cRow.successful, cRow.attempts),
        test: computeRate(tRow.successful, tRow.attempts),
      };
    });

    const rows = [
      {
        label: 'Overall SR',
        controlValue: computeRate(cAll.successful, cAll.attempts),
        testValue: computeRate(tAll.successful, tAll.attempts),
        delta: overallZ.delta,
        pValue: overallZ.pValue,
        nControl: cAll.attempts,
        nTest: tAll.attempts,
        isRate: true,
      },
      ...acquirers.map((acq) => {
        const cRow = control.find((r) => r.payment_processor === acq) || { successful: 0, attempts: 0 };
        const tRow = test.find((r) => r.payment_processor === acq) || { successful: 0, attempts: 0 };
        const z = twoProportionZTest(cRow.successful, cRow.attempts, tRow.successful, tRow.attempts);
        return {
          label: `SR — ${acq}`,
          controlValue: computeRate(cRow.successful, cRow.attempts),
          testValue: computeRate(tRow.successful, tRow.attempts),
          delta: z.delta,
          pValue: z.pValue,
          nControl: cRow.attempts,
          nTest: tRow.attempts,
          isRate: true,
        };
      }),
    ];

    return { chartData: chart, tableRows: rows };
  }, [data]);

  return (
    <PanelCard title="Panel 2 — Attempt Success Rate" subtitle="Success rate by acquirer, control vs test">
      <div style={{ width: '100%', height: 280 }}>
        <ResponsiveContainer>
          <BarChart data={chartData} barCategoryGap="20%">
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis dataKey="acquirer" tick={{ fontSize: 12 }} />
            <YAxis domain={[0, 1]} tickFormatter={(v) => formatPct(v)} tick={{ fontSize: 11 }} />
            <Tooltip formatter={(v) => formatPct(v)} />
            <Legend />
            <Bar dataKey="control" fill="#4285f4" name="Control" />
            <Bar dataKey="test" fill="#fbbc04" name="Test" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <MetricTable rows={tableRows} />
      <SampleReferences samples={samples} />
    </PanelCard>
  );
}
