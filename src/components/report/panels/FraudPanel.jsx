import React, { useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import PanelCard from './PanelCard';
import MetricTable from './MetricTable';
import SampleReferences from './SampleReferences';
import { groupAndSum, splitByVariant, sumMeasures, computeRate } from '../../../utils/aggregation';
import { FRAUD_RESULT_ORDER, FRAUD_RESULT_COLORS, formatPct } from '../../../utils/constants';
import { twoProportionZTest } from '../../../utils/stats';

export default function FraudPanel({ data, samples }) {
  const { chartData, tableRows } = useMemo(() => {
    const { control: allCtrl, test: allTest } = splitByVariant(data);
    const cAll = sumMeasures(allCtrl);
    const tAll = sumMeasures(allTest);

    const byFraud = groupAndSum(data, ['group_name', 'fraud_pre_auth_result']);
    const { control, test } = splitByVariant(byFraud);

    const results = FRAUD_RESULT_ORDER.filter((r) =>
      data.some((d) => d.fraud_pre_auth_result === r)
    );

    const chart = results.map((fr) => {
      const cRow = control.find((r) => r.fraud_pre_auth_result === fr);
      const tRow = test.find((r) => r.fraud_pre_auth_result === fr);
      const cAttempts = cRow ? cRow.attempts : 0;
      const tAttempts = tRow ? tRow.attempts : 0;
      return {
        result: fr,
        control: cAll.attempts > 0 ? cAttempts / cAll.attempts : 0,
        test: tAll.attempts > 0 ? tAttempts / tAll.attempts : 0,
        controlCount: cAttempts,
        testCount: tAttempts,
      };
    });

    const refuseCtrl = control.find((r) => r.fraud_pre_auth_result === 'REFUSE');
    const refuseTest = test.find((r) => r.fraud_pre_auth_result === 'REFUSE');
    const refuseZ = twoProportionZTest(
      refuseCtrl?.attempts || 0, cAll.attempts,
      refuseTest?.attempts || 0, tAll.attempts
    );

    const stiZ = twoProportionZTest(
      cAll.sent_to_issuer, cAll.attempts,
      tAll.sent_to_issuer, tAll.attempts
    );

    const rows = [
      ...results.map((fr) => {
        const cRow = control.find((r) => r.fraud_pre_auth_result === fr);
        const tRow = test.find((r) => r.fraud_pre_auth_result === fr);
        const cCount = cRow?.attempts || 0;
        const tCount = tRow?.attempts || 0;
        const z = twoProportionZTest(cCount, cAll.attempts, tCount, tAll.attempts);
        return {
          label: `${fr} share`,
          controlValue: computeRate(cCount, cAll.attempts),
          testValue: computeRate(tCount, tAll.attempts),
          delta: z.delta,
          pValue: z.pValue,
          nControl: cAll.attempts,
          nTest: tAll.attempts,
          isRate: true,
        };
      }),
      {
        label: 'REFUSE rate',
        controlValue: computeRate(refuseCtrl?.attempts || 0, cAll.attempts),
        testValue: computeRate(refuseTest?.attempts || 0, tAll.attempts),
        delta: refuseZ.delta,
        pValue: refuseZ.pValue,
        nControl: cAll.attempts,
        nTest: tAll.attempts,
        isRate: true,
      },
      {
        label: 'Sent-to-issuer rate',
        controlValue: computeRate(cAll.sent_to_issuer, cAll.attempts),
        testValue: computeRate(tAll.sent_to_issuer, tAll.attempts),
        delta: stiZ.delta,
        pValue: stiZ.pValue,
        nControl: cAll.attempts,
        nTest: tAll.attempts,
        isRate: true,
      },
    ];

    return { chartData: chart, tableRows: rows };
  }, [data]);

  return (
    <PanelCard title="Panel 3 — Fraud Performance" subtitle="Forter pre-auth result distribution">
      <div style={{ width: '100%', height: 280 }}>
        <ResponsiveContainer>
          <BarChart data={chartData} barCategoryGap="20%">
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis dataKey="result" tick={{ fontSize: 12 }} />
            <YAxis domain={[0, 'auto']} tickFormatter={(v) => formatPct(v)} tick={{ fontSize: 11 }} />
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
