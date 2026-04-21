import React, { useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import PanelCard from './PanelCard';
import MetricTable from './MetricTable';
import SampleReferences from './SampleReferences';
import { splitByVariant, sumMeasures, computeRate } from '../../../utils/aggregation';
import { formatPct } from '../../../utils/constants';
import { twoProportionZTest } from '../../../utils/stats';

export default function ThreeDSPanel({ data, samples }) {
  const { chartData, tableRows } = useMemo(() => {
    const allByVariant = splitByVariant(data);
    const cAll = sumMeasures(allByVariant.control);
    const tAll = sumMeasures(allByVariant.test);

    const threeDSData = data.filter((r) => r.fraud_pre_auth_result === 'THREE_DS');
    const { control: ctrl3DS, test: test3DS } = splitByVariant(threeDSData);
    const c3 = sumMeasures(ctrl3DS);
    const t3 = sumMeasures(test3DS);

    const triggerZ = twoProportionZTest(c3.attempts, cAll.attempts, t3.attempts, tAll.attempts);

    const challengedCtrl = ctrl3DS.filter((r) => r.challenge_issued === 'true');
    const challengedTest = test3DS.filter((r) => r.challenge_issued === 'true');
    const cChallenged = sumMeasures(challengedCtrl);
    const tChallenged = sumMeasures(challengedTest);

    const challengeZ = twoProportionZTest(cChallenged.attempts, c3.attempts, tChallenged.attempts, t3.attempts);

    const passZ = twoProportionZTest(c3.three_ds_passed, c3.attempts, t3.three_ds_passed, t3.attempts);

    const frictionlessCtrl = ctrl3DS.filter((r) => r.challenge_issued === 'false');
    const frictionlessTest = test3DS.filter((r) => r.challenge_issued === 'false');
    const cFric = sumMeasures(frictionlessCtrl);
    const tFric = sumMeasures(frictionlessTest);

    const fricSRz = twoProportionZTest(cFric.successful, cFric.attempts, tFric.successful, tFric.attempts);
    const chalSRz = twoProportionZTest(cChallenged.successful, cChallenged.attempts, tChallenged.successful, tChallenged.attempts);

    const fricShareZ = twoProportionZTest(cFric.attempts, cAll.attempts, tFric.attempts, tAll.attempts);
    const chalShareZ = twoProportionZTest(cChallenged.attempts, cAll.attempts, tChallenged.attempts, tAll.attempts);

    const chart = [
      {
        category: 'Frictionless',
        control: computeRate(cFric.successful, cFric.attempts),
        test: computeRate(tFric.successful, tFric.attempts),
      },
      {
        category: 'Challenged',
        control: computeRate(cChallenged.successful, cChallenged.attempts),
        test: computeRate(tChallenged.successful, tChallenged.attempts),
      },
    ];

    const rows = [
      {
        label: '3DS trigger rate',
        controlValue: computeRate(c3.attempts, cAll.attempts),
        testValue: computeRate(t3.attempts, tAll.attempts),
        delta: triggerZ.delta,
        pValue: triggerZ.pValue,
        nControl: cAll.attempts,
        nTest: tAll.attempts,
        isRate: true,
      },
      {
        label: 'Challenge rate (within 3DS)',
        controlValue: computeRate(cChallenged.attempts, c3.attempts),
        testValue: computeRate(tChallenged.attempts, t3.attempts),
        delta: challengeZ.delta,
        pValue: challengeZ.pValue,
        nControl: c3.attempts,
        nTest: t3.attempts,
        isRate: true,
      },
      {
        label: '3DS pass rate',
        controlValue: computeRate(c3.three_ds_passed, c3.attempts),
        testValue: computeRate(t3.three_ds_passed, t3.attempts),
        delta: passZ.delta,
        pValue: passZ.pValue,
        nControl: c3.attempts,
        nTest: t3.attempts,
        isRate: true,
      },
      {
        label: 'Frictionless share (of all attempts)',
        controlValue: computeRate(cFric.attempts, cAll.attempts),
        testValue: computeRate(tFric.attempts, tAll.attempts),
        delta: fricShareZ.delta,
        pValue: fricShareZ.pValue,
        nControl: cAll.attempts,
        nTest: tAll.attempts,
        isRate: true,
      },
      {
        label: 'SR — frictionless',
        controlValue: computeRate(cFric.successful, cFric.attempts),
        testValue: computeRate(tFric.successful, tFric.attempts),
        delta: fricSRz.delta,
        pValue: fricSRz.pValue,
        nControl: cFric.attempts,
        nTest: tFric.attempts,
        isRate: true,
      },
      {
        label: 'Challenged share (of all attempts)',
        controlValue: computeRate(cChallenged.attempts, cAll.attempts),
        testValue: computeRate(tChallenged.attempts, tAll.attempts),
        delta: chalShareZ.delta,
        pValue: chalShareZ.pValue,
        nControl: cAll.attempts,
        nTest: tAll.attempts,
        isRate: true,
      },
      {
        label: 'SR — challenged',
        controlValue: computeRate(cChallenged.successful, cChallenged.attempts),
        testValue: computeRate(tChallenged.successful, tChallenged.attempts),
        delta: chalSRz.delta,
        pValue: chalSRz.pValue,
        nControl: cChallenged.attempts,
        nTest: tChallenged.attempts,
        isRate: true,
      },
    ];

    return { chartData: chart, tableRows: rows };
  }, [data]);

  return (
    <PanelCard title="Panel 4 — 3DS Performance" subtitle="Challenge rates and success within 3DS-triggered attempts">
      <div style={{ width: '100%', height: 250 }}>
        <ResponsiveContainer>
          <BarChart data={chartData} barCategoryGap="20%">
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis dataKey="category" tick={{ fontSize: 12 }} />
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
