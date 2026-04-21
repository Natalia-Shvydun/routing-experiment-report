import React, { useMemo } from 'react';
import PanelCard from './PanelCard';
import SignificanceBadge from './SignificanceBadge';
import SampleReferences from './SampleReferences';
import { splitByVariant, sumMeasures, computeRate } from '../../../utils/aggregation';
import { formatPct, formatPValue, MIN_SAMPLE_SIZE } from '../../../utils/constants';
import { twoProportionZTest } from '../../../utils/stats';

export default function SignificanceSummary({ data, samples }) {
  const rows = useMemo(() => {
    const { control: allCtrl, test: allTest } = splitByVariant(data);
    const cAll = sumMeasures(allCtrl);
    const tAll = sumMeasures(allTest);

    const threeDSData = data.filter((r) => r.fraud_pre_auth_result === 'THREE_DS');
    const { control: ctrl3DS, test: test3DS } = splitByVariant(threeDSData);
    const c3 = sumMeasures(ctrl3DS);
    const t3 = sumMeasures(test3DS);

    const challengedCtrl = ctrl3DS.filter((r) => r.challenge_issued === 'true');
    const challengedTest = test3DS.filter((r) => r.challenge_issued === 'true');
    const cChal = sumMeasures(challengedCtrl);
    const tChal = sumMeasures(challengedTest);

    const refuseCtrl = allCtrl.filter((r) => r.fraud_pre_auth_result === 'REFUSE');
    const refuseTest = allTest.filter((r) => r.fraud_pre_auth_result === 'REFUSE');
    const cRefuse = sumMeasures(refuseCtrl);
    const tRefuse = sumMeasures(refuseTest);

    const metrics = [
      {
        name: 'Overall SR',
        cNum: cAll.successful, cDen: cAll.attempts,
        tNum: tAll.successful, tDen: tAll.attempts,
      },
      {
        name: 'Sent-to-issuer rate',
        cNum: cAll.sent_to_issuer, cDen: cAll.attempts,
        tNum: tAll.sent_to_issuer, tDen: tAll.attempts,
      },
      {
        name: 'REFUSE rate',
        cNum: cRefuse.attempts, cDen: cAll.attempts,
        tNum: tRefuse.attempts, tDen: tAll.attempts,
      },
      {
        name: '3DS trigger rate',
        cNum: c3.attempts, cDen: cAll.attempts,
        tNum: t3.attempts, tDen: tAll.attempts,
      },
      {
        name: 'Challenge rate (3DS)',
        cNum: cChal.attempts, cDen: c3.attempts,
        tNum: tChal.attempts, tDen: t3.attempts,
      },
      {
        name: '3DS pass rate',
        cNum: c3.three_ds_passed, cDen: c3.attempts,
        tNum: t3.three_ds_passed, tDen: t3.attempts,
      },
    ];

    return metrics.map((m) => {
      const z = twoProportionZTest(m.cNum, m.cDen, m.tNum, m.tDen);
      return {
        name: m.name,
        controlValue: computeRate(m.cNum, m.cDen),
        testValue: computeRate(m.tNum, m.tDen),
        ...z,
        nControl: m.cDen,
        nTest: m.tDen,
      };
    });
  }, [data]);

  return (
    <PanelCard title="Panel 5 — Statistical Significance Summary" subtitle="Two-proportion z-test for all key metrics">
      <div style={{ overflowX: 'auto' }}>
        <table className="metric-table">
          <thead>
            <tr>
              <th>Metric</th>
              <th className="num">Control</th>
              <th className="num">Test</th>
              <th className="num">Delta</th>
              <th className="num">z-score</th>
              <th className="num">p-value</th>
              <th className="center">Result</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const insufficient = r.nControl < MIN_SAMPLE_SIZE || r.nTest < MIN_SAMPLE_SIZE;
              return (
                <tr key={r.name} style={r.significant ? { background: '#fef0f0' } : undefined}>
                  <td>{r.name}</td>
                  <td className="num">{formatPct(r.controlValue)}</td>
                  <td className="num">{formatPct(r.testValue)}</td>
                  <td className="num">{formatDeltaPP(r.delta)}</td>
                  <td className="num">{insufficient ? '—' : (r.zScore?.toFixed(3) ?? '—')}</td>
                  <td className="num">{insufficient ? '—' : formatPValue(r.pValue)}</td>
                  <td className="center">
                    <SignificanceBadge pValue={r.pValue} nControl={r.nControl} nTest={r.nTest} delta={r.delta} />
                  </td>
                </tr>
              );
            })}
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
