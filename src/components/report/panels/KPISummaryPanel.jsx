import React, { useMemo } from 'react';
import PanelCard from './PanelCard';
import SignificanceBadge from './SignificanceBadge';
import SampleReferences from './SampleReferences';
import { groupAndSum, splitByVariant, sumMeasures, computeRate } from '../../../utils/aggregation';
import { formatPct } from '../../../utils/constants';
import { twoProportionZTest } from '../../../utils/stats';

export default function KPISummaryPanel({ data, metadata, samples }) {
  const metrics = useMemo(() => {
    const byVariant = groupAndSum(data, ['group_name']);
    const { control, test } = splitByVariant(byVariant);
    const c = sumMeasures(control);
    const t = sumMeasures(test);

    const byFraud = groupAndSum(data, ['group_name', 'fraud_pre_auth_result']);
    const { control: cFraud, test: tFraud } = splitByVariant(byFraud);
    const fraudRate = (rows, result) => {
      const row = rows.find((r) => r.fraud_pre_auth_result === result);
      const total = rows.reduce((s, r) => s + (r.attempts || 0), 0);
      return { count: row?.attempts || 0, total };
    };

    const byChallenge = groupAndSum(data, ['group_name', 'challenge_issued']);
    const { control: cChal, test: tChal } = splitByVariant(byChallenge);
    const chalRate = (rows) => {
      const challenged = rows.find((r) => String(r.challenge_issued) === 'true');
      const total = rows.reduce((s, r) => s + (r.attempts || 0), 0);
      return { count: challenged?.attempts || 0, total };
    };

    const visitorSR = metadata?.visitor_sr;
    const cVis = visitorSR?.Control || visitorSR?.control;
    const tVis = visitorSR?.Test || visitorSR?.test;

    const rows = [];

    if (cVis && tVis) {
      const z = twoProportionZTest(cVis.successful_visitors, cVis.visitors, tVis.successful_visitors, tVis.visitors);
      rows.push({
        label: 'Visitor Success Rate',
        controlVal: computeRate(cVis.successful_visitors, cVis.visitors),
        testVal: computeRate(tVis.successful_visitors, tVis.visitors),
        controlN: cVis.visitors,
        testN: tVis.visitors,
        ...z,
      });
    }

    {
      const z = twoProportionZTest(c.successful, c.attempts, t.successful, t.attempts);
      rows.push({
        label: 'Attempt Success Rate',
        controlVal: computeRate(c.successful, c.attempts),
        testVal: computeRate(t.successful, t.attempts),
        controlN: c.attempts,
        testN: t.attempts,
        ...z,
      });
    }

    {
      const z = twoProportionZTest(c.sent_to_issuer, c.attempts, t.sent_to_issuer, t.attempts);
      rows.push({
        label: 'Sent-to-Issuer Rate',
        controlVal: computeRate(c.sent_to_issuer, c.attempts),
        testVal: computeRate(t.sent_to_issuer, t.attempts),
        controlN: c.attempts,
        testN: t.attempts,
        ...z,
      });
    }

    {
      const cA = fraudRate(cFraud, 'ACCEPT');
      const tA = fraudRate(tFraud, 'ACCEPT');
      const z = twoProportionZTest(cA.count, cA.total, tA.count, tA.total);
      rows.push({
        label: 'Fraud ACCEPT Rate',
        controlVal: computeRate(cA.count, cA.total),
        testVal: computeRate(tA.count, tA.total),
        controlN: cA.total,
        testN: tA.total,
        ...z,
      });
    }

    {
      const cR = fraudRate(cFraud, 'REFUSE');
      const tR = fraudRate(tFraud, 'REFUSE');
      const z = twoProportionZTest(cR.count, cR.total, tR.count, tR.total);
      rows.push({
        label: 'Fraud REFUSE Rate',
        controlVal: computeRate(cR.count, cR.total),
        testVal: computeRate(tR.count, tR.total),
        controlN: cR.total,
        testN: tR.total,
        ...z,
      });
    }

    {
      const cR = fraudRate(cFraud, 'THREE_DS');
      const tR = fraudRate(tFraud, 'THREE_DS');
      const z = twoProportionZTest(cR.count, cR.total, tR.count, tR.total);
      rows.push({
        label: 'Fraud THREE_DS Rate',
        controlVal: computeRate(cR.count, cR.total),
        testVal: computeRate(tR.count, tR.total),
        controlN: cR.total,
        testN: tR.total,
        ...z,
      });
    }

    {
      const cC = chalRate(cChal);
      const tC = chalRate(tChal);
      const z = twoProportionZTest(cC.count, cC.total, tC.count, tC.total);
      rows.push({
        label: '3DS Challenge Rate',
        controlVal: computeRate(cC.count, cC.total),
        testVal: computeRate(tC.count, tC.total),
        controlN: cC.total,
        testN: tC.total,
        ...z,
      });
    }

    {
      const z = twoProportionZTest(c.three_ds_passed, c.attempts, t.three_ds_passed, t.attempts);
      rows.push({
        label: '3DS Pass Rate',
        controlVal: computeRate(c.three_ds_passed, c.attempts),
        testVal: computeRate(t.three_ds_passed, t.attempts),
        controlN: c.attempts,
        testN: t.attempts,
        ...z,
      });
    }

    return rows;
  }, [data, metadata]);

  return (
    <PanelCard title="Key Metrics Summary" subtitle="Top-level KPIs — control vs test with statistical significance">
      <div style={{ overflowX: 'auto' }}>
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
            {metrics.map((m) => (
              <tr key={m.label} style={m.significant ? { background: '#fef0f0' } : undefined}>
                <td style={{ fontWeight: 500 }}>{m.label}</td>
                <td className="num">{formatPct(m.controlVal)}</td>
                <td className="num">{formatPct(m.testVal)}</td>
                <td className="num">{formatDeltaPP(m.delta)}</td>
                <td className="center">
                  <SignificanceBadge pValue={m.pValue} nControl={m.controlN} nTest={m.testN} delta={m.delta} />
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
