import React, { useMemo } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import PanelCard from './PanelCard';
import SignificanceBadge from './SignificanceBadge';
import SampleReferences from './SampleReferences';
import { groupAndSum, splitByVariant, sumMeasures, computeRate } from '../../../utils/aggregation';
import { ROUTING_RULES, formatNumber, formatPct } from '../../../utils/constants';
import { twoProportionZTest } from '../../../utils/stats';

const RULE_COLORS = [
  '#4285f4', '#ea4335', '#fbbc04', '#34a853', '#ff6d01',
  '#46bdc6', '#7b61ff', '#e8710a', '#1a73e8', '#d93025', '#188038',
];

export default function RoutingRulePanel({ data, samples }) {
  const { chartData, tableRows, controlTotal, testTotal, activeRules } = useMemo(() => {
    const byRule = groupAndSum(data, ['group_name', 'routing_rule']);
    const { control, test } = splitByVariant(byRule);
    const cTotal = sumMeasures(control).attempts;
    const tTotal = sumMeasures(test).attempts;

    const ruleOrder = ROUTING_RULES.map((r) => r.name);
    const rules = [...new Set(data.map((r) => r.routing_rule))].sort((a, b) => {
      const ai = ruleOrder.indexOf(a);
      const bi = ruleOrder.indexOf(b);
      return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
    });

    const chart = [
      { variant: 'Control', ...Object.fromEntries(rules.map((rule) => {
        const row = control.find((r) => r.routing_rule === rule) || { attempts: 0 };
        return [rule, cTotal > 0 ? (row.attempts / cTotal) * 100 : 0];
      }))},
      { variant: 'Test', ...Object.fromEntries(rules.map((rule) => {
        const row = test.find((r) => r.routing_rule === rule) || { attempts: 0 };
        return [rule, tTotal > 0 ? (row.attempts / tTotal) * 100 : 0];
      }))},
    ];

    const rows = rules.map((rule) => {
      const cRow = control.find((r) => r.routing_rule === rule) || { attempts: 0, successful: 0 };
      const tRow = test.find((r) => r.routing_rule === rule) || { attempts: 0, successful: 0 };
      const label = ROUTING_RULES.find((r) => r.name === rule)?.label || rule;
      const controlPct = cTotal > 0 ? cRow.attempts / cTotal : 0;
      const testPct = tTotal > 0 ? tRow.attempts / tTotal : 0;
      const splitZ = twoProportionZTest(
        Math.round(controlPct * cTotal), cTotal,
        Math.round(testPct * tTotal), tTotal
      );
      const srZ = twoProportionZTest(
        cRow.successful, cRow.attempts,
        tRow.successful, tRow.attempts
      );
      return {
        rule,
        fullLabel: label,
        controlAttempts: cRow.attempts,
        testAttempts: tRow.attempts,
        controlSR: computeRate(cRow.successful, cRow.attempts),
        testSR: computeRate(tRow.successful, tRow.attempts),
        controlPct,
        testPct,
        splitZ,
        srZ,
      };
    });

    return { chartData: chart, tableRows: rows, controlTotal: cTotal, testTotal: tTotal, activeRules: rules };
  }, [data]);

  return (
    <PanelCard title="Routing Rule Breakdown" subtitle="Attempt volume share and success rate by routing rule">
      <div style={{ width: '100%', height: 160 }}>
        <ResponsiveContainer>
          <BarChart data={chartData} layout="vertical" barCategoryGap="20%" margin={{ left: 60 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis type="number" tick={{ fontSize: 11 }} tickFormatter={(v) => `${v}%`} domain={[0, 100]} />
            <YAxis dataKey="variant" type="category" tick={{ fontSize: 12 }} width={60} />
            <Tooltip formatter={(v, name) => {
              const label = ROUTING_RULES.find((r) => r.name === name)?.label || name;
              return [`${v.toFixed(1)}%`, label];
            }} />
            <Legend formatter={(name) => {
              const label = ROUTING_RULES.find((r) => r.name === name)?.label || name;
              return label.length > 20 ? label.slice(0, 18) + '...' : label;
            }} />
            {activeRules.map((rule, i) => (
              <Bar key={rule} dataKey={rule} stackId="stack" fill={RULE_COLORS[i % RULE_COLORS.length]} name={rule} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div style={{ overflowX: 'auto', marginTop: 16 }}>
        <table className="metric-table">
          <thead>
            <tr>
              <th>Routing Rule</th>
              <th className="num">Ctrl #</th>
              <th className="num">Ctrl %</th>
              <th className="num">Test #</th>
              <th className="num">Test %</th>
              <th className="center">Split Sig</th>
              <th className="num">Ctrl SR</th>
              <th className="num">Test SR</th>
              <th className="num">SR Delta</th>
              <th className="center">SR Sig</th>
            </tr>
          </thead>
          <tbody>
            {tableRows.map((r) => (
              <tr key={r.rule}>
                <td title={r.fullLabel}>{r.fullLabel}</td>
                <td className="num">{formatNumber(r.controlAttempts)}</td>
                <td className="num">{formatPct(r.controlPct)}</td>
                <td className="num">{formatNumber(r.testAttempts)}</td>
                <td className="num">{formatPct(r.testPct)}</td>
                <td className="center">
                  <SignificanceBadge pValue={r.splitZ.pValue} nControl={controlTotal} nTest={testTotal} delta={r.splitZ.delta} />
                </td>
                <td className="num">{formatPct(r.controlSR)}</td>
                <td className="num">{formatPct(r.testSR)}</td>
                <td className="num">{formatDeltaPP(r.srZ.delta)}</td>
                <td className="center">
                  <SignificanceBadge pValue={r.srZ.pValue} nControl={r.controlAttempts} nTest={r.testAttempts} delta={r.srZ.delta} />
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
