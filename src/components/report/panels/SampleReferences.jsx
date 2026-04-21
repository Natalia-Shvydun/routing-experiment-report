import React, { useState, useMemo } from 'react';

export default function SampleReferences({ samples, filterFn }) {
  const [open, setOpen] = useState(false);

  const filtered = useMemo(() => {
    if (!samples || samples.length === 0) return { control: [], test: [] };
    const matched = filterFn ? samples.filter(filterFn) : samples;
    return {
      control: matched.filter((s) => s.group_name?.toLowerCase() === 'control'),
      test: matched.filter((s) => s.group_name?.toLowerCase() === 'test'),
    };
  }, [samples, filterFn]);

  const hasData = filtered.control.length > 0 || filtered.test.length > 0;
  if (!hasData) return null;

  return (
    <div style={{ marginTop: 12, borderTop: '1px solid #f0f0f0', paddingTop: 8 }}>
      <button
        onClick={() => setOpen(!open)}
        style={{
          background: 'none', border: 'none', cursor: 'pointer',
          fontSize: 12, color: '#4285f4', padding: 0,
        }}
      >
        {open ? '▾ Hide' : '▸ Show'} example references ({filtered.control.length} ctrl, {filtered.test.length} test)
      </button>
      {open && (
        <div style={{ display: 'flex', gap: 24, marginTop: 8, fontSize: 12 }}>
          <RefColumn label="Control" items={filtered.control} />
          <RefColumn label="Test" items={filtered.test} />
        </div>
      )}
    </div>
  );
}

function RefColumn({ label, items }) {
  if (items.length === 0) return null;
  return (
    <div style={{ flex: 1 }}>
      <div style={{ fontWeight: 600, marginBottom: 4, color: '#555' }}>{label}</div>
      <table style={{ width: '100%', fontSize: 11, borderCollapse: 'collapse' }}>
        <thead>
          <tr style={{ borderBottom: '1px solid #eee' }}>
            <th style={{ textAlign: 'left', padding: '2px 6px', color: '#888' }}>Reference</th>
            <th style={{ textAlign: 'left', padding: '2px 6px', color: '#888' }}>Processor</th>
            <th style={{ textAlign: 'left', padding: '2px 6px', color: '#888' }}>Fraud</th>
            <th style={{ textAlign: 'left', padding: '2px 6px', color: '#888' }}>3DS</th>
            <th style={{ textAlign: 'center', padding: '2px 6px', color: '#888' }}>OK</th>
          </tr>
        </thead>
        <tbody>
          {items.map((s, i) => (
            <tr key={i} style={{ borderBottom: '1px solid #f5f5f5' }}>
              <td style={{ padding: '3px 6px', fontFamily: 'monospace', userSelect: 'all' }}>
                {s.payment_provider_reference}
              </td>
              <td style={{ padding: '3px 6px' }}>{s.payment_processor}</td>
              <td style={{ padding: '3px 6px' }}>{s.fraud_pre_auth_result}</td>
              <td style={{ padding: '3px 6px' }}>{s.challenge_issued}</td>
              <td style={{ padding: '3px 6px', textAlign: 'center' }}>
                {s.is_customer_attempt_successful ? '✓' : '✗'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
