import React from 'react';
import { MIN_SAMPLE_SIZE, formatPValue } from '../../../utils/constants';

export default function SignificanceBadge({ pValue, nControl, nTest, delta }) {
  if (nControl < MIN_SAMPLE_SIZE || nTest < MIN_SAMPLE_SIZE) {
    return (
      <span
        className="sig-badge"
        style={{ background: '#9e9e9e', color: '#fff' }}
        title="Fewer than 100 attempts in one variant"
      >
        n/a
      </span>
    );
  }

  if (pValue == null) {
    return <span className="sig-badge" style={{ background: '#eee', color: '#999' }}>—</span>;
  }

  const significant = pValue < 0.05;
  let background;
  if (!significant) {
    background = '#9e9e9e';
  } else if (delta != null && delta > 0) {
    background = '#34a853';
  } else if (delta != null && delta < 0) {
    background = '#ea4335';
  } else {
    background = '#fbbc04';
  }

  return (
    <span
      className="sig-badge"
      style={{ background, color: '#fff' }}
      title={`p = ${formatPValue(pValue)}`}
    >
      {significant ? 'Sig' : 'NS'}
    </span>
  );
}
