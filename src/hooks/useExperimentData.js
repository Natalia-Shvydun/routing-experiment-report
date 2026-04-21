import { useState, useEffect } from 'react';

export function useExperimentData() {
  const [data, setData] = useState(null);
  const [metadata, setMetadata] = useState(null);
  const [samples, setSamples] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    let cancelled = false;

    fetch(import.meta.env.BASE_URL + 'data.json')
      .then((res) => {
        if (!res.ok) throw new Error(`Failed to load data.json: ${res.status}`);
        return res.json();
      })
      .then((json) => {
        if (cancelled) return;
        const meta = json.metadata || {};
        const dims = meta.dimensions || {};
        const derivable = [
          'bin_issuer_country_code', 'currency', 'routing_rule',
          'payment_initiator_type', 'payment_flow', 'challenge_issued',
          'system_attempt_rank',
        ];
        for (const dim of derivable) {
          if (!dims[dim] || dims[dim].length === 0) {
            dims[dim] = [...new Set(json.data.map((r) => r[dim]).filter((v) => v != null))].sort();
          }
        }
        meta.dimensions = dims;
        setData(json.data);
        setMetadata(meta);
        setSamples(json.samples || []);
        setLoading(false);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err.message);
        setLoading(false);
      });

    return () => { cancelled = true; };
  }, []);

  return { data, metadata, samples, loading, error };
}
