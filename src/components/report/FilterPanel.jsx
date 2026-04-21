import React, { useState } from 'react';
import { ROUTING_RULES } from '../../utils/constants';
import './FilterPanel.css';

export default function FilterPanel({ filters, dimensions, onChange }) {
  const [collapsed, setCollapsed] = useState(false);

  const toggleMulti = (key, value) => {
    const current = filters[key];
    const next = current.includes(value)
      ? current.filter((v) => v !== value)
      : [...current, value];
    onChange({ ...filters, [key]: next });
  };

  const setField = (key, value) => {
    onChange({ ...filters, [key]: value });
  };

  const resetAll = () => {
    onChange({
      countries: [],
      currencies: [],
      routingRules: [],
      paymentFlows: ['pay_now', 'rnpl_pay_early'],
      challengeIssued: [],
      initiatorType: 'CIT',
      firstAttemptOnly: true,
      systemAttemptRanks: [1],
    });
  };

  const showMITWarning = filters.initiatorType === 'ALL' || filters.initiatorType === 'MIT';

  return (
    <aside className={`filter-panel ${collapsed ? 'filter-panel--collapsed' : ''}`}>
      <div className="filter-panel__toggle" onClick={() => setCollapsed(!collapsed)}>
        <span className="filter-panel__toggle-icon">{collapsed ? '>' : '<'}</span>
        {!collapsed && <span className="filter-panel__toggle-label">Filters</span>}
      </div>

      {!collapsed && (
        <div className="filter-panel__body">
          <button className="filter-panel__reset" onClick={resetAll}>Reset all</button>

          {showMITWarning && (
            <div className="filter-panel__warning">
              MIT included — fraud NULL rates will be high (MIT has no Forter evaluation).
            </div>
          )}

          <FilterSection title="Payment Initiator">
            <div className="filter-panel__toggles">
              {['CIT', 'MIT', 'ALL'].map((val) => (
                <button
                  key={val}
                  className={`toggle-btn ${filters.initiatorType === val ? 'toggle-btn--active' : ''}`}
                  onClick={() => setField('initiatorType', val)}
                >
                  {val}
                </button>
              ))}
            </div>
          </FilterSection>

          <FilterSection title="Attempt Rank">
            <div className="filter-panel__toggles">
              <button
                className={`toggle-btn ${filters.firstAttemptOnly ? 'toggle-btn--active' : ''}`}
                onClick={() => setField('firstAttemptOnly', true)}
              >
                First only
              </button>
              <button
                className={`toggle-btn ${!filters.firstAttemptOnly ? 'toggle-btn--active' : ''}`}
                onClick={() => setField('firstAttemptOnly', false)}
              >
                All attempts
              </button>
            </div>
          </FilterSection>

          <MultiSelectSection
            title="Country"
            options={dimensions?.bin_issuer_country_code || []}
            selected={filters.countries}
            onToggle={(v) => toggleMulti('countries', v)}
            searchable
          />

          <MultiSelectSection
            title="Currency"
            options={dimensions?.currency || []}
            selected={filters.currencies}
            onToggle={(v) => toggleMulti('currencies', v)}
            searchable
          />

          <MultiSelectSection
            title="Routing Rule"
            options={(dimensions?.routing_rule || []).map((r) => r)}
            selected={filters.routingRules}
            onToggle={(v) => toggleMulti('routingRules', v)}
            labelFn={(name) => {
              const rule = ROUTING_RULES.find((r) => r.name === name);
              return rule ? rule.label : name;
            }}
          />

          <MultiSelectSection
            title="Payment Flow"
            options={dimensions?.payment_flow || []}
            selected={filters.paymentFlows}
            onToggle={(v) => toggleMulti('paymentFlows', v)}
          />

          <MultiSelectSection
            title="3DS Response"
            options={(dimensions?.challenge_issued || []).sort()}
            selected={filters.challengeIssued}
            onToggle={(v) => toggleMulti('challengeIssued', v)}
            labelFn={(v) => v === 'true' ? 'Challenged' : v === 'false' ? 'Frictionless' : v}
          />

          <MultiSelectSection
            title="System Attempt Rank"
            options={(dimensions?.system_attempt_rank || []).map(Number).sort((a, b) => a - b)}
            selected={filters.systemAttemptRanks}
            onToggle={(v) => toggleMulti('systemAttemptRanks', v)}
            labelFn={(v) => `Rank ${v}`}
          />
        </div>
      )}
    </aside>
  );
}

function FilterSection({ title, children }) {
  return (
    <div className="filter-section">
      <h4 className="filter-section__title">{title}</h4>
      {children}
    </div>
  );
}

function MultiSelectSection({ title, options, selected, onToggle, labelFn, searchable }) {
  const [expanded, setExpanded] = useState(false);
  const [search, setSearch] = useState('');
  const displayLimit = 8;

  const filtered = searchable && search
    ? options.filter((opt) => {
        const label = labelFn ? labelFn(opt) : opt;
        return label.toLowerCase().includes(search.toLowerCase());
      })
    : options;

  const selectedFirst = [...filtered].sort((a, b) => {
    const aSelected = selected.includes(a) ? 0 : 1;
    const bSelected = selected.includes(b) ? 0 : 1;
    return aSelected - bSelected;
  });

  const visible = expanded || search ? selectedFirst : selectedFirst.slice(0, displayLimit);
  const hasMore = !search && selectedFirst.length > displayLimit;

  return (
    <FilterSection title={`${title}${selected.length > 0 ? ` (${selected.length})` : ''}`}>
      <div className="multi-select">
        {searchable && (
          <input
            type="text"
            className="multi-select__search"
            placeholder={`Search ${title.toLowerCase()}...`}
            value={search}
            onChange={(e) => { setSearch(e.target.value); setExpanded(true); }}
          />
        )}
        {visible.map((opt) => (
          <label key={opt} className="multi-select__item">
            <input
              type="checkbox"
              checked={selected.includes(opt)}
              onChange={() => onToggle(opt)}
            />
            <span className="multi-select__label">{labelFn ? labelFn(opt) : opt}</span>
          </label>
        ))}
        {visible.length === 0 && search && (
          <span className="multi-select__empty">No matches</span>
        )}
        {hasMore && (
          <button className="multi-select__more" onClick={() => setExpanded(!expanded)}>
            {expanded ? 'Show less' : `+${selectedFirst.length - displayLimit} more`}
          </button>
        )}
      </div>
    </FilterSection>
  );
}
