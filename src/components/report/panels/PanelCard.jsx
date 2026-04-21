import React from 'react';
import './PanelCard.css';

export default function PanelCard({ title, subtitle, children }) {
  return (
    <div className="panel-card">
      <div className="panel-card__header">
        <h2 className="panel-card__title">{title}</h2>
        {subtitle && <p className="panel-card__subtitle">{subtitle}</p>}
      </div>
      <div className="panel-card__body">{children}</div>
    </div>
  );
}
