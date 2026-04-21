import React from 'react'
import './DashboardHeader.css'

function DashboardHeader() {
  return (
    <div className="dashboard-header">
      <div className="header-top">
        <span className="owner">Anni Hellsten</span>
        <div className="header-actions">
          <span className="refresh-time">just now</span>
          <button className="icon-button">🔄</button>
          <button className="icon-button">☰</button>
        </div>
      </div>
      <div className="header-title">
        <h1>Discovery Cockpit</h1>
        <div className="title-actions">
          <button className="icon-button">❤️</button>
          <button className="icon-button">🔗</button>
          <button className="icon-button">🔄</button>
        </div>
      </div>
    </div>
  )
}

export default DashboardHeader
