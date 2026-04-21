import React from 'react'
import ApVChart from './ApVChart'
import NarrativeBox from './NarrativeBox'
import './DiscoverSection.css'

function DiscoverSection() {
  return (
    <div className="discover-section">
      <div className="section-header">
        <h2>Discover</h2>
        <div className="section-subtitle">
          <h3>Average ADP views per visitor (ApV)</h3>
          <p>Average number of Activity Detail pages seen by visitors per week. Last 12 weeks vs the same period last year.</p>
        </div>
      </div>
      <div className="discover-content">
        <div className="chart-container">
          <ApVChart />
        </div>
        <NarrativeBox />
      </div>
    </div>
  )
}

export default DiscoverSection
