import React from 'react'
import './ChannelTable.css'

// Data from the dashboard image
const channelData = [
  {
    channel: 'paid_search',
    lastYearApV: 1.4,
    thisYearApV: 1.6,
    apvChange: 12,
    lastYearVisitors: 36069806,
    lastYearPercent: 29,
    thisYearVisitors: 38234153,
    thisYearPercent: 24,
    mixChange: -19
  },
  {
    channel: 'paid_social',
    lastYearApV: 0.8,
    thisYearApV: 0.4,
    apvChange: -56,
    lastYearVisitors: 3178106,
    lastYearPercent: 3,
    thisYearVisitors: 24878495,
    thisYearPercent: 15,
    mixChange: 496
  },
  {
    channel: 'paid_search_ttd',
    lastYearApV: 0.9,
    thisYearApV: 1.4,
    apvChange: 50,
    lastYearVisitors: 22653521,
    lastYearPercent: 18,
    thisYearVisitors: 19315833,
    thisYearPercent: 12,
    mixChange: -35
  },
  {
    channel: 'performance_max',
    lastYearApV: 0.9,
    thisYearApV: 0.9,
    apvChange: 1,
    lastYearVisitors: 3556159,
    lastYearPercent: 3,
    thisYearVisitors: 17402502,
    thisYearPercent: 11,
    mixChange: 273
  },
  {
    channel: 'display',
    lastYearApV: 0.3,
    thisYearApV: 0.4,
    apvChange: 33,
    lastYearVisitors: 13467298,
    lastYearPercent: 11,
    thisYearVisitors: 17217260,
    thisYearPercent: 11,
    mixChange: -3
  },
  {
    channel: 'travel_ads',
    lastYearApV: 1.1,
    thisYearApV: 1.1,
    apvChange: 4,
    lastYearVisitors: 22010713,
    lastYearPercent: 18,
    thisYearVisitors: 12054881,
    thisYearPercent: 7,
    mixChange: -58
  },
  {
    channel: 'direct',
    lastYearApV: 2.1,
    thisYearApV: 1.2,
    apvChange: -42,
    lastYearVisitors: 3621674,
    lastYearPercent: 3,
    thisYearVisitors: 8070625,
    thisYearPercent: 5,
    mixChange: 70
  },
  {
    channel: 'affiliate',
    lastYearApV: 1.4,
    thisYearApV: 1.2,
    apvChange: -11,
    lastYearVisitors: 7115933,
    lastYearPercent: 6,
    thisYearVisitors: 7813808,
    thisYearPercent: 5,
    mixChange: -16
  },
  {
    channel: 'seo_generic',
    lastYearApV: 1.0,
    thisYearApV: 1.2,
    apvChange: 20,
    lastYearVisitors: 5869916,
    lastYearPercent: 5,
    thisYearVisitors: 7074521,
    thisYearPercent: 4,
    mixChange: -8
  },
  {
    channel: 'paid_search_brand',
    lastYearApV: 3.3,
    thisYearApV: 3.8,
    apvChange: 15,
    lastYearVisitors: 2033137,
    lastYearPercent: 2,
    thisYearVisitors: 2919593,
    thisYearPercent: 2,
    mixChange: 9
  },
  {
    channel: 'performance_affiliates',
    lastYearApV: 1.2,
    thisYearApV: 0.5,
    apvChange: -56,
    lastYearVisitors: 405989,
    lastYearPercent: 0,
    thisYearVisitors: 1527995,
    thisYearPercent: 1,
    mixChange: 187
  },
  {
    channel: 'seo_brand',
    lastYearApV: 2.8,
    thisYearApV: 3.2,
    apvChange: 16,
    lastYearVisitors: 630254,
    lastYearPercent: 1,
    thisYearVisitors: 858700,
    thisYearPercent: 1,
    mixChange: 4
  },
  {
    channel: 'email_action_based',
    lastYearApV: 2.0,
    thisYearApV: 2.2,
    apvChange: 11,
    lastYearVisitors: 209989,
    lastYearPercent: 0,
    thisYearVisitors: 686791,
    thisYearPercent: 0,
    mixChange: 149
  },
  {
    channel: 'web_to_app',
    lastYearApV: 3.9,
    thisYearApV: 3.4,
    apvChange: -13,
    lastYearVisitors: 436944,
    lastYearPercent: 0,
    thisYearVisitors: 613952,
    thisYearPercent: 0,
    mixChange: 7
  }
]

function formatNumber(num) {
  return num.toLocaleString()
}

function ChannelTable() {
  return (
    <div className="channel-table-section">
      <div className="section-header">
        <h2>ApV by Channel last 12 weeks</h2>
        <p className="section-link">
          <a href="#">Channel documentation here.</a> Last 12 weeks vs the same period last year.
        </p>
      </div>
      <div className="table-container">
        <table className="channel-table">
          <thead>
            <tr>
              <th className="sortable">
                Year Channel
                <span className="sort-arrow">→</span>
              </th>
              <th>Last Year ApV</th>
              <th>This Year ApV</th>
              <th>% change from last year (ApV)</th>
              <th>Last Year Visitors</th>
              <th>% of total visitors</th>
              <th>This Year Visitors</th>
              <th>% of total visitors</th>
              <th>Mix change</th>
            </tr>
          </thead>
          <tbody>
            {channelData.map((row, index) => (
              <tr key={index}>
                <td className="channel-name">{row.channel}</td>
                <td>{row.lastYearApV}</td>
                <td>{row.thisYearApV}</td>
                <td>
                  <div className="change-cell">
                    <div className="change-bar-container">
                      <div 
                        className={`change-bar ${row.apvChange >= 0 ? 'positive' : 'negative'}`}
                        style={{ width: `${Math.min(Math.abs(row.apvChange), 100)}%` }}
                      />
                    </div>
                    <span className={`change-value ${row.apvChange >= 0 ? 'positive' : 'negative'}`}>
                      {row.apvChange > 0 ? '+' : ''}{row.apvChange}%
                    </span>
                  </div>
                </td>
                <td>{formatNumber(row.lastYearVisitors)}</td>
                <td>{row.lastYearPercent}%</td>
                <td>{formatNumber(row.thisYearVisitors)}</td>
                <td>{row.thisYearPercent}%</td>
                <td className={row.mixChange >= 0 ? 'positive' : 'negative'}>
                  {row.mixChange > 0 ? '+' : ''}{row.mixChange}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default ChannelTable
