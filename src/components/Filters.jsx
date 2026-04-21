import React, { useState } from 'react'
import './Filters.css'

function Filters() {
  const [isAcquisition, setIsAcquisition] = useState('No')

  return (
    <div className="filters">
      <div className="filter-group">
        <label>Value Precision</label>
        <select className="filter-select">
          <option>Probabilistic (with std. error ~1.6%)</option>
        </select>
      </div>
      <div className="filter-group">
        <label>Landing Page Type</label>
        <select className="filter-select">
          <option>is Area or Area Category or City or City Cate...</option>
        </select>
      </div>
      <div className="filter-group">
        <label>Channel Group</label>
        <select className="filter-select">
          <option>is any value</option>
        </select>
      </div>
      <div className="filter-group">
        <label>Channel</label>
        <select className="filter-select">
          <option>is any value</option>
        </select>
      </div>
      <div className="filter-group">
        <label>Platform Name</label>
        <select className="filter-select">
          <option>is any value</option>
        </select>
      </div>
      <div className="filter-group">
        <label>Is Acquisition (Yes / No)</label>
        <div className="toggle-buttons">
          <button
            className={`toggle-button ${isAcquisition === 'Yes' ? 'active' : ''}`}
            onClick={() => setIsAcquisition('Yes')}
          >
            Yes
          </button>
          <button
            className={`toggle-button ${isAcquisition === 'No' ? 'active' : ''}`}
            onClick={() => setIsAcquisition('No')}
          >
            No
          </button>
        </div>
      </div>
    </div>
  )
}

export default Filters
