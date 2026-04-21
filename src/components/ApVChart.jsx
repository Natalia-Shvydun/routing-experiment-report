import React from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer
} from 'recharts'
import './ApVChart.css'

// Data from the dashboard image
const chartData = [
  { week: 'Nov 10', lastYear: 1.2, thisYear: 1.1 },
  { week: 'Nov 17', lastYear: 1.2, thisYear: 1.1 },
  { week: 'Nov 24', lastYear: 1.2, thisYear: 1.1 },
  { week: 'Dec 1', lastYear: 1.1, thisYear: 1.1 },
  { week: 'Dec 8', lastYear: 1.1, thisYear: 1.2 },
  { week: 'Dec 15', lastYear: 1.1, thisYear: 1.2 },
  { week: 'Dec 22', lastYear: 1.1, thisYear: 1.1 },
  { week: 'Dec 29', lastYear: 1.1, thisYear: 1.1 },
  { week: 'Jan 5', lastYear: 1.1, thisYear: 1.1 },
  { week: 'Jan 12', lastYear: 1.1, thisYear: 1.1 },
  { week: 'Jan 19', lastYear: 1.1, thisYear: 1.1 },
  { week: 'Jan 26', lastYear: 1.1, thisYear: 1.1 },
]

function ApVChart() {
  return (
    <div className="apv-chart">
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
          <XAxis 
            dataKey="week" 
            stroke="#666"
            tick={{ fontSize: 12 }}
            label={{ value: 'Session Week (Adjusted to This Year)', position: 'insideBottom', offset: -5, style: { fontSize: 12 } }}
          />
          <YAxis 
            stroke="#666"
            tick={{ fontSize: 12 }}
            label={{ value: 'ADP Views per Visitor, avg.', angle: -90, position: 'insideLeft', style: { fontSize: 12 } }}
            domain={[0, 1.5]}
            ticks={[0, 0.5, 1.0, 1.2]}
          />
          <Tooltip />
          <Legend 
            wrapperStyle={{ paddingTop: '20px' }}
            iconType="line"
          />
          <Line 
            type="monotone" 
            dataKey="lastYear" 
            stroke="#d32f2f" 
            strokeWidth={2}
            name="Last Year"
            dot={{ r: 4 }}
          />
          <Line 
            type="monotone" 
            dataKey="thisYear" 
            stroke="#fbc02d" 
            strokeWidth={2}
            name="This Year"
            dot={{ r: 4 }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

export default ApVChart
