import React from 'react'
import DashboardHeader from './DashboardHeader'
import Filters from './Filters'
import DiscoverSection from './DiscoverSection'
import ChannelTable from './ChannelTable'
import './Dashboard.css'

function Dashboard() {
  return (
    <div className="dashboard">
      <DashboardHeader />
      <Filters />
      <DiscoverSection />
      <ChannelTable />
    </div>
  )
}

export default Dashboard
