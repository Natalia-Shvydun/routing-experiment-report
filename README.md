# Discovery Cockpit Dashboard

A mock analytics dashboard recreated from a Looker Dashboard, built with React and Recharts.

## Features

- **Dashboard Header**: Shows owner, title, and action buttons
- **Filters**: Interactive filter controls including dropdowns and toggle buttons
- **Discover Section**: Line chart comparing "This Year" vs "Last Year" ApV metrics
- **Narrative Box**: Explanatory text about the data insights
- **Channel Table**: Detailed breakdown of ApV metrics by channel with visual change indicators

## Getting Started

### Install Dependencies

```bash
npm install
```

### Run Development Server

```bash
npm run dev
```

The dashboard will be available at `http://localhost:5173`

### Build for Production

```bash
npm run build
```

## Project Structure

```
src/
  components/
    Dashboard.jsx          # Main dashboard container
    DashboardHeader.jsx    # Header with title and actions
    Filters.jsx            # Filter controls
    DiscoverSection.jsx    # Section container for chart and narrative
    ApVChart.jsx           # Line chart component
    NarrativeBox.jsx       # Explanatory text component
    ChannelTable.jsx       # Data table component
```

## Customization

The dashboard is designed to be easily iterated upon. You can:

- **Reorganize sections**: Move components around in `Dashboard.jsx`
- **Change chart types**: Modify `ApVChart.jsx` to use different chart types from Recharts
- **Improve narrative clarity**: Edit the text in `NarrativeBox.jsx`
- **Update styling**: Modify the CSS files in each component directory

## Technologies Used

- React 18
- Recharts (for charting)
- Vite (build tool)
- CSS (styling)
