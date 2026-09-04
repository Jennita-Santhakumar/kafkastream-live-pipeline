import { api } from "./api/client";
import { AlertsFeed } from "./components/AlertsFeed";
import { EventTypeBreakdown } from "./components/EventTypeBreakdown";
import { RecentEventsTable } from "./components/RecentEventsTable";
import { StatTiles } from "./components/StatTiles";
import { ThroughputChart } from "./components/ThroughputChart";
import { usePolling } from "./hooks/usePolling";

const POLL_MS = 3000;

export default function App() {
  const summary = usePolling(() => api.metricsSummary(), POLL_MS);
  const timeseries = usePolling(() => api.metricsTimeseries(15), POLL_MS);
  const events = usePolling(() => api.recentEvents(25), POLL_MS);
  const windowedStats = usePolling(() => api.windowedStats(60), POLL_MS);
  const alerts = usePolling(() => api.alerts(20), POLL_MS);

  return (
    <div className="app">
      <header className="app-header">
        <h1>Realtime Event Streaming Pipeline</h1>
        <p className="subtitle">Kafka &rarr; PySpark Structured Streaming &rarr; Delta Lake</p>
      </header>

      <StatTiles summary={summary.data} />

      <div className="grid-2">
        <ThroughputChart points={timeseries.data ?? []} />
        <EventTypeBreakdown stats={windowedStats.data ?? []} />
      </div>

      <div className="grid-2">
        <AlertsFeed alerts={alerts.data ?? []} />
        <RecentEventsTable events={events.data ?? []} />
      </div>

      {(summary.error || timeseries.error) && (
        <p className="error-banner">
          API unreachable ({summary.error ?? timeseries.error}). Is the backend running?
        </p>
      )}
    </div>
  );
}
