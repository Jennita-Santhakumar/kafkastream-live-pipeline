import type { MetricsSummaryOut } from "../api/client";

export function StatTiles({ summary }: { summary: MetricsSummaryOut | null }) {
  const tiles = [
    { label: "Total events", value: summary?.total_events ?? "-" },
    { label: "Events / sec", value: summary?.current_events_per_sec ?? "-" },
    { label: "p95 lag (ms)", value: summary?.current_p95_lag_ms ?? "-" },
    { label: "Alerts (1h)", value: summary?.alerts_last_hour ?? "-" },
  ];

  return (
    <div className="stat-tiles">
      {tiles.map((tile) => (
        <div className="stat-tile" key={tile.label}>
          <div className="stat-tile-value">{tile.value}</div>
          <div className="stat-tile-label">{tile.label}</div>
        </div>
      ))}
    </div>
  );
}
