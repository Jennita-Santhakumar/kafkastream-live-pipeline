import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import type { MetricPointOut } from "../api/client";

export function ThroughputChart({ points }: { points: MetricPointOut[] }) {
  const data = points.map((p) => ({
    time: new Date(p.processed_at).toLocaleTimeString(),
    events_per_sec: p.events_per_sec,
    p95_lag_ms: p.p95_lag_ms,
  }));

  return (
    <div className="chart-card">
      <h3>Throughput &amp; Lag</h3>
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={data}>
          <XAxis dataKey="time" tick={{ fontSize: 11 }} />
          <YAxis yAxisId="left" tick={{ fontSize: 11 }} />
          <YAxis yAxisId="right" orientation="right" tick={{ fontSize: 11 }} />
          <Tooltip />
          <Line yAxisId="left" type="monotone" dataKey="events_per_sec" stroke="#2563eb" dot={false} name="events/sec" />
          <Line yAxisId="right" type="monotone" dataKey="p95_lag_ms" stroke="#dc2626" dot={false} name="p95 lag (ms)" />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
