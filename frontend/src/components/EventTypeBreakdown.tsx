import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

import type { WindowedStatOut } from "../api/client";

export function EventTypeBreakdown({ stats }: { stats: WindowedStatOut[] }) {
  const totals = new Map<string, number>();
  for (const s of stats) {
    totals.set(s.event_type, (totals.get(s.event_type) ?? 0) + s.event_count);
  }
  const data = Array.from(totals.entries()).map(([event_type, event_count]) => ({
    event_type,
    event_count,
  }));

  return (
    <div className="chart-card">
      <h3>Event Volume by Type (last hour)</h3>
      <ResponsiveContainer width="100%" height={220}>
        <BarChart data={data}>
          <XAxis dataKey="event_type" tick={{ fontSize: 11 }} />
          <YAxis tick={{ fontSize: 11 }} />
          <Tooltip />
          <Bar dataKey="event_count" fill="#2563eb" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
