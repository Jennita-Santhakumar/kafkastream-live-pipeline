import type { EventOut } from "../api/client";

export function RecentEventsTable({ events }: { events: EventOut[] }) {
  return (
    <div className="chart-card">
      <h3>Recent Events</h3>
      <table className="events-table">
        <thead>
          <tr>
            <th>Time</th>
            <th>User</th>
            <th>Type</th>
            <th>Value</th>
            <th>Segment</th>
            <th>Country</th>
          </tr>
        </thead>
        <tbody>
          {events.map((e) => (
            <tr key={e.event_id}>
              <td>{new Date(e.event_time).toLocaleTimeString()}</td>
              <td>{e.user_id}</td>
              <td>{e.event_type}</td>
              <td>{e.value.toFixed(2)}</td>
              <td>{e.user_segment ?? "-"}</td>
              <td>{e.country ?? "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {events.length === 0 && <p className="empty-state">No events yet.</p>}
    </div>
  );
}
