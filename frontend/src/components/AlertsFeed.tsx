import type { AlertOut } from "../api/client";

export function AlertsFeed({ alerts }: { alerts: AlertOut[] }) {
  return (
    <div className="chart-card">
      <h3>Alerts</h3>
      {alerts.length === 0 ? (
        <p className="empty-state">No alerts raised.</p>
      ) : (
        <ul className="alerts-list">
          {alerts.map((a, i) => (
            <li key={i} className={`alert-item severity-${a.severity}`}>
              <span className="alert-severity">{a.severity}</span>
              <span className="alert-message">{a.message}</span>
              <span className="alert-time">{new Date(a.created_at).toLocaleTimeString()}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
