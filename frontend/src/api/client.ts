const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8035";

export interface EventOut {
  event_id: string;
  user_id: string;
  event_type: string;
  event_time: string;
  value: number;
  user_segment: string | null;
  country: string | null;
}

export interface WindowedStatOut {
  window_start: string;
  window_end: string;
  event_type: string;
  event_count: number;
  avg_value: number;
  max_value: number;
}

export interface AlertOut {
  alert_type: string;
  message: string;
  severity: string;
  batch_id: number;
  created_at: string;
}

export interface MetricPointOut {
  batch_id: number;
  event_count: number;
  events_per_sec: number;
  avg_lag_ms: number;
  p95_lag_ms: number;
  processed_at: string;
}

export interface MetricsSummaryOut {
  total_events: number;
  current_events_per_sec: number;
  current_p95_lag_ms: number;
  alerts_last_hour: number;
  latest_batch_id: number | null;
}

async function getJSON<T>(path: string): Promise<T> {
  const resp = await fetch(`${BASE_URL}${path}`);
  if (!resp.ok) {
    throw new Error(`${path} failed: ${resp.status}`);
  }
  return resp.json();
}

export const api = {
  recentEvents: (limit = 25) => getJSON<EventOut[]>(`/api/events/recent?limit=${limit}`),
  windowedStats: (minutes = 60) => getJSON<WindowedStatOut[]>(`/api/stats/windowed?minutes=${minutes}`),
  alerts: (limit = 20) => getJSON<AlertOut[]>(`/api/alerts?limit=${limit}`),
  metricsTimeseries: (minutes = 15) => getJSON<MetricPointOut[]>(`/api/metrics/timeseries?minutes=${minutes}`),
  metricsSummary: () => getJSON<MetricsSummaryOut>(`/api/metrics/summary`),
};
