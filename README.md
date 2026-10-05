# Realtime Event Streaming Pipeline

> **Maintained by [Jennita S](https://github.com/Jennita-Santhakumar)** · [LinkedIn](https://linkedin.com/in/jennitas) · jennitasanthakumar0@gmail.com

A production-shaped real-time data platform: a synthetic e-commerce event producer feeds Kafka,
a PySpark Structured Streaming job validates/deduplicates/enriches events and computes windowed
aggregations, throughput/lag metrics, and rule-based anomaly alerts, all persisted to Delta Lake
— served to a live dashboard through a read-only FastAPI layer, with Prometheus + Grafana for
operational monitoring.

## Architecture

```
┌────────────┐   JSON events   ┌───────────────┐
│  Producer   │────────────────▶│  Kafka (KRaft) │
│ (synthetic  │                 │  topic: events │
│  events)    │                 └───────┬────────┘
└────────────┘                          │
                                         │  two independent readStream queries
                    ┌────────────────────┴────────────────────┐
                    ▼                                          ▼
      ┌──────────────────────────┐              ┌───────────────────────────────┐
      │  foreachBatch query       │              │  native windowed-agg query     │
      │  validate → dedup →       │              │  validate → watermark →        │
      │  enrich (join lookup)     │              │  dedup → groupBy(window,       │
      │  → write events           │              │  event_type) → append sink     │
      │  → compute metrics        │              │  (real stateful finalization)  │
      │  → rule-based alerts       │              └───────────────┬───────────────┘
      └─────────────┬──────────────┘                              │
                     ▼                                            ▼
            ┌──────────────────────────────── Delta Lake (bind-mounted) ───────────────────────────┐
            │  events   pipeline_metrics   alerts   event_stats   lookup/users                       │
            └──────────────────────────────────────────┬───────────────────────────────────────────┘
                                                         │ deltalake (delta-rs) reads
                                                         ▼
                                            ┌─────────────────────────┐
                                            │       FastAPI API        │
                                            │ /api/events  /api/stats  │
                                            │ /api/alerts  /api/metrics│
                                            │ /metrics (Prometheus)    │
                                            └────────────┬─────────────┘
                                                          │ REST (polled every 3s)
                                                          ▼
                                            ┌─────────────────────────┐
                                            │   React Dashboard         │
                                            │ throughput, lag, alerts,  │
                                            │ event-type breakdown,     │
                                            │ recent events table       │
                                            └───────────────────────────┘

                              Prometheus (scrapes API /metrics) → Grafana dashboard
```

## Quickstart

```bash
docker compose up --build
```

| Service         | URL                                   |
|-----------------|----------------------------------------|
| Frontend        | http://localhost:5210                  |
| API             | http://localhost:8035                  |
| API health      | http://localhost:8035/health            |
| API docs        | http://localhost:8035/docs              |
| Kafka (external)| localhost:29099                         |
| Spark UI        | http://localhost:4045                   |
| Prometheus      | http://localhost:9096                   |
| Grafana         | http://localhost:3035 (anonymous viewer access) |

Ports are offset from sibling portfolio projects (`rag-knowledge-engine`, `inferops`,
`ab-testing-framework`) so multiple stacks can run at once without conflicts.

On first `docker compose up`, the `seed-lookup` one-shot service populates the user-lookup Delta
table before `streaming` and `api` start (compose `service_completed_successfully` dependency).
The `producer` starts emitting events immediately; recent events, throughput, and lag populate
within ~30-60s (the `foreachBatch` query's micro-batch trigger). The event-type breakdown chart
(from `event_stats`) takes longer — its native windowed-aggregation query only emits a window once
its watermark passes the window's end, so the first finalized 1-minute window appears roughly
1.5-2 minutes after startup.

## API reference

| Method | Path                        | Description                                              |
|--------|-----------------------------|------------------------------------------------------------|
| GET    | `/api/events/recent`        | Most recent validated+enriched events (`limit`, default 50) |
| GET    | `/api/stats/windowed`       | Finalized windowed aggregates, rolled up from partial per-batch rows (`minutes`, `event_type`) |
| GET    | `/api/alerts`               | Recent rule-based alerts (`limit`, default 50)             |
| GET    | `/api/metrics/timeseries`   | Per-micro-batch throughput/lag points (`minutes`, default 15) |
| GET    | `/api/metrics/summary`      | Latest snapshot: total events, current events/sec, p95 lag, alerts in the last hour |
| GET    | `/metrics`                  | Prometheus text-format exposition of the same summary metrics |
| GET    | `/health`                   | Liveness check                                              |

Full interactive schema at `/docs` once the API is running.

## Local development

**Producer / streaming / backend** (Python 3.11 — matches Docker's `python:3.11-slim` /
`bitnamilegacy/spark:3.5.1` base images and this repo's CI):

```bash
cd producer   # or streaming, or backend
python3.11 -m venv venv
./venv/Scripts/activate   # or source venv/bin/activate on Linux/Mac
pip install -r requirements-dev.txt
pytest tests -v
```

**Running `streaming/tests` locally on Windows** needs a JDK (Spark is a JVM process under
PySpark) and Hadoop's `winutils.exe`/`hadoop.dll` on `HADOOP_HOME`, or every test fails with
`SocketTimeoutException: Accept timed out` (Python-worker spawn) or
`FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset`, neither of which is a code
bug — install Temurin 17 and drop prebuilt Windows Hadoop binaries (e.g.
[cdarlint/winutils](https://github.com/cdarlint/winutils)) under `HADOOP_HOME/bin`. Linux/Mac and
CI don't need this.

The streaming job's integration test (`streaming/tests/test_streaming_integration.py`) spins up a
real Kafka broker via `testcontainers` and runs the actual job against it — needs Docker running
locally, and is resource-heavy enough (two Spark queries + a Kafka container) that it can time out
on a machine already running several other Docker Compose stacks. It's excluded from a plain
`pytest streaming/tests -m "not integration"` run and given a dedicated, less-contended CI runner
as its own step.

**Frontend**:

```bash
cd frontend
npm install
npm run dev
```

## Design Decisions & Trade-offs

- **Single-node Spark (`local[*]`), not the PRD's separate master/worker cluster.** A portfolio
  demo doesn't benefit from real cluster orchestration overhead; the streaming logic (windowing,
  watermarking, foreachBatch) is identical whether it runs on one node or ten. Documented here
  rather than silently diverging from the PRD's docker-compose sketch.
- **Two independent streaming queries against the same Kafka topic**, not one. The PRD's own code
  sketch does per-batch aggregation inside a single writeStream, which only ever emits partial,
  never-finalized windows. Real Structured Streaming windowing needs `withWatermark` +
  `dropDuplicates` + `groupBy(window(...))` with `outputMode("append")`, which requires a
  *streaming* DataFrame — not the static per-batch DataFrame `foreachBatch` hands you. Running a
  second, native stateful query alongside the `foreachBatch` one (which handles raw event writes,
  metrics, and alerting) gets correct, watermark-finalized windows without giving up per-batch
  metrics/alerting flexibility.
- **The API reads Delta tables via `deltalake` (delta-rs), not a second Spark session.** The API
  only needs read access to already-committed Delta tables; a JVM per API replica would add
  startup latency and memory for no benefit. The one deliberate stack split in an otherwise
  single-language pipeline.
- **`event_stats` accumulates one partial row per micro-batch per (window, event_type) from the
  native windowed query's append-mode output** (Spark's append-mode watermark finalization still
  fires once per micro-batch as data crosses the watermark, not once per window overall).
  `/api/stats/windowed` sums `event_count` and re-derives `avg_value` from the underlying sums
  rather than averaging partial averages, matching how the finalized aggregate should look.
- **Alerting uses a rolling z-score kept in the streaming job's driver process**
  (`streaming/src/alerts.py: RollingStats`), not a fault-tolerant external store. State resets on
  job restart. Acceptable for a demo; a real deployment would persist rolling stats in something
  the job re-hydrates from on startup (e.g. the last N rows of `event_stats` itself).
- **Delta Lake storage is a bind-mounted host directory (`./data`), not a named Docker volume** —
  both the `streaming` (writer) and `api` (reader) containers need to see the same files, and a
  named volume is invisible to any host-side inspection/debugging. Ivy/jar caches for Delta and
  the Kafka connector are resolved once at `docker build` time (`streaming/warmup.py`) instead of
  at container startup, so runtime containers never depend on network access to Maven Central.
- **Dashboard polls the API every 3 seconds rather than pushing over a WebSocket.** Simpler to
  build and reason about, and at this data volume the staleness is imperceptible; a
  higher-throughput production dashboard would want server-sent events or a WebSocket instead.

## Benchmark results

The PRD's target of **100K events/sec at <5s end-to-end latency with auto-scaling to 10+ nodes**
describes a multi-node production cluster (separate Kafka brokers, multiple Spark executors,
horizontal scaling) — not something a single laptop running one broker and one Spark process in
`local[*]` mode can approach. Rather than repeat that number, below is what this stack actually
sustained on the development machine, measured with `scripts/benchmark.py` against a real
`docker compose up` run (single Kafka broker, single Spark node, no cluster):

| Metric | Baseline (steady-state producer at 50 events/sec) | Under load (`benchmark.py --rate 3000 --duration 30`) |
|---|---|---|
| Producer-side throughput | 50 events/sec (configured) | **1,500 events/sec sustained** (the synchronous single-threaded producer loop's real ceiling — `--rate 3000` was the *target*, not what it could push) |
| Streaming job processed throughput | ~50-175 events/sec per micro-batch | up to **~3,150 events/sec in a single micro-batch**, ~1,960 events/sec average across the batches right after the spike |
| p95 end-to-end processing lag | **~9-17 seconds** | **~12-18 seconds** during backlog absorption |
| Alerting | — | **10 real alerts** correctly fired (rule-based z-score volume-anomaly detector, `streaming/src/alerts.py`), each event_type's per-batch count landing 3.4-3.7 std-dev above its rolling average during the spike |

Takeaways, stated honestly:

- **Throughput**: a few thousand events/sec, not 100K — roughly 30-60x below the PRD's cluster
  number. The gap is architectural, not a tuning problem: one Kafka partition set on one broker,
  one JVM serving as both Spark driver and sole executor, and Delta writes going to local disk
  with no cluster-level parallelism. Getting to 100K/sec is a matter of adding Kafka partitions +
  brokers and Spark executors, not rewriting this pipeline's logic.
- **Latency**: p95 lag of ~9-18 seconds is well above the PRD's <5s target even at low load. Most
  of it is structural for this design: a 5-second micro-batch trigger interval alone accounts for
  up to 5s of queuing delay before a just-produced event is even picked up, plus JVM/Ivy/Delta
  write overhead per batch on a single node. A lower trigger interval would cut this at the cost
  of more per-batch overhead relative to useful work — a real trade-off, not a free win.
- **Alerting works**: the volume-anomaly z-score rule is not a stub — spiking traffic 30-60x above
  baseline correctly triggered real alerts within one micro-batch of the spike arriving, visible
  live in `/api/alerts` and the dashboard's Alerts panel.

## Tech stack

Kafka (KRaft mode) · PySpark Structured Streaming · Delta Lake · FastAPI · `deltalake` (delta-rs)
· React + Vite + TypeScript + Recharts · Prometheus · Grafana · Docker Compose

## Credits

Developed by Kabilesh Rajaselvan with contributions from Jennita S.
