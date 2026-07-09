# Flash Sale Distributed Data Pipeline

**BIL 401 — Big Data and Distributed Data Processing — Systems Track**

A simulated e-commerce "flash sale" pipeline that integrates **Apache Kafka**, **Apache Spark Structured Streaming**, and **Citus** (distributed PostgreSQL), fully orchestrated with Docker Compose and observable via **Prometheus + Grafana**. Built to genuinely test — not just claim — scalability, connectivity, and fault tolerance of an integrated multi-technology distributed system, running entirely on a single development laptop.

> 📄 **[Read the Final Report](./docs/BIL401_Final_Report.docx)** for the full architecture writeup, methodology, and every measured result referenced below.
> 📄 **[Read Checkpoint 2 (Status Report)](./docs/BIL401_Checkpoint2_StatusReport.docx)** for the mid-project status snapshot.

**Group:** Mete KILIÇ (231401028) · İsmail Emre YILDIZ (221401006)

---

## What this is

A synthetic clickstream generator feeds Kafka; Spark Structured Streaming consumes the stream, separates apparent bot/scalper traffic from legitimate traffic in near real time, and writes the results into a horizontally-sharded Citus cluster; Grafana visualizes throughput, latency, and bot-detection metrics live.

```
Synthetic Producer (Python)
        │
        ▼
Kafka Cluster (3 brokers, KRaft, 6 partitions, RF=3, min.insync.replicas=2)
        │
        ▼
Spark Structured Streaming (1 master + up to 3 workers, foreachBatch sink)
        │  ├─ rate-normalized anomaly detection (events/sec per IP)
        │  └─ per-product/action aggregation
        ▼
Citus — distributed PostgreSQL (1 coordinator + 3 workers)
   events_clean · events_flagged · events_agg · pipeline_metrics
        │
        ▼
Prometheus + Grafana (live dashboard)
```

17 containers, one `docker-compose.yml`, no Kubernetes, no cloud — deliberately, to stay testable on a student laptop as the course brief requires.

---

## Quick start

**Prerequisites:** Docker Desktop with Compose v2.20+ (`docker compose version` to check), internet access for the first run, ~6–8 GB free RAM.

```bash
git clone https://github.com/Metekilic0/Flash-sale-system-project.git
cd Flash-sale-system-project
cp .env.example .env
docker compose up -d --build
```

First boot takes a few minutes (Kafka KRaft quorum forms → topic created → Citus workers register with the coordinator and the schema is bootstrapped → Spark connects and starts the streaming job → producer starts generating traffic → Prometheus/Grafana come up with the dashboard auto-provisioned).

```bash
# Watch it come together
docker compose logs -f kafka-init citus-init stream-processor producer

# Once everything is healthy
bash scripts/healthcheck.sh
```

Then open:
- **Grafana** → http://localhost:3000 (dashboard: *Flash Sale Pipeline – Overview*)
- **Spark master UI** → http://localhost:8080
- **Prometheus** → http://localhost:9090/targets

Full setup, troubleshooting, and chaos-test instructions are in [`docs/USAGE.md`](./docs/USAGE.md).

---

## What was actually tested (not just built)

| Category | What we did | Result |
|---|---|---|
| **Kafka fault tolerance** | Killed 1 of 3 brokers; then killed 2 of 3 on purpose | 1-broker loss: zero impact. 2-broker loss: producer correctly refused writes (`NotEnoughReplicasError`) instead of silently dropping data — exactly the guarantee `min.insync.replicas=2` is meant to provide. Auto-recovered once 2/3 brokers were healthy again. |
| **Citus fault tolerance** | Killed a worker mid-write | Immediate, explicit JDBC failure; in one run the Spark job crashed and was auto-restarted by Docker's `restart: on-failure` policy with no permanent data loss (checkpointed Kafka offsets). |
| **Spark scale-out** | Compared 1 vs. 3 workers on comparable flash-sale batches | ~29–43% lower processing latency at 3 workers; intentionally capped at 3 workers (6 cores) to match the topic's 6 Kafka partitions. |
| **Citus scale-out** | Added a 3rd worker mid-project, ran `rebalance_table_shards()` | 8 of 32 shards per table moved to the new worker with zero data loss, verified by row-count diff before/after. |
| **5× overload stress test** | Manually triggered a burst at 5× nominal target rate | System didn't crash — settled into a steady state. Found the actual bottleneck was our own single-process producer, not Kafka/Spark/Citus. |
| **Anomaly-detection accuracy** | Queried ~93M+ accumulated events | Found the original fixed-count threshold over-flagged legitimate traffic under backlog (84% false-positive rate project-wide); root-caused it to **two** separate issues, fixed both, and re-verified the corrected pipeline flags almost exactly the seeded 15% bot ratio. |
| **Unplanned: real power outage** | Development machine lost power mid-project; Docker's container/image metadata didn't survive | Underlying Docker volumes did. After rebuilding images (~15 min), the system came back with all ~124M previously-processed events intact in Citus — no manual data recovery needed. |

All of the above is described in full, with the actual terminal output and SQL queries used, in the [Final Report](./docs/BIL401_Final_Report.docx).

---

## Repository structure

```
.
├── docker-compose.yml          # full 17-service orchestration
├── .env.example                # all configurable parameters
├── producer/                   # synthetic clickstream generator (Python)
├── spark/
│   ├── Dockerfile              # custom image (Apache Spark + psycopg2)
│   └── jobs/streaming_job.py   # the Structured Streaming application
├── citus/init/                 # coordinator/worker bootstrap SQL
├── monitoring/                 # Prometheus config + Grafana dashboard/provisioning
├── scripts/
│   ├── fault_injection.sh      # chaos-testing scenarios (Kafka/Citus/Spark)
│   ├── healthcheck.sh          # end-to-end smoke test
│   ├── trigger_burst.sh        # manual flash-sale trigger for live demos
│   └── sample_queries.sql      # example analytical queries
├── tests/test_rate_logic.py    # offline unit test for the producer's rate limiter
└── docs/
    ├── BIL401_Final_Report.docx
    └── BIL401_Checkpoint2_StatusReport.docx
```

---

## Why these technologies

- **Kafka** — high-throughput ingestion buffer that absorbs the burst-traffic spikes characteristic of a flash sale, while a producer keeps writing independently of how fast the consumer can keep up.
- **Spark Structured Streaming** — the `foreachBatch` sink lets each micro-batch be treated as a regular batch DataFrame, which is what makes the per-batch anomaly scoring and upsert-style aggregate writes possible.
- **Citus** — turns PostgreSQL into a horizontally-shardable store without giving up SQL, letting us demonstrate genuine write-scaling and shard rebalancing rather than just read replicas.
- **Docker Compose, not Kubernetes** — the course brief specifically asks for something testable on a student laptop; a second orchestration layer on top of an already 17-service stack would add complexity without testing anything new about Kafka/Spark/Citus integration.

---

## License

This is a course project for BIL 401 (Big Data and Distributed Data Processing). Provided as-is for educational reference.
