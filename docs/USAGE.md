# Usage Guide

Detailed setup, operation, and troubleshooting instructions for the Flash Sale Distributed Data Pipeline. For the high-level overview and test results, see the [main README](../README.md).

## 1. Prerequisites

- **Docker Desktop** (or Docker Engine) with **Docker Compose v2.20+** — `docker compose version` to check. This project uses the top-level `name:` field in `docker-compose.yml`, which requires this version.
- **Internet access**, at least on first run: images are pulled (`apache/kafka`, `citusdata/citus`, `grafana`, `prometheus`, etc.) and the Spark job downloads its Kafka/JDBC packages from Maven Central on first launch.
- **Recommended resources**: at least 6–8 GB free RAM, 4+ CPU cores. 17 containers run concurrently (3 Kafka brokers + 4 Citus nodes + Spark master/worker(s)/driver + producer + 2 one-shot init jobs + 4 monitoring components).
- On **Windows**, this was developed and tested via **WSL2 + Docker Desktop**. If you hit `failed to connect to the docker API` errors, check Docker Desktop's **Settings → Resources → WSL Integration** and make sure your distro's toggle is enabled, then `wsl --shutdown` and restart Docker Desktop.

## 2. First-time setup

```bash
git clone https://github.com/Metekilic0/Flash-sale-system-project.git
cd Flash-sale-system-project
cp .env.example .env          # edit ports/passwords/rates if needed
docker compose up -d --build  # builds the producer, spark, and stream-processor images
```

What happens, in order:
1. 3 Kafka brokers form a KRaft quorum → become `healthy`.
2. `kafka-init` creates the `clickstream-events` topic (6 partitions / RF=3 / min.insync.replicas=2 by default) and exits.
3. Citus coordinator + 3 workers come up → become `healthy`.
4. `citus-init` registers all workers with the coordinator (`citus_add_node`) and bootstraps the schema (`events_clean`, `events_flagged`, `events_agg`, `pipeline_metrics`), then exits.
5. `producer` starts generating the synthetic clickstream.
6. `stream-processor` connects to Spark master and starts the Kafka→Citus streaming job (first launch is slower due to Maven package downloads).
7. Prometheus + Grafana come up with the dashboard auto-provisioned.

Watch progress with:
```bash
docker compose logs -f kafka-init citus-init stream-processor producer
```

## 3. Verification

```bash
chmod +x scripts/*.sh   # exec bit can be lost depending on how you extracted/cloned
bash scripts/healthcheck.sh
```

This prints container status, Kafka topic details, Citus's registered node list, row counts for every table, and the most recent `pipeline_metrics` rows. A growing row count on repeated runs means the pipeline is healthy end-to-end.

- **Grafana** → http://localhost:3000 — dashboard "Flash Sale Pipeline – Overview"
- **Spark master UI** → http://localhost:8080
- **Prometheus** → http://localhost:9090/targets (all targets should read "UP")
- **Citus**, directly: `psql -h localhost -p 5433 -U citus -d citus` (password in `.env`)

## 4. Load testing: normal traffic and flash-sale bursts

By default (`PRODUCER_MODE=auto`), the producer endlessly cycles:

```
NORMAL (~100 events/sec, 120s) → FLASH_SALE (~10,000 events/sec, 30s, 15% bot traffic) → NORMAL → ...
```

Tunable via `.env`: `PRODUCER_NORMAL_EPS`, `PRODUCER_BURST_EPS`, `PRODUCER_BOT_RATIO`, durations, etc.

For a **live demo with manual control**, set `PRODUCER_MODE=manual` in `.env`, restart the producer (`docker compose up -d producer`), then trigger a burst on demand:

```bash
bash scripts/trigger_burst.sh
docker compose logs -f producer
```

Bot traffic comes from a small, fixed pool of 8 IP addresses and targets only 5 "hot" products, which is what lets the rate-based anomaly filter (`ANOMALY_RATE_PER_SEC_THRESHOLD`, default 5 events/sec per IP, normalized against the batch's actual time span) catch it reliably. Results land in `events_flagged` and on the Grafana "Distinct flagged (bot-like) IPs" panel.

## 5. Fault-tolerance and scalability tests

```bash
# Scripted, timed run-through of the full fault-tolerance scenario:
# kill a Kafka broker → restart it → kill a Citus worker → restart it →
# scale Spark to 3 workers — checking pipeline_metrics at each step.
bash scripts/fault_injection.sh full-scenario

# Or individually:
bash scripts/fault_injection.sh kill-kafka-broker 3
bash scripts/fault_injection.sh restart-kafka-broker 3
bash scripts/fault_injection.sh kill-citus-worker 2
bash scripts/fault_injection.sh restart-citus-worker 2
bash scripts/fault_injection.sh scale-spark-workers 3
```

To test **beyond** the configured tolerance (i.e. deliberately break the guarantee), kill two Kafka brokers at once:
```bash
docker compose stop kafka-2 kafka-3
# producer should start failing with NotEnoughReplicasError — this is correct, not a bug
docker compose start kafka-2 kafka-3
```

For Citus horizontal scaling beyond 3 workers, add a new service block to `docker-compose.yml` following the `citus-worker-3` pattern, register it (`citus-init` re-run, or manually via `citus_add_node`), then:
```bash
docker compose exec citus-coordinator psql -U citus -d citus -c \
  "SELECT rebalance_table_shards(shard_transfer_mode := 'block_writes');"
```
(`block_writes` is required because our append-only tables lack a primary key/replica identity, which the default logical-replication transfer mode needs.)

## 6. Example analytical queries

`scripts/sample_queries.sql` contains ready-to-run queries: top products, legitimate-vs-flagged ratio, top offending IPs, latency distribution, and Citus shard placement (proof the data is actually distributed across workers).

```bash
docker compose exec citus-coordinator psql -U citus -d citus -f /dev/stdin < scripts/sample_queries.sql
```

## 7. Troubleshooting

- **`stream-processor` keeps restarting / "Connection refused"** — usually Spark master wasn't ready yet, or the `--packages` Maven download failed. Check `docker compose logs stream-processor` and your internet connection.
- **Citus workers don't show up in `pg_dist_node`** — check `docker compose logs citus-init`; it may have tried to connect before all nodes were `healthy`. Re-run: `docker compose up -d citus-init` (the script is idempotent).
- **Citus inter-node auth fails after a restart** (`"no password supplied"`) — this is a known Citus quirk: node-to-node authentication is separate from client-facing auth. Fixed by registering the password explicitly:
  ```bash
  docker compose exec citus-coordinator psql -U citus -d citus -c "
  INSERT INTO pg_dist_authinfo (nodeid, rolename, authinfo)
  VALUES (0, 'citus', 'password=<your CITUS_PASSWORD>')
  ON CONFLICT (nodeid, rolename) DO UPDATE SET authinfo = EXCLUDED.authinfo;"
  ```
  Run this on the coordinator and every worker. It's already baked into `citus/init/01-coordinator-bootstrap.sql` for fresh setups.
- **Grafana shows "no default database configured" for the Citus datasource** — clear Grafana's data volume and let it re-provision cleanly:
  ```bash
  docker compose stop grafana && docker compose rm -f grafana
  docker volume rm <project>_grafana-data
  docker compose up -d grafana
  ```
- **Port conflicts** (5432/9092/3000/...) — adjust `CITUS_HOST_PORT` in `.env` or the relevant `ports:` mapping in `docker-compose.yml`.
- **Bitnami images "not found"** — if you're working from an older fork of this project that still references `bitnami/kafka` or `bitnami/spark`, note that Bitnami removed almost all free, versioned tags from Docker Hub in 2025. This repository already uses the official `apache/kafka` image and a custom Spark image built from the official Apache binary distribution instead.
- **Docker Desktop loses all containers/images after an unclean shutdown** (e.g. a power outage) — this happened to us once. The underlying named volumes (where Kafka/Citus/Grafana actually store data) typically survive even when Docker's image/container metadata doesn't. Run `docker volume ls` to confirm your data volumes are still listed, then `docker compose up -d --build` to rebuild images and reattach to the existing volumes — no data should be lost.

## 8. Cleanup

```bash
docker compose down        # stop containers, keep volumes (data persists)
docker compose down -v     # also delete all data (Kafka/Citus/Grafana), start fresh
```
