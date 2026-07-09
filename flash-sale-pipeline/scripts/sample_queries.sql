-- =============================================================================
-- Example analytical queries for the project report.
-- Run with: docker compose exec citus-coordinator psql -U citus -d citus
-- (or paste individual queries into Grafana's "Citus (Postgres)" datasource)
-- =============================================================================

-- 1) Top 10 products by legitimate activity (all-time)
SELECT product_id, action, SUM(event_count) AS total
FROM events_agg
GROUP BY product_id, action
ORDER BY total DESC
LIMIT 10;

-- 2) Throughput over time: events processed per batch
SELECT batch_ts, total_events, legit_events, flagged_events
FROM pipeline_metrics
ORDER BY batch_ts;

-- 3) What fraction of traffic was flagged as bot-like, overall?
SELECT
    SUM(legit_events)               AS legit_total,
    SUM(flagged_events)              AS flagged_total,
    ROUND(100.0 * SUM(flagged_events) / NULLIF(SUM(total_events), 0), 2) AS flagged_pct
FROM pipeline_metrics;

-- 4) Top offending IPs (most flagged events) - demonstrates the anomaly filter
SELECT ip_address, COUNT(*) AS flagged_event_count, COUNT(DISTINCT batch_id) AS batches_flagged_in
FROM events_flagged
GROUP BY ip_address
ORDER BY flagged_event_count DESC
LIMIT 10;

-- 5) Processing latency distribution (useful for the "performance evaluation"
--    section - compare this during NORMAL vs FLASH_SALE phases / before vs
--    after scaling Spark workers)
SELECT
    MIN(processing_time_ms) AS min_ms,
    ROUND(AVG(processing_time_ms), 1) AS avg_ms,
    MAX(processing_time_ms) AS max_ms,
    COUNT(*) AS batch_count
FROM pipeline_metrics;

-- 6) Citus shard placement - shows the data is actually distributed across
--    both worker nodes (good evidence for the "distributed system" claim)
SELECT
    logicalrelid::regclass AS table_name,
    nodename,
    count(*) AS shard_count
FROM pg_dist_shard ds
JOIN pg_dist_shard_placement sp ON ds.shardid = sp.shardid
GROUP BY logicalrelid, nodename
ORDER BY table_name, nodename;

-- 7) Busiest 1-minute windows (helps locate the flash-sale burst in the data)
SELECT
    date_trunc('minute', batch_ts) AS minute,
    SUM(total_events) AS events_in_minute
FROM pipeline_metrics
GROUP BY minute
ORDER BY minute;
