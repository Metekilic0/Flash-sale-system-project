"""
Flash-Sale Streaming Pipeline - Spark Structured Streaming job
================================================================
Kafka (clickstream-events) -> parse/validate -> per-micro-batch anomaly
filter -> windowed aggregation -> Citus (distributed PostgreSQL) via JDBC.

Design notes (see project report for the full justification):

  * We use the `foreachBatch` sink so that each micro-batch becomes a regular
    (non-streaming) DataFrame. This lets us run arbitrary batch operations
    (groupBy/count for anomaly scoring, JDBC upserts with ON CONFLICT) that
    are awkward or impossible to express directly in the streaming API.

  * Anomaly detection: within a single micro-batch, any ip_address responsible
    for an events-PER-SECOND RATE above ANOMALY_RATE_PER_SEC_THRESHOLD is
    flagged as bot-like ("scalper") traffic for that batch. The rate is
    computed using the *actual* time span covered by the batch's own
    event_ts values (max - min), not the nominal SPARK_TRIGGER_INTERVAL_SEC.
    This matters because, under backpressure, a single micro-batch can end
    up covering far more real time than the configured trigger interval
    (e.g. 20+ seconds of backlog processed in one trigger); a fixed
    per-batch event COUNT threshold (our original design) then over-flags
    ordinary users simply because the batch happens to be unusually large.
    Normalizing by the batch's real time span fixes this. See the project
    report (Section 8.1 / Appendix A.3) for the evidence that motivated
    this change: under the original count-based rule, 84% of all events
    processed over the project's lifetime were flagged, including ordinary
    IPs re-flagged in ~940 separate micro-batches each.

  * Three Citus tables receive the output of every batch:
        events_clean    -> raw legitimate events           (sharded by product_id)
        events_flagged  -> raw bot-like events + reason     (sharded by ip_address)
        events_agg      -> per (product_id, action) counts  (sharded by product_id, upsert)
    plus one coordinator-local table:
        pipeline_metrics -> one row per batch, used for Grafana + the report's
                             performance-evaluation section.
"""

import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "clickstream-events")

CITUS_HOST = os.environ.get("CITUS_HOST", "citus-coordinator")
CITUS_PORT = os.environ.get("CITUS_PORT", "5432")
CITUS_DB = os.environ.get("CITUS_DB", "citus")
CITUS_USER = os.environ.get("CITUS_USER", "citus")
CITUS_PASSWORD = os.environ.get("CITUS_PASSWORD", "citus")

JDBC_URL = f"jdbc:postgresql://{CITUS_HOST}:{CITUS_PORT}/{CITUS_DB}"
JDBC_PROPS = {
    "user": CITUS_USER,
    "password": CITUS_PASSWORD,
    "driver": "org.postgresql.Driver",
}

ANOMALY_RATE_PER_SEC = float(os.environ.get("ANOMALY_RATE_PER_SEC_THRESHOLD", "5.0"))
TRIGGER_INTERVAL_SEC = int(os.environ.get("SPARK_TRIGGER_INTERVAL_SEC", "5"))

CHECKPOINT_LOCATION = "/tmp/spark-checkpoints/flash-sale-pipeline"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("action", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("timestamp", StringType(), False),  # parsed to TimestampType below
    StructField("ip_address", StringType(), False),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FlashSaleStreamProcessor")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages.exclusions", "")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(F.col("value").cast("string").alias("json_str"))
        .select(F.from_json("json_str", EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .drop("timestamp")
    )
    # Watermark guards against unbounded state growth if we later add
    # streaming-native windowed aggregations on top of this stream.
    return parsed.withWatermark("event_ts", "1 minute")


def write_jdbc_append(df: DataFrame, table: str):
    if df.rdd.isEmpty():
        return
    (
        df.write.mode("append")
        .jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)
    )


def upsert_events_agg(batch_df: DataFrame, window_start, batch_id: int):
    """
    events_agg has a composite primary key (window_start, product_id, action).
    Spark's JDBC writer has no native upsert, so we collect the small,
    already-aggregated rows to the driver and issue a parameterized
    INSERT ... ON CONFLICT ... DO UPDATE via psycopg2. This is safe because
    by this point the data is a tiny aggregate (a handful of rows per batch),
    not raw event volume.
    """
    rows = batch_df.collect()
    if not rows:
        return

    import psycopg2  # local import: only the driver needs this, not executors

    conn = psycopg2.connect(
        host=CITUS_HOST, port=CITUS_PORT, dbname=CITUS_DB,
        user=CITUS_USER, password=CITUS_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO events_agg (window_start, product_id, action, event_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (window_start, product_id, action)
                    DO UPDATE SET event_count = events_agg.event_count + EXCLUDED.event_count
                    """,
                    (window_start, row["product_id"], row["action"], row["event_count"]),
                )
    finally:
        conn.close()


def insert_pipeline_metric(metric: dict):
    import psycopg2

    conn = psycopg2.connect(
        host=CITUS_HOST, port=CITUS_PORT, dbname=CITUS_DB,
        user=CITUS_USER, password=CITUS_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_metrics
                    (batch_id, batch_ts, total_events, legit_events, flagged_events,
                     distinct_ips, distinct_flagged_ips, processing_time_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    metric["batch_id"], metric["batch_ts"], metric["total_events"],
                    metric["legit_events"], metric["flagged_events"],
                    metric["distinct_ips"], metric["distinct_flagged_ips"],
                    metric["processing_time_ms"],
                ),
            )
    finally:
        conn.close()


def process_batch(batch_df: DataFrame, batch_id: int):
    """
    Called once per micro-batch by Spark Structured Streaming (foreachBatch).
    `batch_df` here is a *regular* (non-streaming) DataFrame containing only
    the rows that arrived during this trigger interval.
    """
    start = time.time()
    batch_df = batch_df.cache()
    total_events = batch_df.count()

    if total_events == 0:
        batch_df.unpersist()
        return

    window_start = F.date_trunc("second", F.current_timestamp())
    window_start_value = batch_df.select(window_start.alias("w")).first()["w"]

    # ---- 1. Determine the actual time span covered by this batch -----------
    # A backlogged batch can cover far more real event-time than the nominal
    # trigger interval. We measure the span directly from the data (max - min
    # event_ts) rather than assuming it equals SPARK_TRIGGER_INTERVAL_SEC, and
    # floor it at the configured trigger interval so tiny/instant batches
    # don't produce an artificially tiny (and therefore overly strict) span.
    span_row = batch_df.select(
        F.min("event_ts").alias("min_ts"),
        F.max("event_ts").alias("max_ts"),
    ).first()
    raw_span_seconds = 0.0
    if span_row["min_ts"] is not None and span_row["max_ts"] is not None:
        raw_span_seconds = (span_row["max_ts"] - span_row["min_ts"]).total_seconds()
    effective_span_seconds = max(raw_span_seconds, float(TRIGGER_INTERVAL_SEC))
    dynamic_count_threshold = ANOMALY_RATE_PER_SEC * effective_span_seconds

    # ---- 2. Anomaly scoring: rate-normalized events per ip_address ----------
    ip_counts = batch_df.groupBy("ip_address").count()
    bot_ip_rows = ip_counts.filter(F.col("count") > dynamic_count_threshold).collect()
    bot_ips = [r["ip_address"] for r in bot_ip_rows]
    reason_text = (
        f"ip_rate_exceeded_{ANOMALY_RATE_PER_SEC}_events_per_sec"
        f"_over_{effective_span_seconds:.1f}s_span"
    )

    if bot_ips:
        flagged_df = batch_df.filter(F.col("ip_address").isin(bot_ips)) \
            .withColumn("batch_id", F.lit(batch_id)) \
            .withColumn("reason", F.lit(reason_text))
        legit_df = batch_df.filter(~F.col("ip_address").isin(bot_ips)) \
            .withColumn("batch_id", F.lit(batch_id))
    else:
        flagged_df = batch_df.limit(0) \
            .withColumn("batch_id", F.lit(batch_id)) \
            .withColumn("reason", F.lit(""))
        legit_df = batch_df.withColumn("batch_id", F.lit(batch_id))

    flagged_count = flagged_df.count()
    legit_count = total_events - flagged_count
    distinct_ips = batch_df.select("ip_address").distinct().count()

    # ---- 2. Persist raw legitimate + flagged events -------------------------
    write_jdbc_append(
        legit_df.select("event_id", "user_id", "action", "product_id", "ip_address", "event_ts", "batch_id"),
        "events_clean",
    )
    write_jdbc_append(
        flagged_df.select("event_id", "user_id", "action", "product_id", "ip_address", "event_ts", "batch_id", "reason"),
        "events_flagged",
    )

    # ---- 3. Windowed aggregation of legitimate traffic (per product/action) -
    agg_df = legit_df.groupBy("product_id", "action").agg(F.count("*").alias("event_count"))
    upsert_events_agg(agg_df, window_start_value, batch_id)

    # ---- 4. Batch-level metrics for monitoring / the report ------------------
    processing_time_ms = int((time.time() - start) * 1000)
    insert_pipeline_metric({
        "batch_id": batch_id,
        "batch_ts": window_start_value,
        "total_events": total_events,
        "legit_events": legit_count,
        "flagged_events": flagged_count,
        "distinct_ips": distinct_ips,
        "distinct_flagged_ips": len(bot_ips),
        "processing_time_ms": processing_time_ms,
    })

    print(f"[batch {batch_id}] total={total_events} legit={legit_count} "
          f"flagged={flagged_count} bot_ips={len(bot_ips)} took={processing_time_ms}ms")

    batch_df.unpersist()


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    stream_df = read_kafka_stream(spark)

    query = (
        stream_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=f"{TRIGGER_INTERVAL_SEC} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
