CREATE EXTENSION IF NOT EXISTS citus;

-- Ensure coordinator can always authenticate to workers after restarts
INSERT INTO pg_dist_authinfo (nodeid, rolename, authinfo)
VALUES (0, 'citus', 'password=citus_pw_change_me')
ON CONFLICT (nodeid, rolename) DO UPDATE SET authinfo = EXCLUDED.authinfo;

SELECT citus_set_coordinator_host('citus-coordinator', 5432);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_dist_node WHERE nodename = 'citus-worker-1') THEN
    PERFORM citus_add_node('citus-worker-1', 5432);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_dist_node WHERE nodename = 'citus-worker-2') THEN
    PERFORM citus_add_node('citus-worker-2', 5432);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_dist_node WHERE nodename = 'citus-worker-3') THEN
    PERFORM citus_add_node('citus-worker-3', 5432);
  END IF;
END
$$;

-- events_clean
CREATE TABLE IF NOT EXISTS events_clean (
    event_id      text NOT NULL,
    user_id       text NOT NULL,
    action        text NOT NULL,
    product_id    text NOT NULL,
    ip_address    text NOT NULL,
    event_ts      timestamptz NOT NULL,
    batch_id      bigint NOT NULL,
    ingested_at   timestamptz NOT NULL DEFAULT now()
);
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'events_clean'::regclass) THEN
    PERFORM create_distributed_table('events_clean', 'product_id');
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_events_clean_ts ON events_clean (event_ts);

-- events_flagged
CREATE TABLE IF NOT EXISTS events_flagged (
    event_id      text NOT NULL,
    user_id       text NOT NULL,
    action        text NOT NULL,
    product_id    text NOT NULL,
    ip_address    text NOT NULL,
    event_ts      timestamptz NOT NULL,
    batch_id      bigint NOT NULL,
    reason        text NOT NULL,
    ingested_at   timestamptz NOT NULL DEFAULT now()
);
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'events_flagged'::regclass) THEN
    PERFORM create_distributed_table('events_flagged', 'ip_address');
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_events_flagged_ts ON events_flagged (event_ts);

-- events_agg
CREATE TABLE IF NOT EXISTS events_agg (
    window_start  timestamptz NOT NULL,
    product_id    text NOT NULL,
    action        text NOT NULL,
    event_count   bigint NOT NULL,
    PRIMARY KEY (window_start, product_id, action)
);
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'events_agg'::regclass) THEN
    PERFORM create_distributed_table('events_agg', 'product_id');
  END IF;
END $$;

-- pipeline_metrics (coordinator-local)
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id                   bigserial PRIMARY KEY,
    batch_id             bigint NOT NULL,
    batch_ts             timestamptz NOT NULL,
    total_events         integer NOT NULL,
    legit_events         integer NOT NULL,
    flagged_events       integer NOT NULL,
    distinct_ips         integer NOT NULL,
    distinct_flagged_ips integer NOT NULL,
    processing_time_ms   integer NOT NULL,
    created_at           timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_ts ON pipeline_metrics (batch_ts);

CREATE OR REPLACE VIEW v_cluster_nodes AS
SELECT nodename, nodeport, isactive, noderole FROM pg_dist_node;
