-- Postgres "observability" schema: the pipeline's audit trail (stage 6).
--
-- Every DAG step writes one row here per state change. /checks/lineage on the
-- shim reads it to answer "did every stage run for this interval, and how did
-- the latest attempt end". It is the only place lineage is recorded, so a
-- stage that does not write here is reported as NOT RUN - never assumed.
--
-- Applied by scripts/up.sh on every start (idempotent), not by the postgres
-- entrypoint: init scripts only run against an empty data volume, and this
-- table must also converge on a stack whose volume predates a change to it.

CREATE SCHEMA IF NOT EXISTS observability;

CREATE TABLE IF NOT EXISTS observability.pipeline_event (
  event_id        BIGSERIAL   PRIMARY KEY,
  stage           TEXT        NOT NULL,
  step            TEXT        NOT NULL,
  status          TEXT        NOT NULL,
  -- NULL only on a failure alert whose run conf yielded no interval.
  interval_start  DATE,
  interval_end    DATE,
  event_time      TIMESTAMPTZ NOT NULL DEFAULT now(),
  dag_id          TEXT,
  run_id          TEXT,
  metrics         JSONB       NOT NULL DEFAULT '{}'::jsonb,
  message         TEXT,
  CONSTRAINT ck_pipeline_event_stage
    CHECK (stage IN ('replication','ingestion','extraction','reporting','delivery','platform')),
  -- 'failed' is terminal for the attempt. A gate that will be tried again
  -- writes 'retry', so a retried-then-successful stage is not read as failed.
  CONSTRAINT ck_pipeline_event_status
    CHECK (status IN ('started','success','retry','failed','skipped')),
  CONSTRAINT ck_pipeline_event_interval
    CHECK (interval_end >= interval_start)
);

ALTER TABLE observability.pipeline_event
  ALTER COLUMN interval_start DROP NOT NULL,
  ALTER COLUMN interval_end   DROP NOT NULL;

CREATE INDEX IF NOT EXISTS ix_obs_event_interval
  ON observability.pipeline_event (interval_start, stage, event_time);
CREATE INDEX IF NOT EXISTS ix_obs_event_run
  ON observability.pipeline_event (run_id);

-- ---------------------------------------------------------------------------
-- Correlation: one id carried by every event of one pipeline attempt, across
-- the three DAGs and the watcher. The caller passes it as conf.correlation_id
-- to each trigger; a run without one gets nfl_<start>_<end>, the thread of its
-- interval. Rows written before this column existed keep NULL.
-- ---------------------------------------------------------------------------
ALTER TABLE observability.pipeline_event
  ADD COLUMN IF NOT EXISTS correlation_id TEXT;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_pipeline_event_correlation') THEN
    -- Letters, digits and underscore only: the same token is a CloudWatch Logs
    -- filter pattern, which needs quoting for anything else.
    ALTER TABLE observability.pipeline_event
      ADD CONSTRAINT ck_pipeline_event_correlation
      CHECK (correlation_id ~ '^[A-Za-z0-9_]{8,96}$');
  END IF;
END $$;

-- 'platform' is not a pipeline stage: it is where the watcher records the
-- health of what runs the stages (the orchestrator). /checks/lineage counts
-- only the five stages.
ALTER TABLE observability.pipeline_event DROP CONSTRAINT IF EXISTS ck_pipeline_event_stage;
ALTER TABLE observability.pipeline_event
  ADD CONSTRAINT ck_pipeline_event_stage
  CHECK (stage IN ('replication','ingestion','extraction','reporting','delivery','platform'));

CREATE INDEX IF NOT EXISTS ix_obs_event_correlation
  ON observability.pipeline_event (correlation_id, event_time);

-- ---------------------------------------------------------------------------
-- Watcher samples: what nfl-watch measured on each tick, whether or not anyone
-- asked. A gap in this series is itself the signal that the watcher stopped.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.monitor_sample (
  sample_id    BIGSERIAL   PRIMARY KEY,
  sampled_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  probe        TEXT        NOT NULL,
  -- NULL: the probe could not run, which is neither healthy nor breached.
  healthy      BOOLEAN,
  value        DOUBLE PRECISION,
  threshold    DOUBLE PRECISION,
  detail       JSONB       NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT ck_monitor_sample_probe
    CHECK (probe IN ('replication_heartbeat','connector_status','orchestrator'))
);

CREATE INDEX IF NOT EXISTS ix_obs_monitor_probe_time
  ON observability.monitor_sample (probe, sampled_at DESC);

-- ---------------------------------------------------------------------------
-- Alert delivery: one row per attempt to hand an alert to a channel. An alert
-- with no 'sent' row here was recorded but reached nobody.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS observability.alert_delivery (
  delivery_id   BIGSERIAL   PRIMARY KEY,
  event_id      BIGINT      NOT NULL REFERENCES observability.pipeline_event (event_id),
  channel       TEXT        NOT NULL,
  recipients    TEXT        NOT NULL,
  status        TEXT        NOT NULL,
  attempt       INTEGER     NOT NULL,
  attempted_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  message_id    TEXT,
  detail        TEXT,
  CONSTRAINT ck_alert_delivery_status CHECK (status IN ('sent','failed')),
  CONSTRAINT ck_alert_delivery_attempt CHECK (attempt >= 1),
  CONSTRAINT uq_alert_delivery_attempt UNIQUE (event_id, channel, attempt)
);

-- At most one successful delivery per alert and channel: a second dispatcher,
-- or a retry racing a slow send, cannot produce a duplicate notification row.
CREATE UNIQUE INDEX IF NOT EXISTS uq_alert_delivery_sent
  ON observability.alert_delivery (event_id, channel) WHERE status = 'sent';
