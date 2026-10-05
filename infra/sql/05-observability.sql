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
    CHECK (stage IN ('replication','ingestion','extraction','reporting','delivery')),
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
