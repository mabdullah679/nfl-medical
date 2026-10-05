-- Postgres "staging" schema: the DMS/Debezium landing zone.
--
-- Staging mirrors the MySQL source RAW. No business logic, no dedup, no
-- type-tightening beyond what the wire format forces. Business keys stay
-- NULLABLE here on purpose: staging must be able to land a bad source row so
-- the ingestion quality gate has something to actually fail on. Moving the
-- NOT NULL up into staging would hide the defect instead of catching it.
--
-- Every table carries two clocks:
--   updated_at      - source-system mutation time, used for reconciliation
--   dms_loaded_at   - when replication landed the row, used for batch windows

CREATE SCHEMA IF NOT EXISTS staging;

SET search_path TO staging;

CREATE TABLE IF NOT EXISTS staging.patient_account (
  account_id          BIGINT,
  account_number      VARCHAR(32),
  mrn                 VARCHAR(32),
  facility_code       VARCHAR(16),
  patient_last_name   VARCHAR(80),
  patient_first_name  VARCHAR(80),
  guarantor_name      VARCHAR(160),
  account_status      VARCHAR(24),
  balance_cents       BIGINT,
  opened_at           TIMESTAMPTZ,
  closed_at           TIMESTAMPTZ,
  created_at          TIMESTAMPTZ,
  updated_at          TIMESTAMPTZ,

  dms_op              CHAR(1),
  dms_loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  dms_batch_id        TEXT
);

CREATE INDEX IF NOT EXISTS ix_stg_acct_number
  ON staging.patient_account (account_number);
CREATE INDEX IF NOT EXISTS ix_stg_acct_loaded
  ON staging.patient_account (dms_loaded_at);
CREATE INDEX IF NOT EXISTS ix_stg_acct_updated
  ON staging.patient_account (updated_at);

CREATE TABLE IF NOT EXISTS staging.account_transaction (
  transaction_id      BIGINT,
  transaction_ref     VARCHAR(40),
  account_number      VARCHAR(32),
  facility_code       VARCHAR(16),
  transaction_type    VARCHAR(24),
  cpt_code            VARCHAR(10),
  service_date        DATE,
  posted_at           TIMESTAMPTZ,
  amount_cents        BIGINT,
  payer_name          VARCHAR(120),
  created_at          TIMESTAMPTZ,
  updated_at          TIMESTAMPTZ,

  dms_op              CHAR(1),
  dms_loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  dms_batch_id        TEXT
);

CREATE INDEX IF NOT EXISTS ix_stg_txn_ref
  ON staging.account_transaction (transaction_ref);
CREATE INDEX IF NOT EXISTS ix_stg_txn_acct
  ON staging.account_transaction (account_number);
CREATE INDEX IF NOT EXISTS ix_stg_txn_loaded
  ON staging.account_transaction (dms_loaded_at);

CREATE TABLE IF NOT EXISTS staging.payment (
  payment_id          BIGINT,
  payment_ref         VARCHAR(40),
  account_number      VARCHAR(32),
  facility_code       VARCHAR(16),
  payment_method      VARCHAR(24),
  payment_channel     VARCHAR(24),
  amount_cents        BIGINT,
  settled_at          TIMESTAMPTZ,
  received_at         TIMESTAMPTZ,
  reversal_of_ref     VARCHAR(40),
  created_at          TIMESTAMPTZ,
  updated_at          TIMESTAMPTZ,

  dms_op              CHAR(1),
  dms_loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  dms_batch_id        TEXT
);

CREATE INDEX IF NOT EXISTS ix_stg_pay_ref
  ON staging.payment (payment_ref);
CREATE INDEX IF NOT EXISTS ix_stg_pay_acct
  ON staging.payment (account_number);
CREATE INDEX IF NOT EXISTS ix_stg_pay_loaded
  ON staging.payment (dms_loaded_at);

-- Replication heartbeat. Debezium's heartbeat.action.query stamps one row in
-- MySQL hawks.replication_heartbeat every heartbeat.interval.ms; that change
-- travels the same binlog -> Kafka -> JDBC sink path as data and lands here as
-- a NEW row per beat. beat_at is the source-side write time, so now() -
-- max(beat_at) is end-to-end staleness on an idle source. No primary key: the
-- sink is at-least-once and a redelivered beat must not fail the sink task.
CREATE TABLE IF NOT EXISTS staging.replication_heartbeat (
  source_name   TEXT        NOT NULL,
  beat_at       TIMESTAMPTZ NOT NULL,
  dms_op        CHAR(1)
);

-- Converge a volume created before the heartbeat was wired.
ALTER TABLE staging.replication_heartbeat DROP CONSTRAINT IF EXISTS replication_heartbeat_pkey;
ALTER TABLE staging.replication_heartbeat ALTER COLUMN beat_at DROP DEFAULT;
ALTER TABLE staging.replication_heartbeat ADD COLUMN IF NOT EXISTS dms_op CHAR(1);
CREATE INDEX IF NOT EXISTS ix_stg_heartbeat_source_beat
  ON staging.replication_heartbeat (source_name, beat_at DESC);
