-- Postgres "utm" schema: the modelled layer Apache Hop builds from staging.
--
-- Differences from staging, all deliberate:
--   * business keys are NOT NULL + UNIQUE - this is the constraint the
--     ingestion quality gate exists to protect. A staging row with a null or
--     duplicate business key cannot land here; it is rejected to the reject
--     table and counted by the "Is Failed" check.
--   * surrogate keys are generated here, not carried from the source.
--   * batch lineage columns so every utm row traces to the Hop run that made it.

CREATE SCHEMA IF NOT EXISTS utm;

SET search_path TO utm;

-- One row per Hop ingestion run. The "Is Failed" gate reads this.
CREATE TABLE IF NOT EXISTS utm.ingestion_batch (
  batch_id          BIGSERIAL   PRIMARY KEY,
  batch_key         TEXT        NOT NULL UNIQUE,
  window_start      TIMESTAMPTZ NOT NULL,
  window_end        TIMESTAMPTZ NOT NULL,
  started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at       TIMESTAMPTZ,
  status            TEXT        NOT NULL DEFAULT 'RUNNING',
  rows_read         BIGINT      NOT NULL DEFAULT 0,
  rows_loaded       BIGINT      NOT NULL DEFAULT 0,
  rows_rejected     BIGINT      NOT NULL DEFAULT 0,
  failure_reason    TEXT,
  CONSTRAINT ck_ingestion_batch_status
    CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
  CONSTRAINT ck_ingestion_batch_window
    CHECK (window_end > window_start)
);

CREATE INDEX IF NOT EXISTS ix_utm_batch_status ON utm.ingestion_batch (status);
CREATE INDEX IF NOT EXISTS ix_utm_batch_window ON utm.ingestion_batch (window_start, window_end);

CREATE TABLE IF NOT EXISTS utm.dim_patient_account (
  account_sk          BIGSERIAL     PRIMARY KEY,
  account_number      VARCHAR(32)   NOT NULL UNIQUE,
  mrn                 VARCHAR(32),
  facility_code       VARCHAR(16)   NOT NULL,
  patient_last_name   VARCHAR(80),
  patient_first_name  VARCHAR(80),
  guarantor_name      VARCHAR(160),
  account_status      VARCHAR(24)   NOT NULL,
  balance_cents       BIGINT        NOT NULL,
  opened_at           TIMESTAMPTZ   NOT NULL,
  closed_at           TIMESTAMPTZ,
  source_updated_at   TIMESTAMPTZ   NOT NULL,
  utm_batch_id        BIGINT        NOT NULL REFERENCES utm.ingestion_batch (batch_id),
  utm_loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
  CONSTRAINT ck_dim_acct_balance_sane CHECK (balance_cents > -100000000000)
);

CREATE INDEX IF NOT EXISTS ix_utm_acct_batch   ON utm.dim_patient_account (utm_batch_id);
CREATE INDEX IF NOT EXISTS ix_utm_acct_updated ON utm.dim_patient_account (source_updated_at);

CREATE TABLE IF NOT EXISTS utm.fact_account_transaction (
  transaction_sk      BIGSERIAL     PRIMARY KEY,
  transaction_ref     VARCHAR(40)   NOT NULL UNIQUE,
  account_number      VARCHAR(32)   NOT NULL,
  account_sk          BIGINT        REFERENCES utm.dim_patient_account (account_sk),
  facility_code       VARCHAR(16)   NOT NULL,
  transaction_type    VARCHAR(24)   NOT NULL,
  cpt_code            VARCHAR(10),
  service_date        DATE,
  posted_at           TIMESTAMPTZ   NOT NULL,
  amount_cents        BIGINT        NOT NULL,
  payer_name          VARCHAR(120),
  source_updated_at   TIMESTAMPTZ   NOT NULL,
  utm_batch_id        BIGINT        NOT NULL REFERENCES utm.ingestion_batch (batch_id),
  utm_loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_utm_txn_acct    ON utm.fact_account_transaction (account_number);
CREATE INDEX IF NOT EXISTS ix_utm_txn_batch   ON utm.fact_account_transaction (utm_batch_id);
CREATE INDEX IF NOT EXISTS ix_utm_txn_posted  ON utm.fact_account_transaction (posted_at);

CREATE TABLE IF NOT EXISTS utm.fact_payment (
  payment_sk          BIGSERIAL     PRIMARY KEY,
  payment_ref         VARCHAR(40)   NOT NULL UNIQUE,
  account_number      VARCHAR(32)   NOT NULL,
  account_sk          BIGINT        REFERENCES utm.dim_patient_account (account_sk),
  facility_code       VARCHAR(16)   NOT NULL,
  payment_method      VARCHAR(24)   NOT NULL,
  payment_channel     VARCHAR(24)   NOT NULL,
  amount_cents        BIGINT        NOT NULL,
  settled_at          TIMESTAMPTZ,
  received_at         TIMESTAMPTZ   NOT NULL,
  reversal_of_ref     VARCHAR(40),
  source_updated_at   TIMESTAMPTZ   NOT NULL,
  utm_batch_id        BIGINT        NOT NULL REFERENCES utm.ingestion_batch (batch_id),
  utm_loaded_at       TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_utm_pay_acct   ON utm.fact_payment (account_number);
CREATE INDEX IF NOT EXISTS ix_utm_pay_batch  ON utm.fact_payment (utm_batch_id);
CREATE INDEX IF NOT EXISTS ix_utm_pay_recvd  ON utm.fact_payment (received_at);

-- Rows Hop could not promote. The "Is Failed" gate counts these; a non-empty
-- reject set for a batch is what makes that gate fail rather than pass silently.
CREATE TABLE IF NOT EXISTS utm.ingestion_reject (
  reject_id       BIGSERIAL   PRIMARY KEY,
  utm_batch_id    BIGINT      NOT NULL REFERENCES utm.ingestion_batch (batch_id),
  source_table    TEXT        NOT NULL,
  reject_reason   TEXT        NOT NULL,
  business_key    TEXT,
  raw_row         JSONB       NOT NULL,
  rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_utm_reject_batch  ON utm.ingestion_reject (utm_batch_id);
CREATE INDEX IF NOT EXISTS ix_utm_reject_reason ON utm.ingestion_reject (reject_reason);

-- utm.v_reconciliation was removed: nothing read it, and as an owner-rights
-- view it let any role with SELECT on utm read staging counts. Reconciliation
-- is the shim's /checks/ingestion/reconcile, which reads the tables directly.
DROP VIEW IF EXISTS utm.v_reconciliation;
