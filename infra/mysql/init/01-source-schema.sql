-- MySQL source system (Hawks side). This is the system of record the
-- NFL platform ingests FROM. Column shapes here are what AWS DMS / Debezium
-- land verbatim into the Postgres "staging" schema.

CREATE DATABASE IF NOT EXISTS hawks
  CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

USE hawks;

-- Patient financial account. business key = account_number.
CREATE TABLE IF NOT EXISTS patient_account (
  account_id        BIGINT        NOT NULL AUTO_INCREMENT,
  account_number    VARCHAR(32)   NULL,
  mrn               VARCHAR(32)   NULL,
  facility_code     VARCHAR(16)   NOT NULL,
  patient_last_name VARCHAR(80)   NULL,
  patient_first_name VARCHAR(80)  NULL,
  guarantor_name    VARCHAR(160)  NULL,
  account_status    VARCHAR(24)   NOT NULL DEFAULT 'OPEN',
  balance_cents     BIGINT        NOT NULL DEFAULT 0,
  opened_at         DATETIME(3)   NOT NULL,
  closed_at         DATETIME(3)   NULL,
  created_at        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                  ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (account_id),
  KEY ix_patient_account_number (account_number),
  KEY ix_patient_account_updated (updated_at)
) ENGINE=InnoDB;

-- A charge/claim transaction posted against an account.
CREATE TABLE IF NOT EXISTS account_transaction (
  transaction_id    BIGINT        NOT NULL AUTO_INCREMENT,
  transaction_ref   VARCHAR(40)   NULL,
  account_number    VARCHAR(32)   NULL,
  facility_code     VARCHAR(16)   NOT NULL,
  transaction_type  VARCHAR(24)   NOT NULL,
  cpt_code          VARCHAR(10)   NULL,
  service_date      DATE          NULL,
  posted_at         DATETIME(3)   NOT NULL,
  amount_cents      BIGINT        NOT NULL,
  payer_name        VARCHAR(120)  NULL,
  created_at        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                  ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (transaction_id),
  KEY ix_account_transaction_ref (transaction_ref),
  KEY ix_account_transaction_acct (account_number),
  KEY ix_account_transaction_updated (updated_at)
) ENGINE=InnoDB;

-- A patient or payer payment applied to the account.
CREATE TABLE IF NOT EXISTS payment (
  payment_id        BIGINT        NOT NULL AUTO_INCREMENT,
  payment_ref       VARCHAR(40)   NULL,
  account_number    VARCHAR(32)   NULL,
  facility_code     VARCHAR(16)   NOT NULL,
  payment_method    VARCHAR(24)   NOT NULL,
  payment_channel   VARCHAR(24)   NOT NULL,
  amount_cents      BIGINT        NOT NULL,
  settled_at        DATETIME(3)   NULL,
  received_at       DATETIME(3)   NOT NULL,
  reversal_of_ref   VARCHAR(40)   NULL,
  created_at        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
                                  ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (payment_id),
  KEY ix_payment_ref (payment_ref),
  KEY ix_payment_acct (account_number),
  KEY ix_payment_updated (updated_at)
) ENGINE=InnoDB;
