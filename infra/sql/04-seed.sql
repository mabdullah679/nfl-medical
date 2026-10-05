-- MySQL SOURCE seed data. MySQL syntax - this file targets the mysql:8.0.40
-- container, NOT Postgres. (01/02/03 target Postgres.)
--
-- Loaded by the mysql entrypoint as 03-seed.sql (see docker-compose volume
-- mapping) so it runs after the schema and the CDC user.
--
-- ============================ BAD ROWS =====================================
-- Rows deliberately violating quality rules, so the pipeline's failure paths
-- are exercisable rather than theoretical. Every one is tagged BAD-n below.
--
--   BAD-1  patient_account      account_number IS NULL      -> null business key
--   BAD-2  patient_account      account_number duplicated    -> duplicate business key
--   BAD-3  account_transaction  transaction_ref IS NULL      -> null business key
--   BAD-4  account_transaction  transaction_ref duplicated   -> duplicate business key
--   BAD-5  account_transaction  account_number is an orphan  -> referential break
--   BAD-6  payment              payment_ref IS NULL          -> null business key
--   BAD-7  payment              payment_ref duplicated       -> duplicate business key
--
-- Expected effect: utm's NOT NULL + UNIQUE constraints reject these, Hop writes
-- them to utm.ingestion_reject, rows_rejected > 0, and the "Is Failed" gate
-- fails the batch. If a run reports zero rejects, the gate is not wired.
-- ===========================================================================

USE hawks;

SET @t0 = TIMESTAMP('2026-09-01 08:00:00');

-- ---------------------------------------------------------------------------
-- 120 GOOD patient accounts: ACCT-100001 .. ACCT-100120
-- ---------------------------------------------------------------------------
INSERT INTO patient_account
  (account_number, mrn, facility_code, patient_last_name, patient_first_name,
   guarantor_name, account_status, balance_cents, opened_at, closed_at, created_at, updated_at)
WITH RECURSIVE seq(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 120
)
SELECT
  CONCAT('ACCT-', LPAD(100000 + n, 6, '0')),
  CONCAT('MRN', LPAD(n, 7, '0')),
  ELT(1 + (n % 4), 'NFLMED-MAIN', 'NFLMED-MIDTOWN', 'NFLMED-JOHNS', 'NFLMED-DECATUR'),
  ELT(1 + (n % 10), 'Ortiz','Nguyen','Okafor','Patel','Brennan','Silva','Hayes','Kaur','Mbeki','Donovan'),
  ELT(1 + (n % 8),  'Alice','Marcus','Priya','Jonah','Fatima','Dmitri','Grace','Omar'),
  ELT(1 + (n % 3),  'SELF','SPOUSE','PARENT'),
  CASE WHEN n % 11 = 0 THEN 'CLOSED' WHEN n % 7 = 0 THEN 'IN_COLLECTIONS' ELSE 'OPEN' END,
  ((n * 7919) % 450000) + 500,
  @t0 - INTERVAL (n * 3) HOUR,
  CASE WHEN n % 11 = 0 THEN @t0 - INTERVAL n HOUR ELSE NULL END,
  @t0 - INTERVAL (n * 3) HOUR,
  @t0 - INTERVAL (n % 30) HOUR
FROM seq;

-- BAD-1: null business key. Cannot reach utm.dim_patient_account (NOT NULL).
INSERT INTO patient_account
  (account_number, mrn, facility_code, patient_last_name, patient_first_name,
   guarantor_name, account_status, balance_cents, opened_at, created_at, updated_at)
VALUES
  (NULL, 'MRN9000001', 'NFLMED-MAIN', 'Nullkey', 'Testcase',
   'SELF', 'OPEN', 12345, @t0, @t0, @t0);

-- BAD-2: duplicate business key. ACCT-100001 already exists above.
INSERT INTO patient_account
  (account_number, mrn, facility_code, patient_last_name, patient_first_name,
   guarantor_name, account_status, balance_cents, opened_at, created_at, updated_at)
VALUES
  ('ACCT-100001', 'MRN9000002', 'NFLMED-MIDTOWN', 'Dupekey', 'Testcase',
   'SELF', 'OPEN', 54321, @t0, @t0, @t0);

-- ---------------------------------------------------------------------------
-- 200 GOOD transactions: TXN-200001 .. TXN-200200, all on real accounts
-- ---------------------------------------------------------------------------
INSERT INTO account_transaction
  (transaction_ref, account_number, facility_code, transaction_type, cpt_code,
   service_date, posted_at, amount_cents, payer_name, created_at, updated_at)
WITH RECURSIVE seq(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 200
)
SELECT
  CONCAT('TXN-', LPAD(200000 + n, 6, '0')),
  CONCAT('ACCT-', LPAD(100000 + (1 + (n % 120)), 6, '0')),
  ELT(1 + (n % 4), 'NFLMED-MAIN', 'NFLMED-MIDTOWN', 'NFLMED-JOHNS', 'NFLMED-DECATUR'),
  ELT(1 + (n % 4), 'CHARGE','ADJUSTMENT','REFUND','WRITEOFF'),
  ELT(1 + (n % 6), '99213','99214','80053','93000','71046','36415'),
  DATE(@t0 - INTERVAL (n % 45) DAY),
  @t0 - INTERVAL (n * 2) HOUR,
  ((n * 104729) % 180000) + 1000,
  ELT(1 + (n % 5), 'AETNA','BCBS-GA','UNITED','MEDICARE','SELF-PAY'),
  @t0 - INTERVAL (n * 2) HOUR,
  @t0 - INTERVAL (n % 20) HOUR
FROM seq;

-- BAD-3: null business key.
INSERT INTO account_transaction
  (transaction_ref, account_number, facility_code, transaction_type, cpt_code,
   service_date, posted_at, amount_cents, payer_name, created_at, updated_at)
VALUES
  (NULL, 'ACCT-100005', 'NFLMED-MAIN', 'CHARGE', '99213',
   DATE(@t0), @t0, 20000, 'AETNA', @t0, @t0);

-- BAD-4: duplicate business key. TXN-200001 already exists above.
INSERT INTO account_transaction
  (transaction_ref, account_number, facility_code, transaction_type, cpt_code,
   service_date, posted_at, amount_cents, payer_name, created_at, updated_at)
VALUES
  ('TXN-200001', 'ACCT-100006', 'NFLMED-JOHNS', 'ADJUSTMENT', '80053',
   DATE(@t0), @t0, 7500, 'BCBS-GA', @t0, @t0);

-- BAD-5: orphan account_number - no such patient_account row exists.
INSERT INTO account_transaction
  (transaction_ref, account_number, facility_code, transaction_type, cpt_code,
   service_date, posted_at, amount_cents, payer_name, created_at, updated_at)
VALUES
  ('TXN-299999', 'ACCT-999999', 'NFLMED-MAIN', 'CHARGE', '99214',
   DATE(@t0), @t0, 31000, 'UNITED', @t0, @t0);

-- ---------------------------------------------------------------------------
-- 150 GOOD payments: PAY-300001 .. PAY-300150
-- ---------------------------------------------------------------------------
INSERT INTO payment
  (payment_ref, account_number, facility_code, payment_method, payment_channel,
   amount_cents, settled_at, received_at, reversal_of_ref, created_at, updated_at)
WITH RECURSIVE seq(n) AS (
  SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 150
)
SELECT
  CONCAT('PAY-', LPAD(300000 + n, 6, '0')),
  CONCAT('ACCT-', LPAD(100000 + (1 + (n % 120)), 6, '0')),
  ELT(1 + (n % 4), 'NFLMED-MAIN', 'NFLMED-MIDTOWN', 'NFLMED-JOHNS', 'NFLMED-DECATUR'),
  ELT(1 + (n % 4), 'CARD','ACH','CHECK','CASH'),
  ELT(1 + (n % 3), 'PORTAL','IVR','FRONT_DESK'),
  ((n * 48611) % 95000) + 500,
  CASE WHEN n % 9 = 0 THEN NULL ELSE @t0 - INTERVAL (n % 15) HOUR END,
  @t0 - INTERVAL (n * 2) HOUR,
  NULL,
  @t0 - INTERVAL (n * 2) HOUR,
  @t0 - INTERVAL (n % 12) HOUR
FROM seq;

-- BAD-6: null business key.
INSERT INTO payment
  (payment_ref, account_number, facility_code, payment_method, payment_channel,
   amount_cents, settled_at, received_at, created_at, updated_at)
VALUES
  (NULL, 'ACCT-100010', 'NFLMED-MAIN', 'CARD', 'PORTAL',
   4200, @t0, @t0, @t0, @t0);

-- BAD-7: duplicate business key. PAY-300001 already exists above.
INSERT INTO payment
  (payment_ref, account_number, facility_code, payment_method, payment_channel,
   amount_cents, settled_at, received_at, created_at, updated_at)
VALUES
  ('PAY-300001', 'ACCT-100011', 'NFLMED-DECATUR', 'ACH', 'IVR',
   8800, @t0, @t0, @t0, @t0);

-- Expected row counts after seed:
--   patient_account      122  (120 good + BAD-1 + BAD-2)
--   account_transaction  203  (200 good + BAD-3 + BAD-4 + BAD-5)
--   payment              152  (150 good + BAD-6 + BAD-7)
--   TOTAL                477
