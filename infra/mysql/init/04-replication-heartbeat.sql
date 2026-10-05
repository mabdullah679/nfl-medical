-- Source side of the end-to-end replication heartbeat.
--
-- Debezium's heartbeat.action.query (infra/debezium/mysql-source.json) stamps
-- this one row every heartbeat.interval.ms. The change is captured from the
-- binlog like any data change and lands as a new row in Postgres
-- staging.replication_heartbeat, so the age of the newest landed beat measures
-- MySQL -> Kafka -> Postgres even when no business row changes.
--
-- Applied by scripts/up.sh on every start, before the connectors are
-- registered. The CREATE TABLE is deliberately binlogged: on a stack that was
-- already streaming, Debezium learns the table from that DDL event.

CREATE TABLE IF NOT EXISTS hawks.replication_heartbeat (
  source_name VARCHAR(64) NOT NULL,
  beat_at     DATETIME(3) NOT NULL,
  PRIMARY KEY (source_name)
) ENGINE=InnoDB;

-- The CDC account may write this one table and nothing else; its SELECT on
-- hawks.* and replication privileges are in 02-debezium-user.sql.tmpl.
SET SESSION sql_log_bin = 0;
GRANT INSERT, UPDATE ON hawks.replication_heartbeat TO 'debezium'@'%';
