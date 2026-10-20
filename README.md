# NFL Medical Data Platform — Pipeline Test Harness

## Recruiter-friendly overview
This project is a local, repeatable demonstration of a healthcare-style reporting pipeline. It moves data through databases, messaging, scheduled workflows, cloud-storage-style systems, report generation, and secure file delivery. It also deliberately injects failures to prove that the system detects problems, reports them clearly, and recovers safely. The project showcases data engineering, workflow orchestration, integration testing, observability, and security-conscious design.

## Project description
A Docker-based test environment that simulates an NFL medical reporting pipeline from source data through replication, ingestion, reporting, monitoring, and SFTP delivery, with automated end-to-end and failure-path tests.

---

# NFL Data Platform: local test harness

A local, runnable stand-in for the NFL reporting pipeline (Hawks MySQL, replication into PostgreSQL,
ingestion into the UTM schema, Parquet extraction to S3, report generation, SFTP delivery, all
orchestrated by Airflow), with a Bruno collection that exercises it end to end and injects failures.

It exists to test the pipeline's contract against the requirements specification before anything is
built in AWS. It is not the production pipeline, and a green run here does not show that the AWS
deployment works. Read "What this does not prove" before relying on a result.

## Start from nothing

Needs Docker with compose v2 or later, `python3`, `curl`, `lsof`, `ssh-keygen`, and floci (the local AWS
emulator) already answering on port 4566. This project does not start floci.

See the existing setup, testing, architecture, coverage, and limitations sections below.
