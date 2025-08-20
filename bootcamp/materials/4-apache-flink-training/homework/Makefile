# Makefile for Flink Sessionization Job

# Default environment variables (can be overridden)
KAFKA_KEY ?= your_kafka_key
KAFKA_SECRET ?= your_kafka_secret
KAFKA_URL ?= broker:9092
KAFKA_TOPIC ?= web_events
KAFKA_GROUP ?= session-job

POSTGRES_URL ?= jdbc:postgresql://db:5432/postgres
POSTGRES_USER ?= postgres
POSTGRES_PASSWORD ?= postgres

FLINK_JOB_PATH ?= /opt/src/job/window_job.py
SQL_ANALYSIS_PATH ?= session_analysis.sql

.PHONY: run-job verify analyze

# Run the Flink job
run-job:
	@echo "Submitting Flink sessionization job..."
	KAFKA_WEB_TRAFFIC_KEY=$(KAFKA_KEY) \
	KAFKA_WEB_TRAFFIC_SECRET=$(KAFKA_SECRET) \
	KAFKA_URL=$(KAFKA_URL) \
	KAFKA_TOPIC=$(KAFKA_TOPIC) \
	KAFKA_GROUP=$(KAFKA_GROUP) \
	POSTGRES_URL=$(POSTGRES_URL) \
	POSTGRES_USER=$(POSTGRES_USER) \
	POSTGRES_PASSWORD=$(POSTGRES_PASSWORD) \
	./bin/flink run -py $(FLINK_JOB_PATH)

# Verify top 10 rows in Postgres
verify:
	@echo "Verifying top 10 sessions in Postgres..."
	PGPASSWORD=$(POSTGRES_PASSWORD) \
	psql -h db -U $(POSTGRES_USER) -d postgres -c "SELECT * FROM processed_sessions LIMIT 10;"

# Run SQL analysis script
analyze:
	@echo "Running SQL analysis..."
	PGPASSWORD=$(POSTGRES_PASSWORD) \
	psql -h db -U $(POSTGRES_USER) -d postgres -f $(SQL_ANALYSIS_PATH)
