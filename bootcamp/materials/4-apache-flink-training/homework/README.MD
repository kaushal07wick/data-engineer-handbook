# Sessionization Job

This project implements a Flink streaming job to sessionize web events by IP address and host. It also includes SQL analysis to calculate average events per session and compare host performance.

---

## Prerequisites

Before running the job, ensure the following are installed and running:

- Apache Flink (with Python support)
- Kafka broker
- PostgreSQL database
- `psql` command-line client for PostgreSQL

---

## Step 1: Set Environment Variables

Set the required environment variables for Kafka and PostgreSQL:

```bash
export KAFKA_WEB_TRAFFIC_KEY=your_kafka_key        # Kafka username
export KAFKA_WEB_TRAFFIC_SECRET=your_kafka_secret  # Kafka password
export KAFKA_URL=broker:9092                        # Kafka broker address
export KAFKA_TOPIC=web_events                       # Kafka topic to consume
export KAFKA_GROUP=session-job                       # Kafka consumer group ID
export POSTGRES_URL=jdbc:postgresql://db:5432/postgres  # JDBC URL to Postgres
export POSTGRES_USER=postgres                        # Postgres username
export POSTGRES_PASSWORD=postgres                    # Postgres password

Step 2: Run Flink Job

Submit the Flink Python job to process web events:

./bin/flink run -py /opt/src/job/window_job.py

Step 3: Verify Results in PostgreSQL

Query the processed_sessions table to ensure the job ingested data correctly:

psql -h db -U postgres -d postgres -c "SELECT * FROM processed_sessions LIMIT 10;"

Step 4: Run SQL Analysis

Run the SQL script to calculate average events per session and compare hosts:

psql -h db -U postgres -d postgres -f session_analysis.sql