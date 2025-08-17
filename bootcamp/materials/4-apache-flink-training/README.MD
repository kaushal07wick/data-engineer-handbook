# Sessionization Job

This document provides instructions to run the Flink sessionization job, verify results, and perform analysis on sessionized web events.

## Step 1: Export Required Environment Variables
# Set Kafka credentials, topic info, and Postgres connection details.
export KAFKA_WEB_TRAFFIC_KEY=your_kafka_key        # Kafka username
export KAFKA_WEB_TRAFFIC_SECRET=your_kafka_secret  # Kafka password
export KAFKA_URL=broker:9092                        # Kafka broker address
export KAFKA_TOPIC=web_events                       # Kafka topic to consume
export KAFKA_GROUP=session-job                       # Kafka consumer group ID
export POSTGRES_URL=jdbc:postgresql://db:5432/postgres  # JDBC URL to Postgres
export POSTGRES_USER=postgres                        # Postgres username
export POSTGRES_PASSWORD=postgres                    # Postgres password

## Step 2: Submit Flink Job
# Run the Flink Python job to sessionize web events
./bin/flink run -py /opt/src/job/window_job.py

## Step 3: Verify Data in Postgres
# Query the first 10 rows from the processed_sessions table to confirm data ingestion
psql -h db -U postgres -d postgres -c "SELECT * FROM processed_sessions LIMIT 10;"

## Step 4: Run Analysis
# Execute SQL script to calculate average events per session and compare hosts
psql -h db -U postgres -d postgres -f session_analysis.sql
