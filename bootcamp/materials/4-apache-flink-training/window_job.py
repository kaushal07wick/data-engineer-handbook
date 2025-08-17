from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session
import os


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    kafka_url = os.environ.get("KAFKA_URL")
    kafka_topic = os.environ.get("KAFKA_TOPIC")
    kafka_group = os.environ.get("KAFKA_GROUP")

    # --- Source table from Kafka ---
    t_env.execute_sql("DROP TABLE IF EXISTS source_table")
    t_env.execute_sql(f"""
        CREATE TABLE source_table (
            ip STRING,
            host STRING,
            url STRING,
            user_agent STRING,
            status STRING,
            referrer STRING,
            event_time STRING,
            event_ts AS TO_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''),
            WATERMARK FOR event_ts AS event_ts - INTERVAL '15' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{kafka_url}',
            'topic' = '{kafka_topic}',
            'properties.group.id' = '{kafka_group}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_key}" password="{kafka_secret}";',
            'properties.ssl.endpoint.identification.algorithm' = '',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        )
    """)

    # --- Sink table in Postgres ---
    t_env.execute_sql("DROP TABLE IF EXISTS processed_sessions")
    t_env.execute_sql(f"""
        CREATE TABLE processed_sessions (
            ip STRING,
            host STRING,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            event_count BIGINT,
            PRIMARY KEY (ip, host, session_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = 'processed_sessions',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # --- Session windowing ---
    source = t_env.from_path("source_table")
    result = (
        source.window(Session.with_gap(lit(5).minutes).on(col("event_ts")).alias("w"))
              .group_by(col("ip"), col("host"), col("w"))
              .select(
                  col("ip"),
                  col("host"),
                  col("w").start.alias("session_start"),
                  col("w").end.alias("session_end"),
                  col("*").count.alias("event_count")   # ✅ fixed
              )
    )

    # --- Write to Postgres ---
    result.execute_insert("processed_sessions")


if __name__ == "__main__":
    main()
