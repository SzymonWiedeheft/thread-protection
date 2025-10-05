"""
Domain Ingestion DAG

Main workflow for ingesting harmful domains from multiple sources.
Runs hourly to fetch, parse, validate, and publish domains to Kafka.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from custom_operators import (
    IngestionOperator,
    KafkaHealthCheckOperator,
)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

dag = DAG(
    "domain_ingestion_dag",
    default_args=default_args,
    description="Ingest harmful domains from multiple sources",
    schedule_interval="0 * * * *",  # Every hour at minute 0
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "domains", "kafka"],
)

check_kafka = KafkaHealthCheckOperator(
    task_id="check_kafka_health",
    kafka_servers="{{ var.value.kafka_servers }}",
    topics=["malware_domains", "ads_trackers_domains"],
    dag=dag,
)

run_ingestion = IngestionOperator(
    task_id="run_full_ingestion",
    config_path="/opt/ingestion/src/config/sources.yaml",
    kafka_servers="{{ var.value.kafka_servers }}",
    execution_mode="subprocess",
    dag=dag,
)


# Notification function
def send_success_notification(**context):
    """Send success notification."""
    import structlog

    logger = structlog.get_logger()

    execution_date = context["execution_date"]
    task_instance = context["task_instance"]

    # Get ingestion statistics from XCom
    ingestion_result = task_instance.xcom_pull(task_ids="run_full_ingestion")

    logger.info(
        "Ingestion DAG completed successfully",
        execution_date=execution_date.isoformat(),
        result=ingestion_result,
    )


# Task 3: Send success notification
notify_success = PythonOperator(
    task_id="send_success_notification",
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

# Task Dependencies
check_kafka >> run_ingestion >> notify_success


# Alternative DAG with per-source tasks (commented out - use if needed for fine-grained control)
"""
# Create task group for parallel source ingestion
with TaskGroup("fetch_sources", dag=dag) as fetch_sources:

    fetch_stevenblack = IngestionOperator(
        task_id="fetch_stevenblack",
        source_name="stevenblack",
        kafka_servers="{{ var.value.kafka_servers }}",
        execution_mode="subprocess",
    )

    fetch_urlhaus = IngestionOperator(
        task_id="fetch_urlhaus",
        source_name="urlhaus",
        kafka_servers="{{ var.value.kafka_servers }}",
        execution_mode="subprocess",
    )

    fetch_1hosts = IngestionOperator(
        task_id="fetch_1hosts",
        source_name="1hosts",
        kafka_servers="{{ var.value.kafka_servers }}",
        execution_mode="subprocess",
    )

    fetch_adwars = IngestionOperator(
        task_id="fetch_adwars",
        source_name="adwars",
        kafka_servers="{{ var.value.kafka_servers }}",
        execution_mode="subprocess",
    )

# Alternative dependencies for per-source tasks
check_kafka >> fetch_sources >> notify_success
"""
