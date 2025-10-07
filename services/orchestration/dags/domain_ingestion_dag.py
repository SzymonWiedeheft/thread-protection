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
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}

dag = DAG(
    "domain_ingestion_dag",
    default_args=default_args,
    description="Ingest harmful domains from multiple sources",
    schedule_interval="@daily",  # Every hour at minute 0
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
    config_path="src/config/sources.yaml",
    kafka_servers="{{ var.value.kafka_servers }}",
    execution_mode="docker",
    docker_image="thread-protection-ingestion:latest",
    dag=dag,
)


def send_success_notification(**context):
    """Send success notification."""
    import structlog

    logger = structlog.get_logger()

    execution_date = context["execution_date"]
    task_instance = context["task_instance"]

    ingestion_result = task_instance.xcom_pull(task_ids="run_full_ingestion")

    logger.info(
        "Ingestion DAG completed successfully",
        execution_date=execution_date.isoformat(),
        result=ingestion_result,
    )


notify_success = PythonOperator(
    task_id="send_success_notification",
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

check_kafka >> run_ingestion >> notify_success