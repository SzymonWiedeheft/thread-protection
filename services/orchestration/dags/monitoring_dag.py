"""
Monitoring DAG

Health check and monitoring workflow for the domain ingestion pipeline.
Runs every 15 minutes to monitor Kafka, Delta Lake, and Spark jobs.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from custom_operators import (
    KafkaHealthCheckOperator,
    DataFreshnessCheckOperator,
    SparkJobCheckOperator,
)
from monitoring import PrometheusMetrics


# Default arguments for all tasks
default_args = {
    "owner": "sre",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}

# DAG definition
dag = DAG(
    "monitoring_dag",
    default_args=default_args,
    description="Monitor pipeline health and data freshness",
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    catchup=False,
    max_active_runs=1,
    tags=["monitoring", "health-check", "sre"],
)


# Alert function
def alert_on_failure(**context):
    """Send alert on monitoring failure."""
    import structlog

    logger = structlog.get_logger()

    execution_date = context["execution_date"]
    failed_tasks = context["task_instance"].xcom_pull(
        task_ids=None, key="failed_checks"
    )

    logger.error(
        "Monitoring checks failed",
        execution_date=execution_date.isoformat(),
        failed_tasks=failed_tasks,
    )


# Task Group: Infrastructure Health Checks
with TaskGroup("infrastructure_health", dag=dag) as infrastructure_health:

    check_kafka = KafkaHealthCheckOperator(
        task_id="check_kafka",
        kafka_servers="{{ var.value.kafka_servers }}",
        topics=["malware_domains", "ads_trackers_domains"],
        dag=dag,
    )

    check_spark = SparkJobCheckOperator(
        task_id="check_spark",
        spark_master="{{ var.value.spark_master }}",
        dag=dag,
    )


# Task Group: Data Quality Checks
with TaskGroup("data_quality", dag=dag) as data_quality:

    check_bronze_freshness = DataFreshnessCheckOperator(
        task_id="check_bronze_freshness",
        delta_path="{{ var.value.bronze_path }}",
        max_age_minutes=30,
        dag=dag,
    )

    check_silver_freshness = DataFreshnessCheckOperator(
        task_id="check_silver_freshness",
        delta_path="{{ var.value.silver_path }}",
        max_age_minutes=60,
        dag=dag,
    )

    check_gold_freshness = DataFreshnessCheckOperator(
        task_id="check_gold_freshness",
        delta_path="{{ var.value.gold_path }}",
        max_age_minutes=120,
        dag=dag,
    )


# Task: Collect monitoring metrics
def collect_metrics(**context):
    """Collect and record monitoring metrics."""
    import structlog

    logger = structlog.get_logger()

    task_instance = context["task_instance"]
    dag_run = context.get("dag_run")
    dag = context.get("dag")

    # Pull results from all health checks
    kafka_health = task_instance.xcom_pull(
        task_ids="infrastructure_health.check_kafka"
    )
    spark_health = task_instance.xcom_pull(
        task_ids="infrastructure_health.check_spark"
    )
    bronze_health = task_instance.xcom_pull(
        task_ids="data_quality.check_bronze_freshness"
    )
    silver_health = task_instance.xcom_pull(
        task_ids="data_quality.check_silver_freshness"
    )
    gold_health = task_instance.xcom_pull(task_ids="data_quality.check_gold_freshness")

    def _is_success(result):
        if isinstance(result, dict):
            status = result.get("status")
            if isinstance(status, str):
                return status.lower() == "success"
        return bool(result)

    component_statuses = {
        "kafka": _is_success(kafka_health),
        "spark": _is_success(spark_health),
        "bronze": _is_success(bronze_health),
        "silver": _is_success(silver_health),
        "gold": _is_success(gold_health),
    }

    failed_components = [
        component for component, healthy in component_statuses.items() if not healthy
    ]
    all_healthy = not failed_components

    collected_at = datetime.utcnow()

    metrics = {
        "kafka": kafka_health,
        "spark": spark_health,
        "bronze": bronze_health,
        "silver": silver_health,
        "gold": gold_health,
        "summary": {
            "all_healthy": all_healthy,
            "failed_components": failed_components,
        },
        "timestamp": collected_at.isoformat(),
    }

    logger.info("Monitoring metrics collected", metrics=metrics)

    prometheus = PrometheusMetrics(
        job_name="airflow_monitoring_dag",
        subsystem="ingestion",
        default_labels={
            "environment": os.getenv(
                "MONITORING_ENVIRONMENT",
                os.getenv("ENVIRONMENT", "development"),
            ),
            "pipeline": "domain_ingestion",
        },
    )

    start_date = getattr(task_instance, "start_date", None)
    duration_seconds = None
    if start_date:
        try:
            start_ts = start_date.timestamp()
        except AttributeError:
            start_ts = None
        if start_ts is None:
            try:
                start_ts = datetime.fromisoformat(str(start_date)).timestamp()
            except ValueError:
                start_ts = None
        if start_ts is not None:
            duration_seconds = max(0.0, collected_at.timestamp() - start_ts)

    prometheus.push_health_snapshot(
        component_statuses=component_statuses,
        dag_id=dag.dag_id if dag else "monitoring_dag",
        execution_ts=collected_at,
        run_id=dag_run.run_id if dag_run else None,
        duration_seconds=duration_seconds,
    )

    return metrics


collect_metrics_task = PythonOperator(
    task_id="collect_metrics",
    python_callable=collect_metrics,
    provide_context=True,
    dag=dag,
)


# Task: Alert on failure
alert_task = PythonOperator(
    task_id="alert_on_failure",
    python_callable=alert_on_failure,
    provide_context=True,
    trigger_rule="one_failed",  # Only runs if any upstream task fails
    dag=dag,
)


# Task Dependencies
[infrastructure_health, data_quality] >> collect_metrics_task
[infrastructure_health, data_quality] >> alert_task
