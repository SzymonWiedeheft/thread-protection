# Orchestration Service

Apache Airflow-based orchestration for the domain ingestion pipeline.

## Overview

This service provides workflow orchestration using Apache Airflow. It schedules and monitors the execution of the domain ingestion pipeline, ensuring reliable data processing and alerting on failures.

## Components

### DAGs

#### 1. `domain_ingestion_dag.py`
**Purpose:** Main workflow for ingesting harmful domains from multiple sources

**Schedule:** Every hour (`0 * * * *`)

**Tasks:**
- `check_kafka_health` - Verify Kafka broker connectivity
- `run_full_ingestion` - Execute the ingestion service
- `send_success_notification` - Notify on successful completion

**Flow:**
```
check_kafka_health → run_full_ingestion → send_success_notification
```

#### 2. `monitoring_dag.py`
**Purpose:** Monitor pipeline health and data freshness

**Schedule:** Every 15 minutes (`*/15 * * * *`)

**Task Groups:**
- **Infrastructure Health:**
  - `check_kafka` - Verify Kafka brokers
  - `check_spark` - Verify Spark jobs

- **Data Quality:**
  - `check_bronze_freshness` - Verify Bronze layer freshness (< 30 min)
  - `check_silver_freshness` - Verify Silver layer freshness (< 60 min)
  - `check_gold_freshness` - Verify Gold layer freshness (< 120 min)

**Tasks:**
- `collect_metrics` - Aggregate monitoring results
- `alert_on_failure` - Send alerts if checks fail

### Custom Operators

Located in `plugins/custom_operators.py`:

#### `IngestionOperator`
Runs the domain ingestion service either as:
- Subprocess (default)
- Docker container

**Parameters:**
- `source_name` - Specific source to ingest (None = all sources)
- `config_path` - Path to sources.yaml
- `kafka_servers` - Kafka bootstrap servers
- `execution_mode` - 'subprocess' or 'docker'

#### `KafkaHealthCheckOperator`
Verifies Kafka broker health and topic availability.

**Parameters:**
- `kafka_servers` - Kafka bootstrap servers
- `topics` - List of required topics

#### `DataFreshnessCheckOperator`
Checks Delta Lake table freshness.

**Parameters:**
- `delta_path` - Path to Delta table
- `max_age_minutes` - Maximum allowed age

#### `SparkJobCheckOperator`
Monitors Spark Structured Streaming jobs.

**Parameters:**
- `spark_master` - Spark master URL
- `job_name` - Specific job to check (None = all)

## Setup

### 1. Install Dependencies

```bash
cd services/orchestration
poetry install
```

### 2. Configure Airflow Variables

Set the following Airflow variables:

```bash
airflow variables set kafka_servers "kafka:9092"
airflow variables set spark_master "spark://spark-master:7077"
airflow variables set bronze_path "s3a://delta-lake/bronze"
airflow variables set silver_path "s3a://delta-lake/silver"
airflow variables set gold_path "s3a://delta-lake/gold"
```

### 3. Deploy DAGs

Copy DAGs to Airflow DAGs folder:

```bash
cp dags/*.py $AIRFLOW_HOME/dags/
cp plugins/*.py $AIRFLOW_HOME/plugins/
```

Or use Docker volume mount:

```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ./plugins:/opt/airflow/plugins
```

## Running DAGs

### Trigger Manually

```bash
# Trigger ingestion DAG
airflow dags trigger domain_ingestion_dag

# Trigger monitoring DAG
airflow dags trigger monitoring_dag
```

### Test DAG

```bash
# Test domain ingestion DAG
airflow dags test domain_ingestion_dag 2025-10-05

# Test monitoring DAG
airflow dags test monitoring_dag 2025-10-05
```

### Backfill

```bash
# Backfill ingestion DAG for date range
airflow dags backfill domain_ingestion_dag \
  --start-date 2025-10-01 \
  --end-date 2025-10-05
```

## Testing

Run DAG validation tests:

```bash
cd services/orchestration
poetry run pytest tests/ -v
```

## Monitoring

### Airflow UI

Access the Airflow web UI at `http://localhost:8090`

**Credentials:** admin / admin

### DAG Monitoring

- **Graph View:** Visualize task dependencies
- **Tree View:** View historical runs
- **Gantt View:** Analyze task durations
- **Log View:** Debug task failures

### Metrics

The monitoring DAG collects metrics for:
- Kafka broker health
- Spark job status
- Data freshness across Bronze/Silver/Gold layers

**TODO:** Integrate with Prometheus for metric export

## Configuration

### DAG Parameters

Edit DAG files to customize:
- Schedule intervals
- Retry policies
- Timeouts
- Notification settings

### Airflow Configuration

Key settings in `airflow.cfg`:

```ini
[core]
dags_folder = /opt/airflow/dags
plugins_folder = /opt/airflow/plugins
executor = LocalExecutor

[scheduler]
dag_dir_list_interval = 30
catchup_by_default = False
```

## Troubleshooting

### DAG Not Appearing

1. Check DAG folder path
2. Verify no Python syntax errors
3. Check Airflow scheduler logs

```bash
airflow dags list
airflow dags list-import-errors
```

### Task Failures

1. Check task logs in Airflow UI
2. Verify Kafka/Spark connectivity
3. Check ingestion service logs

```bash
airflow tasks test domain_ingestion_dag run_full_ingestion 2025-10-05
```

### Kafka Connection Issues

1. Verify Kafka broker is running
2. Check network connectivity
3. Verify topic exists

```bash
# Test Kafka connectivity
kafka-topics --bootstrap-server kafka:9092 --list
```

## Development

### Adding New Tasks

1. Create task in DAG file:

```python
new_task = PythonOperator(
    task_id='new_task',
    python_callable=my_function,
    dag=dag,
)
```

2. Define dependencies:

```python
check_kafka >> new_task >> run_ingestion
```

### Creating Custom Operators

1. Add operator to `plugins/custom_operators.py`:

```python
class MyOperator(BaseOperator):
    def __init__(self, param1, **kwargs):
        super().__init__(**kwargs)
        self.param1 = param1

    def execute(self, context):
        # Implementation
        pass
```

2. Use in DAG:

```python
from custom_operators import MyOperator

task = MyOperator(
    task_id='my_task',
    param1='value',
    dag=dag,
)
```

## Best Practices

1. **Idempotency:** Tasks should be idempotent (safe to retry)
2. **Timeouts:** Set reasonable execution timeouts
3. **Retries:** Configure retries with exponential backoff
4. **Monitoring:** Monitor DAG success rates and durations
5. **Alerts:** Configure email/Slack alerts for failures
6. **Testing:** Test DAGs before deploying to production

## References

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Writing DAGs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html)
- [Custom Operators](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html)
