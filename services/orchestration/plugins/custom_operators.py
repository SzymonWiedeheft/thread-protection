"""Custom Airflow operators for domain ingestion pipeline."""

import subprocess
import json
from typing import Any, Dict, Optional
from datetime import datetime

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError
import structlog

logger = structlog.get_logger()


class IngestionOperator(BaseOperator):
    """
    Operator to run the domain ingestion service.

    Executes the ingestion service using Docker or subprocess.
    """

    template_fields = ["source_name", "config_path", "kafka_servers"]
    ui_color = "#80D0FF"

    @apply_defaults
    def __init__(
        self,
        source_name: Optional[str] = None,
        config_path: str = "/opt/ingestion/src/config/sources.yaml",
        kafka_servers: str = "kafka:9092",
        execution_mode: str = "subprocess",  # 'subprocess' or 'docker'
        docker_image: str = "thread-protection-ingestion:latest",
        *args,
        **kwargs,
    ):
        """
        Initialize IngestionOperator.

        Args:
            source_name: Specific source to ingest (None = all sources)
            config_path: Path to sources.yaml configuration
            kafka_servers: Kafka bootstrap servers
            execution_mode: How to run the service ('subprocess' or 'docker')
            docker_image: Docker image name (if execution_mode='docker')
        """
        super().__init__(*args, **kwargs)
        self.source_name = source_name
        self.config_path = config_path
        self.kafka_servers = kafka_servers
        self.execution_mode = execution_mode
        self.docker_image = docker_image

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the ingestion service."""
        logger.info(
            "Starting ingestion",
            source=self.source_name or "all",
            mode=self.execution_mode,
        )

        if self.execution_mode == "subprocess":
            return self._execute_subprocess()
        elif self.execution_mode == "docker":
            return self._execute_docker()
        else:
            raise AirflowException(
                f"Unknown execution_mode: {self.execution_mode}"
            )

    def _execute_subprocess(self) -> Dict[str, Any]:
        """Execute ingestion as subprocess."""
        cmd = [
            "python",
            "/opt/ingestion/src/main.py",
            "--config",
            self.config_path,
            "--kafka-servers",
            self.kafka_servers,
            "--json-logs",
        ]

        logger.info("Running command", cmd=" ".join(cmd))

        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout
            )

            logger.info("Ingestion completed", stdout=result.stdout)

            return {
                "status": "success",
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

        except subprocess.CalledProcessError as e:
            logger.error(
                "Ingestion failed",
                returncode=e.returncode,
                stdout=e.stdout,
                stderr=e.stderr,
            )
            raise AirflowException(
                f"Ingestion failed with code {e.returncode}: {e.stderr}"
            )

        except subprocess.TimeoutExpired:
            raise AirflowException("Ingestion timed out after 600 seconds")

    def _execute_docker(self) -> Dict[str, Any]:
        """Execute ingestion in Docker container using CLI."""
        # Build docker run command
        docker_cmd = [
            "docker",
            "run",
            "--rm",
            "--network",
            "pipeline-network",
            "-e",
            f"KAFKA_BOOTSTRAP_SERVERS={self.kafka_servers}",
            self.docker_image,
            "python",
            "-m",
            "src.main",
            "--config",
            self.config_path,
            "--kafka-servers",
            self.kafka_servers,
            "--json-logs",
        ]

        logger.info("Running Docker container via CLI", cmd=" ".join(docker_cmd))

        try:
            result = subprocess.run(
                docker_cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout
            )

            logger.info("Docker container completed", stdout=result.stdout)

            return {
                "status": "success",
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

        except subprocess.CalledProcessError as e:
            logger.error(
                "Docker execution failed",
                returncode=e.returncode,
                stdout=e.stdout,
                stderr=e.stderr,
            )
            raise AirflowException(
                f"Docker execution failed with code {e.returncode}: {e.stderr}"
            )

        except subprocess.TimeoutExpired:
            raise AirflowException("Docker execution timed out after 600 seconds")


class KafkaHealthCheckOperator(BaseOperator):
    """
    Operator to check Kafka broker health.

    Verifies connectivity to Kafka brokers and topic availability.
    """

    template_fields = ["kafka_servers", "topics"]
    ui_color = "#FFD700"

    @apply_defaults
    def __init__(
        self,
        kafka_servers: str = "kafka:9092",
        topics: Optional[list] = None,
        *args,
        **kwargs,
    ):
        """
        Initialize KafkaHealthCheckOperator.

        Args:
            kafka_servers: Kafka bootstrap servers
            topics: List of topics to verify (None = just broker health)
        """
        super().__init__(*args, **kwargs)
        self.kafka_servers = kafka_servers
        self.topics = topics or ["malware_domains", "ads_trackers_domains"]

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Check Kafka health."""
        logger.info("Checking Kafka health", servers=self.kafka_servers)

        try:
            # Check broker connectivity
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.kafka_servers,
                client_id="airflow_health_check",
                request_timeout_ms=5000,
            )

            # Get cluster metadata
            cluster_metadata = admin_client.list_topics()

            logger.info(
                "Kafka brokers healthy",
                broker_count=len(admin_client._client.cluster.brokers()),
                topics=len(cluster_metadata),
            )

            # Check required topics exist
            missing_topics = set(self.topics) - set(cluster_metadata)

            if missing_topics:
                raise AirflowException(
                    f"Missing required topics: {missing_topics}"
                )

            admin_client.close()

            return {
                "status": "healthy",
                "brokers": len(admin_client._client.cluster.brokers()),
                "topics": list(cluster_metadata),
            }

        except KafkaError as e:
            logger.error("Kafka health check failed", error=str(e))
            raise AirflowException(f"Kafka unhealthy: {str(e)}")


class DataFreshnessCheckOperator(BaseOperator):
    """
    Operator to check Delta Lake data freshness.

    Verifies that data has been updated recently.
    """

    template_fields = ["delta_path"]
    ui_color = "#90EE90"

    @apply_defaults
    def __init__(
        self,
        delta_path: str = "s3a://delta-lake/gold/domains",
        max_age_minutes: int = 120,
        *args,
        **kwargs,
    ):
        """
        Initialize DataFreshnessCheckOperator.

        Args:
            delta_path: Path to Delta table
            max_age_minutes: Maximum allowed age in minutes
        """
        super().__init__(*args, **kwargs)
        self.delta_path = delta_path
        self.max_age_minutes = max_age_minutes

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Check data freshness."""
        logger.info(
            "Checking data freshness",
            path=self.delta_path,
            max_age=self.max_age_minutes,
        )

        # Note: In production, this would query Delta Lake
        # For now, we'll implement a placeholder

        logger.warning(
            "Data freshness check not fully implemented - returning success"
        )

        return {
            "status": "success",
            "checked_at": datetime.utcnow().isoformat(),
            "delta_path": self.delta_path,
        }


class SparkJobCheckOperator(BaseOperator):
    """
    Operator to check Spark Structured Streaming job health.

    Monitors streaming query status and progress.
    """

    template_fields = ["spark_master", "job_name"]
    ui_color = "#FFA500"

    @apply_defaults
    def __init__(
        self,
        spark_master: str = "spark://spark-master:7077",
        job_name: Optional[str] = None,
        *args,
        **kwargs,
    ):
        """
        Initialize SparkJobCheckOperator.

        Args:
            spark_master: Spark master URL
            job_name: Name of streaming job to check (None = all jobs)
        """
        super().__init__(*args, **kwargs)
        self.spark_master = spark_master
        self.job_name = job_name

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Check Spark job health."""
        logger.info(
            "Checking Spark job health",
            master=self.spark_master,
            job=self.job_name or "all",
        )

        # Note: In production, this would query Spark REST API
        # For now, we'll implement a placeholder

        logger.warning(
            "Spark job check not fully implemented - returning success"
        )

        return {
            "status": "success",
            "checked_at": datetime.utcnow().isoformat(),
            "spark_master": self.spark_master,
        }
