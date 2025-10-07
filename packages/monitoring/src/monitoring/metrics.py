"""Utilities for publishing monitoring metrics to Prometheus."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping, MutableMapping, Optional

import structlog
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class PrometheusMetrics:
    """Helper to publish pipeline health snapshots to Prometheus Pushgateway."""

    pushgateway_url: Optional[str] = None
    job_name: str = "monitoring_dag"
    namespace: str = "thread_protection"
    subsystem: str = "ingestion"
    default_labels: MutableMapping[str, str] = field(default_factory=dict)
    timeout_seconds: int = 5
    _hostname: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not self.pushgateway_url:
            self.pushgateway_url = os.getenv(
                "PROMETHEUS_PUSHGATEWAY_URL",
                "http://prometheus-pushgateway:9091",
            )
        if not self.default_labels:
            self.default_labels = {
                "environment": os.getenv(
                    "MONITORING_ENVIRONMENT",
                    os.getenv("ENVIRONMENT", "development"),
                ),
                "pipeline": self.subsystem,
            }
        self._hostname = socket.gethostname()

    @property
    def _metric_prefix(self) -> str:
        return f"{self.namespace}_{self.subsystem}".replace("-", "_")

    def _registry(self) -> CollectorRegistry:
        return CollectorRegistry()

    def push_health_snapshot(
        self,
        component_statuses: Mapping[str, bool],
        dag_id: str,
        execution_ts: datetime,
        run_id: Optional[str] = None,
        duration_seconds: Optional[float] = None,
    ) -> None:
        """Push an aggregated health snapshot for the ingestion pipeline."""

        registry = self._registry()

        component_metric = Gauge(
            f"{self._metric_prefix}_component_health",
            "Health status per pipeline component (1=healthy, 0=unhealthy)",
            labelnames=["component", "dag_id", *sorted(self.default_labels)],
            registry=registry,
        )

        labels_common = {"dag_id": dag_id, **self.default_labels}

        for component, healthy in component_statuses.items():
            component_metric.labels(component=component, **labels_common).set(
                1.0 if healthy else 0.0
            )

        overall_metric = Gauge(
            f"{self._metric_prefix}_pipeline_health",
            "Overall pipeline health (1=healthy, 0=unhealthy)",
            labelnames=["dag_id", "run_id", *sorted(self.default_labels)],
            registry=registry,
        )

        all_healthy = all(component_statuses.values()) if component_statuses else False
        overall_metric.labels(
            dag_id=dag_id,
            run_id=run_id or "unknown",
            **self.default_labels,
        ).set(1.0 if all_healthy else 0.0)

        failed_checks_metric = Gauge(
            f"{self._metric_prefix}_pipeline_failed_checks_total",
            "Total number of failed checks in the latest monitoring DAG run",
            labelnames=["dag_id", *sorted(self.default_labels)],
            registry=registry,
        )
        failed_checks_metric.labels(dag_id=dag_id, **self.default_labels).set(
            float(sum(0 if status else 1 for status in component_statuses.values()))
        )

        last_run_metric = Gauge(
            f"{self._metric_prefix}_pipeline_last_run_timestamp",
            "UTC timestamp of the latest monitoring DAG execution",
            labelnames=["dag_id", *sorted(self.default_labels)],
            registry=registry,
        )
        last_run_metric.labels(dag_id=dag_id, **self.default_labels).set(
            execution_ts.timestamp()
        )

        if duration_seconds is not None:
            duration_metric = Gauge(
                f"{self._metric_prefix}_pipeline_run_duration_seconds",
                "Runtime of monitoring DAG run in seconds",
                labelnames=["dag_id", "run_id", *sorted(self.default_labels)],
                registry=registry,
            )
            duration_metric.labels(
                dag_id=dag_id,
                run_id=run_id or "unknown",
                **self.default_labels,
            ).set(duration_seconds)

        grouping_key = {
            "instance": self._hostname,
            "dag_id": dag_id,
        }

        try:
            push_to_gateway(
                self.pushgateway_url,
                job=self.job_name,
                registry=registry,
                grouping_key=grouping_key,
                timeout=self.timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to push metrics to Prometheus Pushgateway",
                pushgateway_url=self.pushgateway_url,
                error=str(exc),
            )

    def record_gauge(
        self,
        name: str,
        value: float,
        description: str,
        labels: Optional[Mapping[str, str]] = None,
    ) -> None:
        """Record a single gauge metric using the Pushgateway."""
        registry = self._registry()
        metric = Gauge(
            name,
            description,
            labelnames=list((labels or {}).keys()),
            registry=registry,
        )
        metric.labels(**(labels or {})).set(value)
        try:
            push_to_gateway(
                self.pushgateway_url,
                job=self.job_name,
                registry=registry,
                timeout=self.timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to push gauge metric",
                metric=name,
                pushgateway_url=self.pushgateway_url,
                error=str(exc),
            )
