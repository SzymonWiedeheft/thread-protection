from datetime import datetime
from typing import Dict, List
from unittest.mock import patch

import pytest

from monitoring.metrics import PrometheusMetrics


@pytest.fixture
def component_statuses() -> Dict[str, bool]:
    return {
        "kafka": True,
        "spark": False,
        "bronze": True,
    }


def gather_samples(registry) -> List:
    samples = []
    for metric in registry.collect():
        samples.extend(metric.samples)
    return samples


def test_push_health_snapshot_creates_expected_metrics(component_statuses: Dict[str, bool]) -> None:
    metrics = PrometheusMetrics(
        pushgateway_url="http://example.com:9091",
        job_name="test_job",
        namespace="example",
        subsystem="ingestion",
        default_labels={"environment": "test", "pipeline": "ingestion"},
    )
    execution_ts = datetime(2024, 1, 1, 12, 0, 0)

    with patch("monitoring.metrics.push_to_gateway") as push_mock:
        metrics.push_health_snapshot(
            component_statuses=component_statuses,
            dag_id="monitoring_dag",
            execution_ts=execution_ts,
            run_id="manual__2024-01-01T12:00:00+00:00",
            duration_seconds=42.0,
        )

    push_mock.assert_called_once()
    kwargs = push_mock.call_args.kwargs
    assert kwargs["job"] == "test_job"
    assert kwargs["grouping_key"]["dag_id"] == "monitoring_dag"

    samples = gather_samples(kwargs["registry"])
    by_name = {}
    for sample in samples:
        by_name.setdefault(sample.name, []).append(sample)

    component_metric_name = "example_ingestion_component_health"
    assert component_metric_name in by_name
    component_values = {
        sample.labels["component"]: sample.value
        for sample in by_name[component_metric_name]
    }
    assert component_values["kafka"] == pytest.approx(1.0)
    assert component_values["spark"] == pytest.approx(0.0)

    overall_metric_name = "example_ingestion_pipeline_health"
    assert by_name[overall_metric_name][0].value == pytest.approx(0.0)

    failed_metric_name = "example_ingestion_pipeline_failed_checks_total"
    assert by_name[failed_metric_name][0].value == pytest.approx(1.0)

    last_run_metric_name = "example_ingestion_pipeline_last_run_timestamp"
    assert by_name[last_run_metric_name][0].value == pytest.approx(
        execution_ts.timestamp()
    )

    duration_metric_name = "example_ingestion_pipeline_run_duration_seconds"
    assert by_name[duration_metric_name][0].value == pytest.approx(42.0)
