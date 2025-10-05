"""Tests for Airflow DAGs."""

import pytest
import ast
from pathlib import Path


def test_domain_ingestion_dag_syntax():
    """Test that domain ingestion DAG has valid Python syntax."""
    dag_file = Path(__file__).parent.parent / "dags" / "domain_ingestion_dag.py"

    with open(dag_file, "r") as f:
        code = f.read()

    # Should not raise SyntaxError
    ast.parse(code)

    # Check for key elements
    assert "domain_ingestion_dag" in code
    assert "check_kafka_health" in code
    assert "run_full_ingestion" in code
    assert "schedule_interval=\"0 * * * *\"" in code or "schedule_interval='0 * * * *'" in code


def test_monitoring_dag_syntax():
    """Test that monitoring DAG has valid Python syntax."""
    dag_file = Path(__file__).parent.parent / "dags" / "monitoring_dag.py"

    with open(dag_file, "r") as f:
        code = f.read()

    # Should not raise SyntaxError
    ast.parse(code)

    # Check for key elements
    assert "monitoring_dag" in code
    assert "infrastructure_health" in code
    assert "data_quality" in code
    assert "schedule_interval=\"*/15 * * * *\"" in code or "schedule_interval='*/15 * * * *'" in code


def test_custom_operators_syntax():
    """Test that custom operators have valid Python syntax."""
    operators_file = Path(__file__).parent.parent / "plugins" / "custom_operators.py"

    with open(operators_file, "r") as f:
        code = f.read()

    # Should not raise SyntaxError
    ast.parse(code)

    # Check for key operators
    assert "IngestionOperator" in code
    assert "KafkaHealthCheckOperator" in code
    assert "DataFreshnessCheckOperator" in code
    assert "SparkJobCheckOperator" in code


def test_domain_ingestion_dag_structure():
    """Test domain ingestion DAG structure."""
    dag_file = Path(__file__).parent.parent / "dags" / "domain_ingestion_dag.py"

    with open(dag_file, "r") as f:
        code = f.read()

    # Parse AST
    tree = ast.parse(code)

    # Find DAG definition
    dag_found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "dag":
                    dag_found = True
                    break

    assert dag_found, "DAG object 'dag' not found in domain_ingestion_dag.py"


def test_monitoring_dag_structure():
    """Test monitoring DAG structure."""
    dag_file = Path(__file__).parent.parent / "dags" / "monitoring_dag.py"

    with open(dag_file, "r") as f:
        code = f.read()

    # Parse AST
    tree = ast.parse(code)

    # Find DAG definition
    dag_found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "dag":
                    dag_found = True
                    break

    assert dag_found, "DAG object 'dag' not found in monitoring_dag.py"


def test_domain_ingestion_dag_imports():
    """Test that domain ingestion DAG has required imports."""
    dag_file = Path(__file__).parent.parent / "dags" / "domain_ingestion_dag.py"

    with open(dag_file, "r") as f:
        code = f.read()

    # Check for required imports
    assert "from airflow import DAG" in code
    assert "from custom_operators import" in code
    assert "datetime" in code


def test_monitoring_dag_imports():
    """Test that monitoring DAG has required imports."""
    dag_file = Path(__file__).parent.parent / "dags" / "monitoring_dag.py"

    with open(dag_file, "r") as f:
        code = f.read()

    # Check for required imports
    assert "from airflow import DAG" in code
    assert "from custom_operators import" in code
    assert "TaskGroup" in code


def test_dag_files_exist():
    """Test that all required DAG files exist."""
    dags_dir = Path(__file__).parent.parent / "dags"

    assert (dags_dir / "__init__.py").exists()
    assert (dags_dir / "domain_ingestion_dag.py").exists()
    assert (dags_dir / "monitoring_dag.py").exists()


def test_plugin_files_exist():
    """Test that all required plugin files exist."""
    plugins_dir = Path(__file__).parent.parent / "plugins"

    assert (plugins_dir / "custom_operators.py").exists()


def test_domain_ingestion_dag_default_args():
    """Test domain ingestion DAG has default args defined."""
    dag_file = Path(__file__).parent.parent / "dags" / "domain_ingestion_dag.py"

    with open(dag_file, "r") as f:
        code = f.read()

    # Check for default args
    assert "default_args = {" in code
    assert '"owner"' in code or "'owner'" in code
    assert '"retries"' in code or "'retries'" in code


def test_monitoring_dag_default_args():
    """Test monitoring DAG has default args defined."""
    dag_file = Path(__file__).parent.parent / "dags" / "monitoring_dag.py"

    with open(dag_file, "r") as f:
        code = f.read()

    # Check for default args
    assert "default_args = {" in code
    assert '"owner"' in code or "'owner'" in code
    assert '"retries"' in code or "'retries'" in code


def test_domain_ingestion_dag_tags():
    """Test domain ingestion DAG has appropriate tags."""
    dag_file = Path(__file__).parent.parent / "dags" / "domain_ingestion_dag.py"

    with open(dag_file, "r") as f:
        code = f.read()

    # Check for tags
    assert "tags=" in code
    assert "ingestion" in code


def test_monitoring_dag_tags():
    """Test monitoring DAG has appropriate tags."""
    dag_file = Path(__file__).parent.parent / "dags" / "monitoring_dag.py"

    with open(dag_file, "r") as f:
        code = f.read()

    # Check for tags
    assert "tags=" in code
    assert "monitoring" in code
