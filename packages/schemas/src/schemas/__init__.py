"""Schemas package."""

from schemas.domain import DomainModel
from schemas.kafka_message import KafkaMessageSchema
from schemas.validation_rules import is_valid_domain, clean_domain

__all__ = [
    "DomainModel",
    "KafkaMessageSchema",
    "is_valid_domain",
    "clean_domain",
]

# Delta schemas rely on PySpark, which might not be installed in every
# runtime (e.g. Airflow when the ingestion service runs as a subprocess).
# Import lazily so that non-Spark consumers can continue to use the rest of
# the package without pulling in pyspark.
try:
    from schemas import delta_schemas  # type: ignore
except (
    ModuleNotFoundError
) as delta_import_error:  # pragma: no cover - only hit in non-Spark envs
    print(f"Module '{delta_import_error}' not found.")

    class _DeltaSchemasProxy:
        """Proxy that raises a helpful error if Delta helpers are accessed without pyspark."""

        def __getattr__(self, item: str):
            raise ModuleNotFoundError(
                "schemas.delta_schemas requires pyspark to be installed in this environment. "
                "Install pyspark (e.g. pip install pyspark==3.5.*) before using Delta helpers."
            )

    delta_schemas = _DeltaSchemasProxy()  # type: ignore[assignment]
else:  # pragma: no cover - exercised in Spark-enabled environments
    __all__.append("delta_schemas")
