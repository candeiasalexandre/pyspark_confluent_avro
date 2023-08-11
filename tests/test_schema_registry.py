from typing import Any, Dict
from confluent_kafka.schema_registry import SchemaRegistryClient

from pyspark_confluent_avro.schema_registry import get_schema_dict, registry_avro_schema


def test_populate_schema_registry(
    schema_registry_client: SchemaRegistryClient, example_schema: Dict[str, Any]
) -> None:
    schema_name = "test_populate_schema_registry"
    registry_avro_schema(schema_registry_client, schema_name, example_schema)
    retrieved_schema = get_schema_dict(schema_registry_client, schema_name)

    assert retrieved_schema == example_schema
