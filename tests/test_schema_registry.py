from typing import Any, Dict
from confluent_kafka.schema_registry import SchemaRegistryClient
import pytest

from pyspark_confluent_avro.schema_registry import get_schema_dict, registry_avro_schema


@pytest.fixture()
def example_schema() -> Dict[str, Any]:
    return {
        "type": "record",
        "name": "ExampleMessage",
        "namespace": "example.avro",
        "fields": [
            {"name": "field_1", "type": "string"},
            {"name": "field_2", "type": "int"},
            {"name": "filed_4", "type": {"type": "array", "items": "int"}},
        ],
    }


@pytest.fixture()
def schema_registry_config() -> Dict[str, str]:
    return {
        "url": "http://localhost:8081",
        "basic.auth.user.info": ":",
    }


@pytest.fixture()
def schema_registry_client(
    schema_registry_config: Dict[str, str]
) -> SchemaRegistryClient:
    client = SchemaRegistryClient(schema_registry_config)
    return client


def test_populate_schema_registry(
    schema_registry_client: SchemaRegistryClient, example_schema: Dict[str, Any]
) -> None:
    schema_name = "test_populate_schema_registry"
    registry_avro_schema(schema_registry_client, schema_name, example_schema)
    retrieved_schema = get_schema_dict(schema_registry_client, schema_name)

    assert retrieved_schema == example_schema
