import pytest
from typing import Any, Dict
from confluent_kafka.schema_registry import SchemaRegistryClient


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def schema_registry_config() -> Dict[str, str]:
    return {
        "url": "http://localhost:8081",
        "basic.auth.user.info": ":",
    }


@pytest.fixture(scope="session")
def schema_registry_client(
    schema_registry_config: Dict[str, str]
) -> SchemaRegistryClient:
    client = SchemaRegistryClient(schema_registry_config)
    return client
