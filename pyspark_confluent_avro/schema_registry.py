import json
from typing import Any, Dict

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient


def registry_avro_schema(
    client: SchemaRegistryClient, name: str, schema: Dict[str, Any]
) -> None:
    _schema = Schema(schema_str=json.dumps(schema), schema_type="AVRO")
    client.register_schema(name, _schema)


def get_schema_dict(client: SchemaRegistryClient, name: str) -> Dict[str, Any]:
    schema = client.get_latest_version(name).schema
    return json.loads(schema.schema_str)  # type: ignore
