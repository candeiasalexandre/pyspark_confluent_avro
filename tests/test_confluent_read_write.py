import json
from typing import Any, Dict, List

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient

from pyspark_confluent_avro.confluent_read_write import (
    read_avro_kafka,
    write_avro_kafka,
)
from pyspark_confluent_avro.spark_kafka import KafkaOptions


@pytest.fixture()
def example_messages() -> List[Dict[str, Any]]:
    num_messages = 10
    return [
        {
            "id": int(msg_num),
            "field_1": f"field_1_{msg_num}",
            "field_2": msg_num,
            "field_3": [i for i in range(10, 10 + msg_num)],
        }
        for msg_num in range(num_messages)
    ]


def test_write_read(
    kafka_topic: KafkaOptions,
    schema_registry_client: SchemaRegistryClient,
    example_schema: Dict[str, Any],
    example_messages: List[Dict[str, Any]],
) -> None:
    kafka_conf = {
        "bootstrap.servers": kafka_topic.host,
        "compression.type": kafka_topic.compression_type,
        "group.id": "test-tailor",
        "auto.offset.reset": "earliest",
    }

    write_avro_kafka(
        example_messages,
        kafka_conf,
        kafka_topic.topic,
        schema_registry_client,
        json.dumps(example_schema),
    )

    read_messages = read_avro_kafka(
        kafka_conf,
        kafka_topic.topic,
        schema_registry_client,
        json.dumps(example_schema),
    )

    assert len(read_messages) == len(example_messages)
