import json
from typing import Any, Dict, List

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient

from pyspark_confluent_avro.confluent_read_write import (
    read_avro_confluent_kafka,
    write_avro_confluent_kafka,
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
        "group.id": "pytest-tests",
        "auto.offset.reset": "earliest",
    }

    write_avro_confluent_kafka(
        example_messages,
        kafka_conf,
        kafka_topic.topic,
        schema_registry_client,
        json.dumps(example_schema),
    )

    read_messages = read_avro_confluent_kafka(
        kafka_conf,
        kafka_topic.topic,
        schema_registry_client,
        json.dumps(example_schema),
    )

    field_1_read_messages = set([x["field_1"] for x in read_messages])
    field_1_original_messages = set([x["field_1"] for x in example_messages])

    assert len(read_messages) == len(example_messages)
    assert field_1_read_messages == field_1_original_messages
