import json
from typing import Any, Dict, List

import pandas as pd
import pyspark.sql.functions as spark_func
import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro as spark_from_avro
from pyspark.sql.avro.functions import to_avro as spark_to_avro

from pyspark_confluent_avro.confluent_read_write import (
    read_avro_kafka,
    write_avro_kafka,
)
from pyspark_confluent_avro.spark_kafka import KafkaOptions, read_kafka, write_kafka


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


@pytest.fixture()
def example_data(spark_session: SparkSession) -> DataFrame:
    num_rows = 10
    pdf = pd.DataFrame(
        {
            "id": list(range(num_rows)),
            "field_1": [f"field_1{value}" for value in range(num_rows)],
            "field_2": list(range(num_rows)),
            "field_3": [
                [i for i in range(10, 10 + row_number)]
                for row_number in range(num_rows)
            ],
        }
    )

    df = spark_session.createDataFrame(pdf)
    df = df.withColumn("message", spark_func.struct("field_1", "field_2", "field_3"))

    return df


def test_write_spark_read_confluent(
    kafka_topic: KafkaOptions,
    schema_registry_client: SchemaRegistryClient,
    example_schema: Dict[str, Any],
    example_data: DataFrame,
) -> None:
    """
    This test shows that if we write a message to Kafka serialized in avro
    using the pyspark.sql.avro.functions.to_avro function,  we cannot read it in using the confluent_kafka consumer.
    We will get a deserialization error!
    """
    schema_json = json.dumps(example_schema)
    kafka_conf = {
        "bootstrap.servers": kafka_topic.host,
        "compression.type": kafka_topic.compression_type,
        "group.id": "pytest-tests",
        "auto.offset.reset": "earliest",
    }

    df_message_avro = example_data.withColumn(
        "message_avro",
        spark_to_avro(example_data["message"], schema_json),
    )

    write_kafka(df_message_avro, "message_avro", kafka_topic)

    with pytest.raises(SerializationError) as e_info:
        _ = read_avro_kafka(
            kafka_conf,
            kafka_topic.topic,
            schema_registry_client,
            schema_json,
        )

    assert (
        " This message was not produced with a Confluent Schema Registry serializer"
        in str(e_info)
    )


def test_write_confluent_read_spark(
    kafka_topic: KafkaOptions,
    schema_registry_client: SchemaRegistryClient,
    example_schema: Dict[str, Any],
    example_messages: List[Dict[str, Any]],
    spark_session: SparkSession,
) -> None:
    """
    This test shows that if we write a message to Kafka serialized in avro
    using the the confluent_kafka producer, we cannot deserialize it correctly using the
    pyspark.sql.avro.functions.from_avro.
    We will messages populate with default fields, and not the correct data.
    """
    kafka_conf = {
        "bootstrap.servers": kafka_topic.host,
        "compression.type": kafka_topic.compression_type,
        "group.id": "pytest-tests",
        "auto.offset.reset": "earliest",
    }

    schema_json = json.dumps(example_schema)

    write_avro_kafka(
        example_messages,
        kafka_conf,
        kafka_topic.topic,
        schema_registry_client,
        schema_json,
    )

    df_messages_read = read_kafka(spark_session, kafka_topic)
    pdf_messages_read = df_messages_read.withColumn(
        "message",
        spark_from_avro(df_messages_read["value"], schema_json),
    ).toPandas()

    field_1_read_messages = set(
        [x["field_1"] for x in pdf_messages_read["message"].to_list()]
    )
    field_1_original_messages = set([x["field_1"] for x in example_messages])

    assert field_1_original_messages.intersection(field_1_read_messages) == set()


from pyspark_confluent_avro.spark_avro_serde import from_avro as custom_from_avro


def test_write_confluent_read_spark_custom(
    kafka_topic: KafkaOptions,
    schema_registry_client: SchemaRegistryClient,
    example_schema: Dict[str, Any],
    example_messages: List[Dict[str, Any]],
    spark_session: SparkSession,
    schema_registry_config: Dict[str, str],
) -> None:
    """ """
    kafka_conf = {
        "bootstrap.servers": kafka_topic.host,
        "compression.type": kafka_topic.compression_type,
        "group.id": "pytest-tests",
        "auto.offset.reset": "earliest",
    }

    schema_json = json.dumps(example_schema)

    write_avro_kafka(
        example_messages,
        kafka_conf,
        kafka_topic.topic,
        schema_registry_client,
        schema_json,
    )

    df_messages_read = read_kafka(spark_session, kafka_topic)
    pdf_messages_read = df_messages_read.withColumn(
        "message",
        custom_from_avro(
            df_messages_read["value"],
            schema_json,
            {"schema.registry.url": schema_registry_config["url"]},
        ),
    ).toPandas()

    field_1_read_messages = set(
        [x["field_1"] for x in pdf_messages_read["message"].to_list()]
    )
    field_1_original_messages = set([x["field_1"] for x in example_messages])

    assert (
        field_1_original_messages.intersection(field_1_read_messages)
        == field_1_original_messages
    )
