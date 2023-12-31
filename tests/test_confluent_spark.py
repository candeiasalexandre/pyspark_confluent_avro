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
    read_avro_confluent_kafka,
    write_avro_confluent_kafka,
)
from pyspark_confluent_avro.spark_avro_serde import from_avro as custom_from_avro
from pyspark_confluent_avro.spark_avro_serde import (
    from_avro_abris_config,
    to_avro_abris_config,
)
from pyspark_confluent_avro.spark_avro_serde import to_avro as custom_to_avro
from pyspark_confluent_avro.spark_kafka import (
    KafkaOptions,
    read_spark_kafka,
    write_spark_kafka,
)


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
            "field_1": [f"field_1_{value}" for value in range(num_rows)],
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

    write_spark_kafka(df_message_avro, "message_avro", kafka_topic)

    with pytest.raises(SerializationError) as e_info:
        _ = read_avro_confluent_kafka(
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

    write_avro_confluent_kafka(
        example_messages,
        kafka_conf,
        kafka_topic.topic,
        schema_registry_client,
        schema_json,
    )

    df_messages_read = read_spark_kafka(spark_session, kafka_topic)
    pdf_messages_read = df_messages_read.withColumn(
        "message",
        spark_from_avro(df_messages_read["value"], schema_json),
    ).toPandas()

    field_1_read_messages = set(
        [x["field_1"] for x in pdf_messages_read["message"].to_list()]
    )
    field_1_original_messages = set([x["field_1"] for x in example_messages])

    assert field_1_original_messages.intersection(field_1_read_messages) == set()


def test_write_confluent_read_spark_custom(
    kafka_topic: KafkaOptions,
    schema_registry_client: SchemaRegistryClient,
    example_schema: Dict[str, Any],
    example_messages: List[Dict[str, Any]],
    spark_session: SparkSession,
    schema_registry_config: Dict[str, str],
) -> None:
    """
    Test that shows correctness if we use the Abris package to read a confluent Avro serialized message.
    IF we use Abris from_avro, we get the correct data when reading.
    """
    kafka_conf = {
        "bootstrap.servers": kafka_topic.host,
        "compression.type": kafka_topic.compression_type,
        "group.id": "pytest-tests",
        "auto.offset.reset": "earliest",
    }
    schema_json = json.dumps(example_schema)

    write_avro_confluent_kafka(
        example_messages,
        kafka_conf,
        kafka_topic.topic,
        schema_registry_client,
        schema_json,
    )

    df_messages_read = read_spark_kafka(spark_session, kafka_topic)
    abris_config = from_avro_abris_config(
        {"schema.registry.url": schema_registry_config["url"]}, kafka_topic.topic, False
    )
    pdf_messages_read = df_messages_read.withColumn(
        "message",
        custom_from_avro(df_messages_read["value"], abris_config),
    ).toPandas()

    field_1_read_messages = set(
        [x["field_1"] for x in pdf_messages_read["message"].to_list()]
    )
    field_1_original_messages = set([x["field_1"] for x in example_messages])

    assert (
        field_1_original_messages.intersection(field_1_read_messages)
        == field_1_original_messages
    )


def test_write_spark_custom_read_confluent(
    kafka_topic: KafkaOptions,
    schema_registry_client: SchemaRegistryClient,
    example_schema: Dict[str, Any],
    example_data: DataFrame,
    schema_registry_config: Dict[str, str],
    example_messages: List[Dict[str, Any]],
) -> None:
    """
    Test that shows correctness if we use the Abris package to write an Avro serialized message.
    If we serialize the messages with Abris, when we read from kafka using kafka_confluent we
    get the right data.
    """
    schema_json = json.dumps(example_schema)
    kafka_conf = {
        "bootstrap.servers": kafka_topic.host,
        "compression.type": kafka_topic.compression_type,
        "group.id": "pytest-tests",
        "auto.offset.reset": "earliest",
    }

    # this will register the schema automatically
    # check https://github.com/AbsaOSS/ABRiS
    abris_config = to_avro_abris_config(
        {"schema.registry.url": schema_registry_config["url"]},
        kafka_topic.topic,
        False,
        schema_json=schema_json,
    )
    df_message_avro = example_data.withColumn(
        "message_avro",
        custom_to_avro(example_data["message"], abris_config),
    )
    write_spark_kafka(df_message_avro, "message_avro", kafka_topic)

    read_messages = read_avro_confluent_kafka(
        kafka_conf,
        kafka_topic.topic,
        schema_registry_client,
        schema_json,
    )

    field_1_read_messages = set([x["field_1"] for x in read_messages])
    field_1_original_messages = set([x["field_1"] for x in example_messages])

    assert len(read_messages) == len(example_messages)
    assert field_1_read_messages == field_1_original_messages
