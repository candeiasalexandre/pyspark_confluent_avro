import json
from typing import Any, Dict

import pandas as pd
import pyspark.sql.functions as spark_func
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro as spark_from_avro
from pyspark.sql.avro.functions import to_avro as spark_to_avro

from pyspark_confluent_avro.spark_kafka import KafkaOptions, read_kafka, write_kafka


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


def test_write_spark_avro(
    spark_session: SparkSession,
    example_data: DataFrame,
    example_schema: Dict[str, Any],
    kafka_topic: KafkaOptions,
) -> None:
    schema_json = json.dumps(example_schema)
    df_message_avro = example_data.withColumn(
        "message_avro",
        spark_to_avro(example_data["message"], schema_json),
    )

    write_kafka(df_message_avro, "message_avro", kafka_topic)

    df_messages_read = read_kafka(spark_session, kafka_topic)
    df_messages_read = df_messages_read.withColumn(
        "message",
        spark_from_avro(df_messages_read["value"], schema_json),
    )

    pdf_messages_read = df_messages_read.select("message").toPandas()

    assert len(pdf_messages_read) == 10
