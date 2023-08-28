import os
import sys
from concurrent.futures import wait
from pathlib import Path
from typing import Any, Dict, Generator

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient

from pyspark_confluent_avro.spark_kafka import KafkaOptions


@pytest.fixture(scope="session")
def example_schema() -> Dict[str, Any]:
    return {
        "type": "record",
        "name": "ExampleMessage",
        "namespace": "example.avro",
        "fields": [
            {"name": "field_1", "type": "string"},
            {"name": "field_2", "type": "long"},
            {"name": "field_3", "type": {"type": "array", "items": "long"}},
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


@pytest.fixture()
def kafka_host() -> str:
    return "localhost:29099"


@pytest.fixture()
def kafka_topic(kafka_host: str, tmp_path: Path) -> Generator[KafkaOptions, None, None]:
    client = AdminClient({"bootstrap.servers": kafka_host})

    topic_name = tmp_path.name
    topic_creation = client.create_topics([NewTopic(topic_name)])
    wait(topic_creation.values())

    yield KafkaOptions(kafka_host, topic_name)
    topic_deletion = client.delete_topics([topic_name])
    wait(topic_deletion.values())


def reduce_spark_logging(sc):
    """Reduce logging in SparkContext instance."""

    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.OFF)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.OFF)


@pytest.fixture(scope="session")
def spark_session(tmp_path_factory: pytest.TempPathFactory):
    # this is to make available in spark all the packages present on the env we are running the tests
    os.environ["PYSPARK_PYTHON"] = f"{sys.executable}"

    warehouse_location = tmp_path_factory.mktemp("hive")
    metadata_location = tmp_path_factory.mktemp("metastore")

    _spark_default_config = {
        "spark.default.parallelism": 1,
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.cores": 1,
        "spark.executor.instances": 1,
        "spark.io.compression.codec": "lz4",
        "spark.rdd.compress": "false",
        "spark.sql.shuffle.partitions": 1,
        "spark.shuffle.compress": "false",
        "spark.sql.catalogImplementation": "hive",
        "spark.master": "local[1]",
        "spark.sql.warehouse.dir": str(warehouse_location),
        "spark.driver.extraJavaOptions": f"-Dderby.system.home={str(metadata_location)}",
    }

    try:
        from pyspark.sql import SparkSession
    except ImportError as import_error:
        raise import_error

    session_builder = SparkSession.builder.appName("pyspark-confluent-avro-tests")
    for key, value in _spark_default_config.items():
        session_builder = session_builder.config(key, value)

    session = (
        session_builder.config(
            "spark.jars.packages",
            (
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
                "org.apache.spark:spark-avro_2.12:3.1.2,"
                # package needed for avro ser/deserialization
                # check https://github.com/AbsaOSS/ABRiS
                "za.co.absa:abris_2.12:5.1.1"
            ),
        )
        # adds extra repository needed for confluent packages
        .config("spark.jars.repositories", "https://packages.confluent.io/maven/")
        .enableHiveSupport()
        .getOrCreate()
    )

    # reduce_spark_logging(session.sparkContext)

    yield session
    session.stop()
