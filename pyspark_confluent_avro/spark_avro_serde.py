from typing import Dict

from pyspark import SparkContext
from pyspark.sql.column import (  # type: ignore
    Column,
    _to_java_column,
)

# taken from https://github.com/AbsaOSS/ABRiS/blob/master/documentation/python-documentation.md


class AbrisFromAvroConfig:
    def __init__(self, original_object) -> None:  # noqa: ANN001
        self.original_object = original_object

    def unwrap(self):  # noqa: ANN201
        return self.original_object


class AbrisToAvroConfig:
    def __init__(self, original_object) -> None:  # noqa: ANN001
        self.original_object = original_object

    def unwrap(self):  # noqa: ANN201
        return self.original_object


def from_avro(col: Column, config: AbrisFromAvroConfig) -> Column:
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm  # type: ignore
    abris_avro = jvm_gateway.za.co.absa.abris.avro

    return Column(abris_avro.functions.from_avro(_to_java_column(col), config.unwrap()))


def from_avro_abris_config(
    config_map: Dict[str, str], topic: str, is_key: bool
) -> AbrisFromAvroConfig:
    """
    Args:
        config_map (Dict[str, str]): configuration map to pass to deserializer,
            ex: {'schema.registry.url': 'http://localhost:8081'}
        topic (str): kafka topic name
        is_key (bool, optional): _description_. Defaults to True.

    Returns:
        _type_: FromAvroConfig
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm  # type: ignore
    scala_map = jvm_gateway.PythonUtils.toScalaMap(config_map)

    return AbrisFromAvroConfig(
        jvm_gateway.za.co.absa.abris.config.AbrisConfig.fromConfluentAvro()
        .downloadReaderSchemaByLatestVersion()
        .andTopicNameStrategy(topic, is_key)
        .usingSchemaRegistry(scala_map)
    )


def to_avro(col: Column, config: AbrisToAvroConfig) -> Column:
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm  # type: ignore
    abris_avro = jvm_gateway.za.co.absa.abris.avro

    return Column(abris_avro.functions.to_avro(_to_java_column(col), config.unwrap()))


def to_avro_abris_config(
    config_map: Dict[str, str], topic: str, is_key: bool
) -> AbrisToAvroConfig:
    """
    Args:
        config_map (Dict[str, str]): configuration map to pass to deserializer,
            ex: {'schema.registry.url': 'http://localhost:8081'}
        topic (str): kafka topic name
        is_key (bool, optional): _description_. Defaults to True.

    Returns:
        _type_: ToAvroConfig
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm  # type: ignore
    scala_map = jvm_gateway.PythonUtils.toScalaMap(config_map)

    return AbrisToAvroConfig(
        jvm_gateway.za.co.absa.abris.config.AbrisConfig.toConfluentAvro()
        .downloadSchemaByLatestVersion()
        .andTopicNameStrategy(topic, is_key)
        .usingSchemaRegistry(scala_map)
    )
