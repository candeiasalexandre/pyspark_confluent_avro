from typing import Dict
from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column

# taken from https://github.com/AbsaOSS/ABRiS/blob/master/documentation/python-documentation.md


def from_avro(col: Column, config):
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro

    return Column(abris_avro.functions.from_avro(_to_java_column(col), config))


def from_avro_abris_config(config_map: Dict[str, str], topic: str, is_key: bool):
    """
    Args:
        config_map (Dict[str, str]): configuration map to pass to deserializer,
            ex: {'schema.registry.url': 'http://localhost:8081'}
        topic (str): kafka topic name
        is_key (bool, optional): _description_. Defaults to True.

    Returns:
        _type_: FromAvroConfig
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    scala_map = jvm_gateway.PythonUtils.toScalaMap(config_map)

    return (
        jvm_gateway.za.co.absa.abris.config.AbrisConfig.fromConfluentAvro()
        .downloadReaderSchemaByLatestVersion()
        .andTopicNameStrategy(topic, is_key)
        .usingSchemaRegistry(scala_map)
    )


def to_avro(col: Column, config):
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro

    return Column(abris_avro.functions.to_avro(_to_java_column(col), config))


def to_avro_abris_config(config_map: Dict[str, str], topic: str, is_key: bool):
    """
    Args:
        config_map (Dict[str, str]): configuration map to pass to deserializer,
            ex: {'schema.registry.url': 'http://localhost:8081'}
        topic (str): kafka topic name
        is_key (bool, optional): _description_. Defaults to True.

    Returns:
        _type_: ToAvroConfig
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    scala_map = jvm_gateway.PythonUtils.toScalaMap(config_map)

    return (
        jvm_gateway.za.co.absa.abris.config.AbrisConfig.toConfluentAvro()
        .downloadSchemaByLatestVersion()
        .andTopicNameStrategy(topic, is_key)
        .usingSchemaRegistry(scala_map)
    )
