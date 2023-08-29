from typing import Dict
from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column


def to_avro(col: Column, schema_str: str, schema_id: int) -> Column:
    """
    Serializes rows in column to Avro in confluent format
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    config = _create_to_avro_abris_config(schema_str, schema_id)
    return Column(abris_avro.functions.to_avro(_to_java_column(col), config))


def from_avro(
    col: Column, schema_str: str, schema_registry_config: Dict[str, str]
) -> Column:
    """
    Deserializes rows in column in Avro Confluent format to a struct
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    config = _create_from_avro_abris_config(schema_str, schema_registry_config)
    return Column(abris_avro.functions.from_avro(_to_java_column(col), config))


def _create_to_avro_abris_config(schema_json: str, schema_id: int):
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    avro_config = (
        jvm_gateway.za.co.absa.abris.config.ToAvroConfig(
            jvm_gateway.PythonUtils.toScalaMap({})
        )
        .withSchema(schema_json)
        .withSchemaId(schema_id)
    )
    return avro_config


def _create_from_avro_abris_config(schema_json, schema_registry_config: Dict[str, str]):
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    avro_config = (
        jvm_gateway.za.co.absa.abris.config.FromAvroConfig(
            jvm_gateway.PythonUtils.toScalaMap({}), jvm_gateway.scala.Option.apply(None)
        )
        .withReaderSchema(schema_json)
        .withSchemaRegistryConfig(
            jvm_gateway.PythonUtils.toScalaMap(schema_registry_config)
        )
    )
    return avro_config
