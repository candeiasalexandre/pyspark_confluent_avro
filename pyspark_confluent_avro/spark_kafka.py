from dataclasses import dataclass
from typing import Dict

from pyspark.sql import DataFrame, SparkSession


@dataclass
class KafkaOptions:
    host: str
    topic: str
    compression_type: str = "gzip"

    def to_writer_options(self) -> Dict[str, str]:
        return {
            "kafka.bootstrap.servers": self.host,
            "topic": self.topic,
            "kafka.compression.type": self.compression_type,
        }


def write_spark_kafka(df: DataFrame, column: str, options: KafkaOptions) -> None:
    _df = df.withColumnRenamed(column, "value")
    _df = _df.select("value")
    _df.write.format("kafka").options(**options.to_writer_options()).save()


def read_spark_kafka(spark: SparkSession, options: KafkaOptions) -> DataFrame:
    _options = {**options.to_writer_options(), "subscribe": options.topic}
    return spark.read.format("kafka").options(**_options).load()
