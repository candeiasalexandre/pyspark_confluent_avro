from typing import Any, Dict, List
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import MessageField, SerializationContext


def read_avro_kafka(
    conf: Dict[str, str],
    topic_name: str,
    schema_registry_client: SchemaRegistryClient,
    schema_str: str,
    timeout: int = 1,
) -> List[Dict[str, Any]]:
    consumer = Consumer(conf)
    deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=schema_str,
    )

    consumer.subscribe([topic_name])

    messages = []
    timeout_count = 0
    try:
        while timeout_count <= 5:
            message = consumer.poll(timeout=timeout)
            if message is None:
                timeout_count += 1
            else:
                if message.error():
                    raise KafkaException(message.error())
                deserialized_message = deserializer(
                    message.value(),
                    SerializationContext(message.topic(), MessageField.VALUE),
                )
                messages.append(deserialized_message)
                consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    return messages


def write_avro_kafka(
    messages: List[Dict[str, Any]],
    conf: Dict[str, Any],
    topic_name: str,
    schema_registry_client: SchemaRegistryClient,
    schema_str: str,
    timeout: int = 1,
) -> None:

    producer = Producer(conf)
    avro_serializer = AvroSerializer(schema_registry_client, schema_str=schema_str)

    for message in messages:
        producer.poll(0)
        serialized_message = avro_serializer(
            message, SerializationContext(topic_name, MessageField.VALUE)
        )
        producer.produce(
            topic=topic_name,
            value=serialized_message,
        )
    producer.flush(timeout=timeout)
