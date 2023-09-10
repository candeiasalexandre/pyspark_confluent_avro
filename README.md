# Pyspark Confluent AVRO

This repo contains tests used to write the blogpost ["Exchanging AVRO messages between confluent_kafka & (Py)Spark"](https://candeiasalexandre.github.io/posts/exchanging-avro-messages-confluent_kafka-pyspark/).

## Dependencies

- Spark 3.1.2, you should have spark installed at the system level. To install it on macos you can follow this [tutorial](https://kontext.tech/article/596/apache-spark-301-installation-on-macos).
- Poetry
- Docker

## How to run tests

- Install dependencies above
- `make setup-env`
- `docker-compose up` ... wait till kafka & schema registry is up
- `make tests`

