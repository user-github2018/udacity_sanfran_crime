import logging
import logging.config
from pathlib import Path


import producer_server

BROKER_URL = "localhost:9093"

logger = logging.getLogger(__name__)


def run_kafka_server():
    input_file = "police-department-calls-for-service.json"
    calls_topic_name = "sanfrancisco.police.stats.calls"

    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=calls_topic_name,
        num_partitions=1,
        num_replicas=1,
        bootstrap_servers=f"{BROKER_URL}",
        client_id=f"producer.{calls_topic_name}",
    )

    return producer


def feed():
    try:
        producer = run_kafka_server()
        producer.generate_data()
    except KeyboardInterrupt as e:
        logger.info("Shutting down")
        producer.flush()
        producer.close(5)


if __name__ == "__main__":
    feed()