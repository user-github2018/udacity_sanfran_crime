import time
from confluent_kafka import Consumer

BROKER_URL = "PLAINTEXT://localhost:9093"


def consume(topic_name, earliest=True):
    """Consumes data from the Kafka Topic"""
    offset = "earliest" if earliest else "latest"
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0", "auto.offset.reset": offset})
    c.subscribe([topic_name])

    while True:
        messages = c.consume(5, timeout=1.0)
        for message in messages:
            print(f"consume message {message.key()}: {message.value()}")
        time.sleep(1)


def main():
    try:
        consume("sanfrancisco.police.stats.calls")
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()
        