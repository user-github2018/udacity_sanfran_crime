from kafka import KafkaProducer
import logging
import json
import time
import datetime

from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)
logging.getLogger("kafka.conn").setLevel('ERROR')



class KakfaClient:
    def __init__(self, bootstrap_servers):
        self.client = AdminClient({"bootstrap.servers": bootstrap_servers})

    def topic_exists(self, topic_name):
        """Checks if the given topic exists"""
        topic_metadata = self.client.list_topics(timeout=5)
        return topic_metadata.topics.get(topic_name) is not None

    def create_topic(self, topic_name, num_partitions, num_replicas):
        """Creates the producer topic"""
        futures = self.client.create_topics(
            [NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=num_replicas)]
        )
        for _, future in futures.items():
            try:
                future.result()
                logger.info(f"kafka topic {topic_name} created")
            except Exception as e:
                logger.info(f"topic creation kafka failed {topic_name} with exception: {e}")

class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, num_partitions, num_replicas, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        client = KakfaClient(kwargs.get('bootstrap_servers'))
        exists = client.topic_exists(topic)
        if exists is False:
            client.create_topic(self.topic, num_partitions, num_replicas)

    def read_file(self):
        """
        Read json file of calls.
        """
        with open(self.input_file, 'r') as f:
            data = json.load(f)
        return data

    def generate_data(self):
        records = self.read_file()
        logger.info(f"started producing data for kafka topic {self.topic}")
        for i in records:
            message = self.dict_to_binary(i)
            self.send(self.topic, message)
            curr_time = datetime.datetime.utcnow().replace(microsecond=0)
            logger.info(f"{len(message)} bytes sent at {curr_time.isoformat()}")
            time.sleep(1)

    @staticmethod
    def dict_to_binary(json_dict):
        return json.dumps(json_dict).encode("utf-8")
        