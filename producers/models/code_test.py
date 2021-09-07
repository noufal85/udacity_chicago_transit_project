from loguru import logger
from pathlib import Path

from confluent_kafka import avro


from dataclasses import asdict, dataclass, field
from io import BytesIO


from confluent_kafka import Producer
from faker import Faker
from fastavro import parse_schema, writer


from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "my-first-python-topic"




def main():
    logger.debug('main')
    c = Consumer({"bootstrap.servers":BROKER_URL, "group.id": "first-python-consumer-group"})
    print(dir(c))
    print(c.list_topics())

def main2():
    logger.debug('main')
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    print(dir(client))
    print(client.describe_configs)
    cluster_metadata = client.list_topics(timeout=5.0)
    print(dir(cluster_metadata))
    print(cluster_metadata.orig_broker_name)
    topic_name ='testTopic'
    details = client.create_topics([NewTopic(topic_name, num_partitions=3, replication_factor=1)])
    # print(cluster_metadata)
    # print(details)
    

if __name__ == "__main__" :
    main2()