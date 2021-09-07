"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient,AvroConsumer
logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL: str = "http://localhost:8081"
BOOTSTRAP_SERVERS: str = "PLAINTEXT://localhost:9092"
BROKER_URL = "PLAINTEXT://localhost:9092"
REST_PROXY_URL: str = "http://localhost:8082"
KAFKA_CONNECT_URL: str = "http://localhost:8083/connectors"
KAFKA_BROKER: str = "PLAINTEXT://localhost:9092"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self._client = None
        self.broker_properties = {
            "bootstrap.servers": KAFKA_BROKER,
            "schema.registry.url": SCHEMA_REGISTRY_URL,
            "on_delivery": delivery_report,
        }

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "schema.registry.url": SCHEMA_REGISTRY_URL ,
            "bootstrap.servers": BOOTSTRAP_SERVERS
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            {
            "bootstrap.servers": BROKER_URL,  
            "schema.registry.url": SCHEMA_REGISTRY_URL,
        },
            default_key_schema=key_schema,
            default_value_schema=value_schema,
         )
        
    @property
    def client(self):
        #logger.info(f"creating Kafka Admin client - {BROKER_URL}")
        self._client = AdminClient({"bootstrap.servers": BROKER_URL})
        return self._client        

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        #logger.info("creating topic %s", self.topic_name)
        #logger.info(dir(self.client))
        # cluster_metadata = self.client.list_topics(timeout=5)
        # logger.debug("cluster_metadata: %s", cluster_metadata)
        # topics = cluster_metadata.topics
        # if self.topic_name not in topics:
        #     logger.info(f"creating topic name - {self.topic_name}, num_part- {self.num_partitions}, num_replicas - {self.num_replicas}" )
        #     topic_details = self.client.create_topics([NewTopic(self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)])
        #     logger.info("Topic %s created", self.topic_name)
        # else:
        #     logger.debug(f"Topic already exists: {self.topic_name}")
        #     return
        topic = NewTopic(self.topic_name, num_partitions=1, replication_factor=1)
        self.client.create_topics([topic])
        

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        self.producer.flush()
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

def delivery_report(err, msg):
    """Callback on message delivery result"""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()}[{msg.partition()}]")
