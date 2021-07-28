"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = "http://localhost:8081",
BOOTSTRAP_SERVERS = "PLAINTEXT://localhost:9092"
BROKER_URL = "PLAINTEXT://localhost:9092"
REST_PROXY_URL = "http://localhost:8082"
KAFKA_CONNECT_URL = "http://localhost:8083/connectors"



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
            "bootstrap.servers" :BOOTSTRAP_SERVERS,    
            "schema.registry.url": SCHEMA_REGISTRY_URL
            },
            default_key_schema=key_schema,
            default_value_schema=value_schema,
         )
        
    @property
    def client(self):
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
        cluster_metadata = self.client.list_topics(timeout=5.0)
        topics = cluster_metadata.topics
        if self.topic_name not in topics:
            topic_details = self.client.create_topics([NewTopic(self.topic_name, num_partitions=self.num_partitions, num_replicas=self.num_replicas)])
        else:
            logger.debug(f"Topic already exists: {self.topic_name}")
            return
        
        
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
