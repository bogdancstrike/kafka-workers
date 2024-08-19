import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from elasticsearch import Elasticsearch
import json
from config import KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS, ELASTICSEARCH_HOST, ELASTICSEARCH_INDEX, \
    ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD, ELASTICSEARCH_CERT_PATH, KAFKA_OUTPUT_TOPIC
import ssl
from logger import logger


def create_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='consumer_for_es'
            )
            logger.info("Kafka consumer connected.")
            return consumer
        except NoBrokersAvailable:
            logger.error("No Kafka brokers available. Retrying in 5 seconds...")
            time.sleep(5)


def create_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("Kafka producer connected.")
            return producer
        except NoBrokersAvailable:
            logger.error("No Kafka brokers available. Retrying in 5 seconds...")
            time.sleep(5)


# Setup SSL context for Elasticsearch
ssl_context = ssl.create_default_context(cafile=ELASTICSEARCH_CERT_PATH)

# Initialize Elasticsearch client once
es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    http_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD),
    ssl_context=ssl_context,
    verify_certs=True
)


def save_to_elasticsearch_and_forward(message):
    try:
        # Parse the incoming Kafka message
        message_dict = json.loads(message)

        # Prepare the document structure for Elasticsearch
        document = {
            "id": None,  # This will be filled by Elasticsearch
            "timestamp": datetime.utcnow().isoformat(),
            "type": "article",
            "content": message_dict
        }

        # Save the document to Elasticsearch and capture the response
        es_response = es.index(index=ELASTICSEARCH_INDEX, body=document)

        # Get the Elasticsearch generated ID
        es_id = es_response['_id']

        # Update the document with the Elasticsearch ID
        document['id'] = es_id

        # Log the structured message
        logger.debug(f"Structured message ready to be sent to Kafka: {document}")

        # Send the updated document to the Kafka output topic
        producer.send(KAFKA_OUTPUT_TOPIC, document)
        logger.debug(f"Structured message sent to Kafka topic {KAFKA_OUTPUT_TOPIC}")

    except json.JSONDecodeError:
        logger.error(f"Failed to decode message: {message}")
    except Exception as e:
        logger.error(f"Error saving message to Elasticsearch or sending to Kafka: {e}")


if __name__ == '__main__':
    try:
        # Create Kafka consumer and producer with retry mechanisms
        consumer = create_kafka_consumer()
        producer = create_kafka_producer()

        # Continuously listen to Kafka messages
        for message in consumer:
            # Decode the message value from bytes to string
            message_value = message.value.decode('utf-8')
            # Save to Elasticsearch and forward to the output Kafka topic
            save_to_elasticsearch_and_forward(message_value)
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    finally:
        # Ensure resources are closed properly
        if consumer:
            consumer.close()
        if producer:
            producer.close()
        logger.info("Kafka consumer and producer closed.")
