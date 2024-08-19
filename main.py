import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from elasticsearch import Elasticsearch, ConnectionError, NotFoundError
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
            logger.info(f"Kafka consumer connected to topic {KAFKA_TOPIC}")
            return consumer
        except NoBrokersAvailable as e:
            logger.error(f"No Kafka brokers available: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except KafkaError as e:
            logger.error(f"Kafka error occurred: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error while connecting to Kafka consumer: {e}. Retrying in 5 seconds...")
            time.sleep(5)


def create_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Kafka producer connected to topic {KAFKA_OUTPUT_TOPIC}")
            return producer
        except NoBrokersAvailable as e:
            logger.error(f"No Kafka brokers available: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except KafkaError as e:
            logger.error(f"Kafka error occurred: {e}. Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error while connecting to Kafka producer: {e}. Retrying in 5 seconds...")
            time.sleep(5)


# Setup SSL context for Elasticsearch
ssl_context = ssl.create_default_context(cafile=ELASTICSEARCH_CERT_PATH)

# Initialize Elasticsearch client once
try:
    es = Elasticsearch(
        [ELASTICSEARCH_HOST],
        basic_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD),
        ssl_context=ssl_context,
        verify_certs=True
    )
    # Perform a simple operation to check connection
    es.ping()
    logger.info(f"Connected to Elasticsearch at {ELASTICSEARCH_HOST}")
except ConnectionError as e:
    logger.error(f"Failed to connect to Elasticsearch: {e}")
    raise
except Exception as e:
    logger.error(f"Unexpected error while connecting to Elasticsearch: {e}")
    raise


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

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode message: {message}. Error: {e}")
    except KafkaError as e:
        logger.error(f"Kafka error while sending message: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in save_to_elasticsearch_and_forward: {e}")


if __name__ == '__main__':
    consumer = None
    producer = None
    try:
        logger.info("Starting Kafka consumer...")
        # Create Kafka consumer and producer with retry mechanisms
        consumer = create_kafka_consumer()
        producer = create_kafka_producer()

        # Continuously listen to Kafka messages
        for message in consumer:
            try:
                # Decode the message value from bytes to string
                logger.debug("new message arrived")
                message_value = message.value.decode('utf-8')
                # Save to Elasticsearch and forward to the output Kafka topic
                save_to_elasticsearch_and_forward(message_value)
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
    finally:
        # Ensure resources are closed properly
        if consumer:
            try:
                consumer.close()
                logger.info("Kafka consumer closed.")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
        if producer:
            try:
                producer.close()
                logger.info("Kafka producer closed.")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
