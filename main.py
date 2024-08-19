import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from elasticsearch import Elasticsearch, ConnectionError
import json
import ssl
from config import KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS, ELASTICSEARCH_HOST, ELASTICSEARCH_INDEX, \
    ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD, ELASTICSEARCH_CERT_PATH, KAFKA_OUTPUT_TOPIC
from logger import logger


def create_kafka_consumer():
    """Create and return a Kafka consumer with retry logic."""
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
    """Create and return a Kafka producer with retry logic."""
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


def process(message):
    """Process the Kafka message by saving it to Elasticsearch and updating it with ES's ID."""
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
        logger.debug("Message saved to Elasticsearch")

        # Get the Elasticsearch generated ID
        es_id = es_response['_id']

        # Update the document with the Elasticsearch ID
        document['id'] = es_id

        # Log the structured message
        logger.debug(f"Structured message updated with Elasticsearch ID: {document}")

        # Return the updated document as the processed message
        return document

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode message: {message}. Error: {e}")
        raise
    except KafkaError as e:
        logger.error(f"Kafka error while processing message: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in process function: {e}")
        raise


def handle_message(consumer, producer):
    """Continuously consume messages, process them, and forward to another Kafka topic."""
    try:
        logger.info("Starting message handling loop...")
        for message in consumer:
            try:
                # Decode the message value from bytes to string
                logger.debug("New message arrived")
                message_value = message.value.decode('utf-8')

                # Process the message using the custom process function
                processed_message = process(message_value)

                # Send the processed message to the output Kafka topic
                producer.send(KAFKA_OUTPUT_TOPIC, processed_message)
                logger.debug(f"Processed message sent to Kafka topic {KAFKA_OUTPUT_TOPIC}")

            except Exception as e:
                logger.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
    finally:
        close_resources(consumer, producer)


def close_resources(consumer, producer):
    """Close Kafka consumer and producer resources gracefully."""
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


if __name__ == '__main__':
    consumer = None
    producer = None
    try:
        # Create Kafka consumer and producer with retry mechanisms
        consumer = create_kafka_consumer()
        producer = create_kafka_producer()

        # Handle incoming Kafka messages
        handle_message(consumer, producer)

    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
        close_resources(consumer, producer)
