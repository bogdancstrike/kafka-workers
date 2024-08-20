# main.py

from framework import fetch_configuration, create_kafka_consumer, create_kafka_producer, handle_message, close_resources
from config import CONSUMER_NAME
from utils.logger import logger


def main():
    consumer = None
    producer = None
    try:
        # Fetch configuration from the database
        config = fetch_configuration(CONSUMER_NAME)

        # Parse topics and Kafka bootstrap server from configuration
        KAFKA_INPUT_TOPICS = config.topics_input.split(',')
        KAFKA_OUTPUT_TOPICS = config.topics_output.split(',')
        KAFKA_BOOTSTRAP_SERVERS = config.kafka_bootstrap_server.split(',')

        # Create Kafka consumer and producer with retry mechanisms
        consumer = create_kafka_consumer(KAFKA_INPUT_TOPICS, KAFKA_BOOTSTRAP_SERVERS)
        producer = create_kafka_producer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_OUTPUT_TOPICS)

        # Handle incoming Kafka messages
        handle_message(consumer, producer, KAFKA_INPUT_TOPICS, KAFKA_OUTPUT_TOPICS)

    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
        close_resources(consumer, producer)


if __name__ == '__main__':
    main()
