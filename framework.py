import time
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
import json
from models import ConsumerConfig, create_session, init_db
from config import MESSAGE_TIMEOUT, CONSUMER_NAME
from utils.logger import logger
from worker import process  # Import the process function from worker.py

# Initialize the database
init_db()


def fetch_configuration(consumer_name):
    """
    Fetch configuration for the given consumer from the database using SQLAlchemy.

    Args:
        consumer_name (str): The name of the consumer to fetch the configuration for.

    Returns:
        ConsumerConfig: A SQLAlchemy object containing the configuration for the consumer.

    Raises:
        Exception: If no configuration is found for the specified consumer name.
    """
    session = create_session()
    config = session.query(ConsumerConfig).filter_by(consumer_name=consumer_name).first()
    session.close()
    if config:
        return config
    else:
        raise Exception(f"No configuration found for consumer: {consumer_name}")


def create_kafka_consumer(topics_input, bootstrap_servers):
    """
    Create or Connect and return a Kafka consumer with retry logic.

    Args:
        topics_input (list): List of Kafka topics to subscribe to.
        bootstrap_servers (list): List of Kafka bootstrap servers.

    Returns:
        KafkaConsumer: A Kafka consumer connected to the specified topics.

    Raises:
        NoBrokersAvailable, KafkaError, Exception: If unable to connect to the Kafka brokers.
    """
    while True:
        try:
            consumer = KafkaConsumer(
                *topics_input,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='consumer_for_es'
            )
            logger.info(f"Kafka consumer connected to topics {topics_input}")
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


def create_kafka_producer(bootstrap_servers, output_topics):
    """
    Create or Connect and return a Kafka producer with retry logic.

    Args:
        bootstrap_servers (list): List of Kafka bootstrap servers.
        output_topics (list): List of output Kafka topics.

    Returns:
        KafkaProducer: A Kafka producer connected to the specified servers.

    Raises:
        NoBrokersAvailable, KafkaError, Exception: If unable to connect to the Kafka brokers.
    """
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info(f"Kafka producer connected to topics {output_topics}")
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


def handle_message(consumer, producer, topics_input, output_topics, consumer_name=CONSUMER_NAME):
    """
    Continuously consume messages, process them, and forward to another Kafka topic.

    This function runs a loop that continuously listens for messages from the specified input topics.
    When messages with the same ID from all input topics have been received, they are aggregated into
    a single message, processed, and then sent to the specified output topics.

    Args:
        consumer (KafkaConsumer): The Kafka consumer object that listens for incoming messages.
        producer (KafkaProducer): The Kafka producer object that sends the processed messages.
        topics_input (list): List of input Kafka topics to consume from. Messages from these topics
                             are aggregated based on their IDs.
        output_topics (list): List of output Kafka topics to send processed messages to.
        consumer_name (str): The name of the consumer, used for organizing processed fields and
                             logging. Defaults to the value of CONSUMER_NAME from the configuration.

    Raises:
        Exception: Handles and logs any unexpected errors that occur during message processing.
    """
    try:
        logger.info("Starting message handling loop...")
        message_buffer = defaultdict(dict)
        topic_tracker = defaultdict(set)  # Track which topics have sent a message for each ID
        timestamp_buffer = {}

        while True:
            current_time = time.time()

            # Poll for new messages from Kafka with a short timeout
            message_batch = consumer.poll(timeout_ms=1000)

            # Process each message in the batch
            for tp, messages in message_batch.items():
                for message in messages:
                    try:
                        message_value = json.loads(message.value.decode('utf-8'))
                        message_id = message_value.get('id')
                        topic = message.topic

                        # Update message and timestamp buffers
                        if message_id not in message_buffer:
                            message_buffer[message_id] = {}
                            timestamp_buffer[message_id] = current_time

                        # Merge fields from the topic's message into the aggregate message
                        for key, value in message_value.items():
                            if key == "content" and key in message_buffer[message_id]:
                                # Merge content dictionaries
                                message_buffer[message_id]["content"].update(value)
                            else:
                                # Update or add other fields
                                message_buffer[message_id][key] = value

                        topic_tracker[message_id].add(topic)

                        # Log the arrival of a message for this ID
                        num_received = len(topic_tracker[message_id])
                        total_expected = len(topics_input)
                        logger.info(
                            f"Message {num_received}/{total_expected} arrived for ID = {message_id} with content: {message_value}")

                        # Check if all topics have arrived for the given ID (for aggregation)
                        if num_received == total_expected:
                            logger.info(f"All {total_expected} messages received for ID = {message_id}. Processing...")

                            # Process the aggregated message
                            aggregated_message = message_buffer.pop(message_id)
                            processed_message = process(aggregated_message, consumer_name)
                            logger.debug(f"Processed message: {processed_message}")

                            # Send the processed message to all output topics
                            for output_topic in output_topics:
                                producer.send(output_topic, processed_message)
                                logger.debug(f"Processed message sent to Kafka topic {output_topic}")

                            # Clean up timestamp and topic tracker buffers
                            timestamp_buffer.pop(message_id, None)
                            topic_tracker.pop(message_id, None)

                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

            # Check for timeouts on each iteration, even if no new messages were processed
            for mid in list(timestamp_buffer.keys()):
                if current_time - timestamp_buffer[mid] > MESSAGE_TIMEOUT:
                    logger.warn(
                        f"Timeout reached, ignoring and removing incomplete message with ID = {mid} from memory")
                    # Remove the incomplete message from memory
                    message_buffer.pop(mid, None)
                    timestamp_buffer.pop(mid, None)
                    topic_tracker.pop(mid, None)

    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
    finally:
        close_resources(consumer, producer)


def close_resources(consumer, producer):
    """
    Close Kafka consumer and producer resources gracefully.

    Args:
        consumer (KafkaConsumer): The Kafka consumer object to close.
        producer (KafkaProducer): The Kafka producer object to close.
    """
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
