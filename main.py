from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
from elasticsearch import Elasticsearch
import json
from utils.config import KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS, ELASTICSEARCH_HOST, ELASTICSEARCH_INDEX, \
    ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD, ELASTICSEARCH_CERT_PATH, KAFKA_OUTPUT_TOPIC
import ssl

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset = 'earliest',
    enable_auto_commit = True,
    group_id = 'consumer_for_es'
)

producer = KafkaProducer(
    bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

ssl_context = ssl.create_default_context(cafile=ELASTICSEARCH_CERT_PATH)

es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    http_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD),
    ssl_context=ssl_context,
    verify_certs=True
)

def save_to_elasticsearch_and_forward(message):
    try:
        # Parse the message
        message_dict = json.loads(message)

        # Save the message to Elasticsearch and capture the response
        es_response = es.index(index=ELASTICSEARCH_INDEX, body=message_dict)

        # Extract the Elasticsearch document ID
        es_id = es_response['_id']

        # Add the ID to the original message
        message_dict['_id'] = es_id

        # Log the response with the added ID
        print(f"Message saved to Elasticsearch with ID {es_id}: {es_response}")

        # Send the updated message with the ID to the Kafka output topic
        producer.send(KAFKA_OUTPUT_TOPIC, message_dict)
        print(f"Updated message with Elasticsearch ID sent to Kafka topic {KAFKA_OUTPUT_TOPIC}")

    except json.JSONDecodeError:
        print(f"Failed to decode message: {message}")
    except Exception as e:
        print(f"Error saving message to Elasticsearch or sending to Kafka: {e}")




if __name__ == '__main__':
    for message in consumer:
        message_value = message.value.decode('utf-8')
        save_to_elasticsearch_and_forward(message_value)
