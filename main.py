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

        # Create the structured JSON object
        structured_message = {
            "_index": ELASTICSEARCH_INDEX,
            "_id": es_id,
            "timestamp_ingestion": datetime.utcnow().isoformat(),
            "type": "article",
            "content": message_dict
        }

        # Log the structured message
        print(f"Structured message ready to be sent to Kafka: {structured_message}")

        # Send the structured message to the Kafka output topic
        producer.send(KAFKA_OUTPUT_TOPIC, structured_message)
        print(f"Structured message sent to Kafka topic {KAFKA_OUTPUT_TOPIC}")

    except json.JSONDecodeError:
        print(f"Failed to decode message: {message}")
    except Exception as e:
        print(f"Error saving message to Elasticsearch or sending to Kafka: {e}")




if __name__ == '__main__':
    for message in consumer:
        message_value = message.value.decode('utf-8')
        save_to_elasticsearch_and_forward(message_value)
