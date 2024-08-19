# Kafka to Elasticsearch Processor (Consumer 1)
This project is a Python-based service that consumes messages from a Kafka topic, processes them, and then stores the processed messages in Elasticsearch. After saving the message in Elasticsearch, it forwards the processed message to another Kafka topic.

## Features
- Continuous Kafka Message Consumption: The service listens to a specified Kafka topic and processes incoming messages continuously.
- Elasticsearch Integration: Messages are stored in Elasticsearch with additional metadata.
- Kafka Message Forwarding: After processing, messages are forwarded to another Kafka topic.
- Resilience: The service includes retry mechanisms to handle temporary unavailability of Kafka brokers.
- Logging: Uses a logging system to track the service's behavior and any errors encountered.

## Handling error and retries

- Kafka Connection: The service includes a retry mechanism that will continuously attempt to connect to Kafka brokers if they are unavailable at startup.
- Elasticsearch Storage: Any issues with storing messages in Elasticsearch or forwarding them to Kafka are logged, ensuring you have visibility into what went wrong.

## Installation

```code
pip install -r requirements.txt
```

## Configuration

```code
# KAFKA
KAFKA_TOPIC = 'your_kafka_topic'  # The Kafka topic to consume messages from
KAFKA_OUTPUT_TOPIC = 'your_kafka_output_topic'  # The Kafka topic to forward processed messages to
KAFKA_BOOTSTRAP_SERVERS = ['172.17.12.80:9092']  # List of Kafka brokers to connect to

# ELASTICSEARCH
ELASTICSEARCH_CERT_PATH = 'es-cert.crt'  # Path to the Elasticsearch SSL certificate (if using HTTPS)
ELASTICSEARCH_HOST = '_elasticsearch_host'  # Hostname or IP address of the Elasticsearch instance
ELASTICSEARCH_USERNAME = 'elastic'  # Username for Elasticsearch authentication
ELASTICSEARCH_PASSWORD = 'password'  # Password for Elasticsearch authentication
ELASTICSEARCH_INDEX = 'index'  # The Elasticsearch index where messages will be stored

```

## Running

```code
python main.py
```

## Building the Docker image

To build the Docker image for this service:

1. Make sure your Dockerfile is correctly set up in the root directory of the project.
2. Build the Docker image:

```code
docker build -t your-docker-image:latest .
```

## Deploying on Kubernetes

1. Apply the deployment and service configuration to your Kubernetes cluster:

```code
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
```

2. Verify the deployment:

```code
kubectl get pods -l app=kafka-es-processor
```

3. Verify the service:

```code
kubectl get svc kafka-es-processor-service
```
