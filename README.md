# Kafka Consumer-Producer Skeleton

This repository provides a skeleton for creating Kafka consumer-producer applications. The skeleton is designed to read messages from a Kafka topic, process them, and forward the processed messages to another Kafka topic. The `process()` function is the key area where custom processing logic can be implemented.

## Getting Started

### Prerequisites

- Python 3.x
- Kafka Cluster
- Elasticsearch (optional, depending on your use case)
- Required Python packages: `kafka-python`, `elasticsearch`, `ssl`, `json`, `logging`, and others as listed in your `requirements.txt`.

### Installation

1. Clone the repository:
    ```bash
    git clone https://gitlab-prod.devnet.dcti.ro/serviciul-osint/qsint/consumers/consumer-template
    cd consumer-template
    ```

2. Install the required packages:
    ```bash
    pip install -r requirements.txt
    ```

3. Configure your environment variables in the `config.py` file:
    ```python
    KAFKA_INPUT_TOPIC = 'kafka_input_topic'
    KAFKA_OUTPUT_TOPIC = 'kafka_output_topic'
    KAFKA_BOOTSTRAP_SERVERS = ['kafka_broker:9092']
   
    ELASTICSEARCH_HOST = 'elasticsearch_host'
    ELASTICSEARCH_INDEX = 'index'
    ELASTICSEARCH_USERNAME = 'username'
    ELASTICSEARCH_PASSWORD = 'password'
    ELASTICSEARCH_CERT_PATH = '/path/to/elastic/ca.crt'
    ```

## Usage

To create a new consumer, you only need to modify the `process()` method. The rest of the code handles the Kafka consumer-producer logic and error handling.

### Running the Consumer

Once you've implemented your custom `process()` function, run the consumer:

```bash
python main.py
```

### Implementing Custom Processing Logic

1. Saving to Elasticsearch (Default Implementation)
   By default, the process() function is set up to save incoming messages to Elasticsearch and update the message with the Elasticsearch-generated ID.

```code
def process(message):
    """Process the Kafka message by saving it to Elasticsearch and updating it with ES's ID."""
    try:
        message_dict = json.loads(message)

        document = {
            "id": None,
            "timestamp": datetime.utcnow().isoformat(),
            "type": "article",
            "content": message_dict
        }

        es_response = es.index(index=ELASTICSEARCH_INDEX, body=document)
        es_id = es_response['_id']
        document['id'] = es_id

        return document

    except Exception as e:
        logger.error(f"Unexpected error in process function: {e}")
        raise

```

2. Calling an External API
   If your use case involves processing the message by calling an external API, you can modify the process() function as follows:

```code
import requests

def process(message):
    """Process the Kafka message by sending it to an external API and returning the API response."""
    try:
        message_dict = json.loads(message)
        response = requests.post('https://api.example.com/endpoint', json=message_dict)
        
        if response.status_code == 200:
            result = response.json()
            return result
        else:
            logger.error(f"API call failed with status code {response.status_code}: {response.text}")
            raise Exception("API call failed")
    
    except Exception as e:
        logger.error(f"Unexpected error in process function: {e}")
        raise
```

3. Custom Data Transformation
   If you need to perform custom data transformation or enrichment, you can modify the process() function like this:

```code
def process(message):
    """Process the Kafka message by performing custom data transformation."""
    try:
        message_dict = json.loads(message)

        # Example: Add a new field with a custom transformation
        message_dict['new_field'] = "Transformed: " + message_dict.get('existing_field', '')

        return message_dict

    except Exception as e:
        logger.error(f"Unexpected error in process function: {e}")
        raise
```

Handling Errors

The skeleton includes robust error handling for various scenarios, including connection issues with Kafka and Elasticsearch, JSON decoding errors, and unexpected exceptions. Ensure that your process() function either handles or raises exceptions as needed.
