from utils.logger import logger


# worker.py

def process(message, consumer_name):
    """
    Process the Kafka message by adding or modifying fields.

    This function is intended to be customized by developers to implement specific processing logic.
    The message passed to this function will contain all fields combined from multiple input topics (if applicable).

    Args:
        message (dict): The aggregated message with fields combined from one or more topics.
                        The structure of the message will typically look like this:

                        {
                            "id": <unique_message_id>,
                            "text": <common_text>,
                            "field1": <value_from_topic_1>,
                            "field2": <value_from_topic_2>,
                            ...
                        }

                        The exact fields will depend on the messages received from the input topics.
                        The developer can modify existing fields or add new fields based on the
                        processing logic required.

        consumer_name (str): The name of the consumer, as defined in `config.py`. This is also the
                             ID of the consumer in the database and is used to retrieve the consumer's
                             settings and for logging purposes.

    Returns:
        dict: The final processed message. This should be the modified message that will be sent
              to the output Kafka topics. Any new fields added or existing fields modified should
              be included in the returned dictionary.
    """

    # Default implementation: Return the message without any modifications.
    # This is where custom processing logic should be implemented.
    logger.debug(f"{consumer_name}: processing message")
    return message
