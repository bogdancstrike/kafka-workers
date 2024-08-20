# region Database configuration
# MySQL
DB_HOST = 'localhost'
DB_PORT = 3306
DB_NAME = 'dev'
DB_USER = 'dev'
DB_PASSWORD = 'dev'

# ELASTIC SEARCH
ELASTICSEARCH_CERT_PATH = 'es-cert.crt'
ELASTICSEARCH_HOST = 'https://10.30.11.14:9200'
ELASTICSEARCH_USERNAME = 'elastic'
ELASTICSEARCH_PASSWORD = 'YTrEkQ9G8OzH5rQi1wgK'
ELASTICSEARCH_INDEX = 'modul_colectare'
# endregion

# region consumer
CONSUMER_NAME = 'consumer_skeleton'
MESSAGE_TIMEOUT = 30  # Timeout in seconds for message aggregation
# endregion
