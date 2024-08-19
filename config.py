# region Date care vor veni din baza de date

# KAFKA
KAFKA_INPUT_TOPIC = 'test_modul_colectare'
KAFKA_OUTPUT_TOPIC = 'modul_colectare_out'
KAFKA_BOOTSTRAP_SERVERS = ['172.17.12.80:9092']
# endregion


# region Date care tin de business-ul consumatorului - nu vin din baza de date

# ELASTIC SEARCH
ELASTICSEARCH_CERT_PATH = 'es-cert.crt'
ELASTICSEARCH_HOST = 'https://10.30.11.14:9200'
ELASTICSEARCH_USERNAME = 'elastic'
ELASTICSEARCH_PASSWORD = 'YTrEkQ9G8OzH5rQi1wgK'
ELASTICSEARCH_INDEX = 'modul_colectare'
# endregion
