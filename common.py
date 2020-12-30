'''Common functions used by both consumer and producer modules'''
import logging
import sys
import os

import psycopg2

#from kafka import KafkaConsumer, KafkaProducer
import kafka

KAFKA_CERT_FILE = 'service.cert'
KAFKA_KEY_FILE = 'service.key'
AIVEN_CA_CERT_FILE = 'ca.pem' #this will be the same for Kafka and PostgreSQL

KAFKA_CONSUMER = 0
KAFKA_PRODUCER = 1

def configure_logging():
    '''For demo purposes, log everything to stdout.'''
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

#doing this at module scope makes the logger a singleton
LOGGER = configure_logging()

def get_logger():
    '''get the singleton logger we can use anywhere'''
    return LOGGER

def connect_to_postgresql(uri): #TODO catch exceptions?
    '''For simplicity just use single uri connection string parameter'''
    LOGGER.info("Connecting to PostgreSQL...")
    #TODO should use verify-full or verify-ca
    connection = psycopg2.connect(uri)
    dsn_params = connection.get_dsn_parameters()
    LOGGER.info("PostgreSQL connection complete, info: %s", str(dsn_params))
    return connection

def get_kafka_connection(object_type, kafka_url, cert_path, topic_name=""):
    '''create either a producer or consumer, depending on object_type parameter.
    object_type must be either KAFKA_CONSUMER or KAFKA_PRODUCER'''
    if object_type == KAFKA_CONSUMER:
        connection = kafka.KafkaConsumer(topic_name, bootstrap_servers=kafka_url,
            security_protocol='SSL',
            ssl_cafile=os.path.join(cert_path, AIVEN_CA_CERT_FILE),
            ssl_certfile=os.path.join(cert_path, KAFKA_CERT_FILE),
            ssl_keyfile=os.path.join(cert_path, KAFKA_KEY_FILE))
    elif object_type == KAFKA_PRODUCER:
        connection = kafka.KafkaProducer(bootstrap_servers=kafka_url,
            security_protocol='SSL',
            ssl_cafile=os.path.join(cert_path, AIVEN_CA_CERT_FILE),
            ssl_certfile=os.path.join(cert_path, KAFKA_CERT_FILE),
            ssl_keyfile=os.path.join(cert_path, KAFKA_KEY_FILE))
    else:
        raise ValueError("object_type parameter must be either KAFKA_CONSUMER or KAFKA_PRODUCER")
    return connection


def check_kafka_ssl_files(cert_path):
    '''Checks necessary SSL resources are available. Specific to Aiven Kafka implementation. '''
    files_found = set(os.listdir(cert_path))
    required_files = {KAFKA_CERT_FILE, KAFKA_KEY_FILE, AIVEN_CA_CERT_FILE}
    if files_found.issuperset(required_files):
        LOGGER.info("Aiven Kafka SSL config files found OK")
    else:
        missing = required_files.difference(files_found)
        message = "Required file(s) not found in %s: %s" % (cert_path, str(missing))
        LOGGER.error(message)
        raise FileNotFoundError(message)
    return True
