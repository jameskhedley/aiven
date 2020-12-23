'''Common functions used by both consumer and producer modules'''
import logging
import sys, os

import psycopg2

from kafka import KafkaConsumer, KafkaProducer

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
logger = configure_logging()

def get_logger():
    '''get the singleton logger we can use anywhere'''
    return logger
    
def connect_to_postgresql(uri): #TODO catch exceptions?
    '''For simplicity just use single uri connection string parameter'''
    logger.info("Connecting to PostgreSQL...")
    connection = psycopg2.connect(uri)
    logger.info("PostgreSQL connection complete, info: %s" % (connection.get_dsn_parameters()))
    return connection
    
def get_kafka_connection(type, kafka_url, cert_path, topic_name=""):
    '''create either a producer or consumer, depending on type parameter.
    type must be either KAFKA_CONSUMER or KAFKA_PRODUCER'''
    if type == KAFKA_CONSUMER:
        connection = KafkaConsumer(topic_name, bootstrap_servers=kafka_url, security_protocol='SSL', 
            ssl_cafile=os.path.join(cert_path, AIVEN_CA_CERT_FILE), 
            ssl_certfile=os.path.join(cert_path, KAFKA_CERT_FILE), 
            ssl_keyfile=os.path.join(cert_path, KAFKA_KEY_FILE))
    elif type == KAFKA_PRODUCER:
        connection = KafkaProducer(bootstrap_servers=kafka_url, security_protocol='SSL', 
            ssl_cafile=os.path.join(cert_path, AIVEN_CA_CERT_FILE), 
            ssl_certfile=os.path.join(cert_path, KAFKA_CERT_FILE), 
            ssl_keyfile=os.path.join(cert_path, KAFKA_KEY_FILE))
    else:
        raise ValueError("type parameter must be either KAFKA_CONSUMER or KAFKA_PRODUCER")
    return connection
        
        
def check_kafka_ssl_files(cert_path):
    '''Checks necessary SSL resources are available. Specific to Aiven Kafka implementation. '''
    files_found = set(os.listdir(cert_path))
    required_files = {KAFKA_CERT_FILE, KAFKA_KEY_FILE, AIVEN_CA_CERT_FILE}
    if files_found.issuperset(required_files):
        logger.info("Aiven Kafka SSL config files found OK")
    else:
        missing = required_files.difference(files_found)
        message = "Required file(s) not found in %s: %s" % (cert_path, str(missing))
        logger.error(message)
        raise FileNotFoundError(message)