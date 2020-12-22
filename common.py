'''Common functions used by both consumer and producer modules'''
import logging
import sys, os

import psycopg2

KAFKA_CERT_FILE = 'service.cert'
KAFKA_KEY_FILE = 'service.key'
AIVEN_CA_CERT_FILE = 'ca.pem' #this will be the same for Kafka and PostgreSQL

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

logger = configure_logging()

def get_logger():
    return logger
    
def connect_to_postgresql(uri):
    '''For simplicity just use single uri connection string parameter'''
    logger.info("Connecting to PostgreSQL...")
    connection = psycopg2.connect(uri)
    logger.info("PostgreSQL connection complete, info: %s" % (conn.get_dsn_parameters()))
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