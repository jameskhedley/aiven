"""Demonstration of how to consume information from Aiven Kafka and write it
into Aiven PostgreSQL
"""

import argparse
import os

from kafka import KafkaConsumer

from common import configure_logging, connect_to_postgresql, check_kafka_ssl_files, \
    AIVEN_CA_CERT_FILE, KAFKA_CERT_FILE, KAFKA_KEY_FILE, get_logger


def main(kafka_url, topic_name, postgesql_uri, cert_path=""):
    if not cert_path:
        cert_path = os.getcwd()
    check_kafka_ssl_files(cert_path)
    
    conn = connect_to_postgresql(postgesql_uri)
    
    logger.info("Starting Kafka consumer, ctrl+c or SIGINT to exit")
    # set up the kafka consumer and establish connection to the Aiven instance
    consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_url, security_protocol='SSL', 
        ssl_cafile=os.path.join(cert_path, AIVEN_CA_CERT_FILE), 
        ssl_certfile=os.path.join(cert_path, KAFKA_CERT_FILE), 
        ssl_keyfile=os.path.join(cert_path, KAFKA_KEY_FILE))
        
    # consume messages
    for message in consumer:
        print(message) #TODO write into postgresql
    
    
if __name__=='__main__':
    logger = get_logger()
    parser = argparse.ArgumentParser(description="Tool for monitoring websites using Aiven Kafka - consumer.")
    parser.add_argument('kafka_url',
                    help="URL of the Aiven Kafka service to consume - required")
    parser.add_argument('topic_name',
                    help="Kafka topic to consume - required")
    parser.add_argument('postgesql_uri',
                    help="URI of the PostgreSQL instance - required")
    parser.add_argument('--cert-path', required=False,
                    help="Directory containing access cert and key and CA cert for Aiven Kafka service")
    args = vars(parser.parse_args())
    
    #print("args = %s" % args)
    main(**args)