"""Demonstration of how to consume information from Aiven Kafka and write it
into Aiven PostgreSQL
"""

import argparse
import os

from kafka import KafkaConsumer

KAFKA_CERT_FILE = 'service.cert'
KAFKA_KEY_FILE = 'service.key'
KAFKA_CA_CERT_FILE = 'ca.pem'

def main(kafka_url, topic_name, cert_path=""):
    if not cert_path:
        cert_path = os.getcwd()
    check_kafka_ssl_files(cert_path)
     
    # set up the kafka consumer and establish connection to the Aiven instance
    consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_url, security_protocol='SSL', 
        ssl_cafile=os.path.join(cert_path, KAFKA_CA_CERT_FILE), 
        ssl_certfile=os.path.join(cert_path, KAFKA_CERT_FILE), 
        ssl_keyfile=os.path.join(cert_path, KAFKA_KEY_FILE))
        
    # consume messages
    for message in consumer:
        print(message)
        
def check_kafka_ssl_files(cert_path):
    files_found = set(os.listdir(cert_path))
    required_files = {KAFKA_CERT_FILE, KAFKA_KEY_FILE, KAFKA_CA_CERT_FILE}
    if not files_found.issuperset(required_files):
        raise FileNotFoundError("At least one required file not found in %s" % cert_path) #TODO fix
    
    
if __name__=='__main__':
    parser = argparse.ArgumentParser(description="Tool for monitoring websites using Aiven Kafka.")
    parser.add_argument('kafka_url',
                    help="URL of the Aiven Kafka service to consume - required")
    parser.add_argument('topic_name',
                    help="Kafka topic to consume - required")
    parser.add_argument('--cert-path', required=False,
                    help="Directory containing access cert and key and CA cert for Aiven Kafka service")
    args = vars(parser.parse_args())
    
    #print("args = %s" % args)
    main(**args)