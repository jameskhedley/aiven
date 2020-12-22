"""Demonstration of how to write a producer to publush data to Aiven Kafka into Aiven PostgreSQL.
Monitors a given web site using a given regex and records status codes and response times.
"""

import argparse
import os
import time
import requests
import re
import json

from kafka import KafkaProducer

from common import configure_logging, connect_to_postgresql, check_kafka_ssl_files, \
    AIVEN_CA_CERT_FILE, KAFKA_CERT_FILE, KAFKA_KEY_FILE, get_logger
    
REQUEST_INTERVAL = 2 # query website every 2 seconds    

def main(kafka_url, topic_name, monitor_url, monitor_regex, cert_path=""):
    if not cert_path:
        cert_path = os.getcwd()
    check_kafka_ssl_files(cert_path)
    
    producer = KafkaProducer(bootstrap_servers=kafka_url, security_protocol='SSL', 
                            ssl_cafile=os.path.join(cert_path, AIVEN_CA_CERT_FILE), 
                            ssl_certfile=os.path.join(cert_path, KAFKA_CERT_FILE), 
                            ssl_keyfile=os.path.join(cert_path, KAFKA_KEY_FILE))

    logger.info("Starting producer loop, ctrl+c or SIGINT to exit")
    while True:
        r = requests.get(monitor_url, stream=True)
        #don't read data yet but record statistics
        statistics = {"response_time": str(r.elapsed), "status_code": r.status_code, 
                        "url": monitor_url, "matched_string": ""}
    
        #build the regex pattern according to the encoding of the connection
        pattern = bytes(monitor_regex, encoding=r.encoding)
        regex = re.compile(pattern)

        # streaming the page may save memory
        for line in r.iter_lines():
            found = regex.search(line)
            if found:
                statistics['matched_string'] = str(found.group())
                break
        
        logger.info("Sending this payload to topic %s: %s" % (topic_name, str(statistics)))
        #whatever happened, report to kafka
        bytes_value = bytes(json.dumps(statistics), encoding='utf8')
        producer.send(topic_name, key=b"something-better", value=bytes_value)
        
        time.sleep(REQUEST_INTERVAL)

    
if __name__=='__main__':
    logger = get_logger()
    parser = argparse.ArgumentParser(description="Tool for monitoring websites using Aiven Kafka - producer.")
    parser.add_argument('kafka_url',
                    help="URL of the Aiven Kafka service to consume - required")
    parser.add_argument('topic_name',
                    help="Kafka topic to publish to - required")
    parser.add_argument('monitor_url',
                    help="URL of web page to monitor - required")
    parser.add_argument('monitor_regex',
                    help="Regex pattern to look for  - required")
    parser.add_argument('--cert-path', required=False,
                    help="Directory containing access cert and key and CA cert for Aiven Kafka service")
    args = vars(parser.parse_args())
    
    #print("args = %s" % args)
    main(**args)