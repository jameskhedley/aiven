"""Demonstration of how to write a producer to publush data to Aiven Kafka into Aiven PostgreSQL.
Monitors a given web site using a given regex and records status codes and response times.
"""

import argparse
import os
import time
import re
import json
import requests

import common
logger = common.get_logger()

REQUEST_INTERVAL = 10 # query website every n seconds

def main(kafka_url, topic_name, monitor_url, monitor_regex, cert_path=""):
    '''Connect to kafka instance, query the givern website and report observations
    '''
    if not cert_path:
        cert_path = os.getcwd()
    common.check_kafka_ssl_files(cert_path)

    producer = common.get_kafka_connection(common.KAFKA_PRODUCER, kafka_url, cert_path)

    logger.info("Starting producer loop, ctrl+c or SIGINT to exit")
    while True:
        main_inner(producer, topic_name, monitor_url, monitor_regex)

def main_inner(kafka_producer, topic_name, monitor_url, monitor_regex):
    '''Split this out of main so it's easier to test, basically'''
    resp = requests.get(monitor_url, stream=True)
    #don't read data yet but record statistics
    statistics = {"response_time": str(resp.elapsed), "status_code": resp.status_code,
                    "url": monitor_url, "matched_string": ""}

    #build the regex pattern according to the encoding of the connection
    pattern = bytes(monitor_regex, encoding=resp.encoding)
    regex = re.compile(pattern)

    # streaming the page will save some memory
    for line in resp.iter_lines():
        found = regex.search(line)
        if found:
            #import pdb; pdb.set_trace()
            statistics['matched_string'] = found.group().decode(resp.encoding)
            break

    logger.info("Sending this payload to topic %s: %s", topic_name, str(statistics))
    #whatever happened, report to kafka
    bytes_value = bytes(json.dumps(statistics), encoding='utf8')

    #TODO think about a better key to use?
    print(bytes_value)
    kafka_producer.send(topic_name, key=b"something-better", value=bytes_value)

    time.sleep(REQUEST_INTERVAL)


if __name__=='__main__':
    DESCRIPTION = "Tool for monitoring websites using Aiven Kafka - producer."
    parser = argparse.ArgumentParser(description=DESCRIPTION)
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
