"""Demonstration of how to consume information from Aiven Kafka and write it
into Aiven PostgreSQL
"""

import argparse
import os
import json
import datetime

from common import connect_to_postgresql, check_kafka_ssl_files, \
    get_logger, get_kafka_connection, KAFKA_CONSUMER


def main(kafka_url, topic_name, postgesql_uri, cert_path=""):
    if not cert_path:
        cert_path = os.getcwd()
    check_kafka_ssl_files(cert_path)
    
    conn = connect_to_postgresql(postgesql_uri)
    
    setup_db(conn)
    
    logger.info("Starting Kafka consumer, ctrl+c or SIGINT to exit")
    # set up the kafka consumer and establish connection to the Aiven instance
    consumer = get_kafka_connection(KAFKA_CONSUMER, kafka_url, cert_path, topic_name)
    
    cursor = conn.cursor()
    # consume messages and write to db
    for message in consumer:
        insert_query = '''INSERT INTO statistics (url, response_time, timestamp, status_code, matched_string)
                VALUES ('%(url)s', '%(response_time)s', '%(timestamp)s', %(status_code)s, '%(matched_string)s' )'''
        
        #import pdb; pdb.set_trace()
        all_values = json.loads(message.value)
        
        #convert the kafka timestamp to postgresql format
        dt0 = datetime.datetime.fromtimestamp(message.timestamp/1e3) #microseconds to seconds
        all_values.update({'timestamp': dt0.strftime('%Y-%m-%d %H:%M:%S')})
        #TODO should add timezone
        
        final_query = insert_query % all_values
        cursor.execute(final_query)
        conn.commit()
        logger.info("Message value %s written to db" % (all_values))
        #TODO how do we know it worked? 
        #TODO how do we clean up conn and cursor?
        
        
def setup_db(connection):
    '''Takes a psycopg2 connection object and makes sure the db has the necessary table.
    If not, attempt to create the table.'''
    #TODO could validate the table if already existing.
    cursor = connection.cursor()
    exists_query = '''select exists(select * from information_schema.tables 
                        where table_name='statistics')'''
    cursor.execute(exists_query)
    exists = cursor.fetchone()[0]
    if exists:
        logger.info("Statistics table already exists.")
    else:
        logger.info("Statistics table does not exist yet, creating...")
        #TODO factor these out maybe?
        create_query = '''CREATE TABLE statistics 
        (ID BIGSERIAL PRIMARY KEY NOT NULL, URL TEXT NOT NULL, RESPONSE_TIME TIME, 
        TIMESTAMP TIMESTAMP, STATUS_CODE INTEGER NOT NULL, MATCHED_STRING TEXT);'''
        cursor.execute(create_query)
        #double check it worked
        cursor.execute(exists_query)
        exists = cursor.fetchone()[0]
        cursor.close()
        if exists:
            logger.info("Statistics table created OK")
        else:
            raise RuntimeError("Couldn't create statistics table in db") #TODO could be better
    return True
            
    
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