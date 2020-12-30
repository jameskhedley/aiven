'''tests for aiven kafka producer and consumer'''
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import unittest
from unittest import mock
import json
import logging
import kafka
import psycopg2

import common
import producer
import consumer

#def test_producer_main()

class MyTest(unittest.TestCase):
    def test_get_logger(self):
        got_logger = common.get_logger()
        expected_logger = logging.getLogger()
        self.assertEqual(type(got_logger), type(expected_logger))
        self.assertEqual(got_logger.level, logging.INFO)
    
    @mock.patch("psycopg2.connect")
    def test_db_connect(self, mock_connect):
        connection = common.connect_to_postgresql("blah")
        self.assertIsNotNone(connection)
        mock_connect.assert_called_once_with("blah")
        
    @mock.patch("kafka.KafkaConsumer", autospec=True)
    @mock.patch("kafka.KafkaProducer", autospec=True)
    def test_kafka_connect_consumer(self, mock_producer, mock_consumer):
        PATH = "path"
        consumer = common.get_kafka_connection(common.KAFKA_CONSUMER, "url", PATH, "topic")
        self.assertIsNotNone(consumer)
        mock_consumer.assert_called_once_with("topic", bootstrap_servers='url',
                                            security_protocol='SSL',
                                            ssl_cafile=os.path.join(PATH, "ca.pem"),
                                            ssl_certfile=os.path.join(PATH, "service.cert"),
                                            ssl_keyfile=os.path.join(PATH, "service.key"))
        #can't check the type directly as its mocked but can infer right one was called
        mock_producer.assert_not_called()

    @mock.patch("kafka.KafkaConsumer", autospec=True)
    @mock.patch("kafka.KafkaProducer", autospec=True)
    def test_kafka_connect_producer(self, mock_producer, mock_consumer):
        PATH = "path"
        producer = common.get_kafka_connection(common.KAFKA_PRODUCER, "url", PATH, "topic")
        self.assertIsNotNone(producer)
        mock_producer.assert_called_once_with(bootstrap_servers='url',
                                            security_protocol='SSL',
                                            ssl_cafile=os.path.join(PATH, "ca.pem"),
                                            ssl_certfile=os.path.join(PATH, "service.cert"),
                                            ssl_keyfile=os.path.join(PATH, "service.key"))
        #can't check the type directly as its mocked but can infer right one was called
        mock_consumer.assert_not_called()
    
    @mock.patch("os.listdir")
    def test_check_ssl_files(self, mock_listdir):
        mock_listdir.return_value = ['ca.pem', 'service.cert', 'service.key']
        result0 = common.check_kafka_ssl_files("path")
        self.assertTrue(result0)
        
        mock_listdir.return_value = ['ca.pem', 'service.cert', 'service.key', 'some.file']
        result1 = common.check_kafka_ssl_files("path")
        self.assertTrue(result1)
        
        mock_listdir.return_value = ['ca.pem', 'service.cert']
        try:
            result2 = common.check_kafka_ssl_files("path")
        except FileNotFoundError:
            result2 = False
        self.assertFalse(result2)
    
    def test_consumer_setup_db(self):
        #simulate the table already exists case
        with mock.patch("psycopg2.connect") as mock_db_conn:
            mock_db_conn.cursor.return_value.fetchone.return_value = [True]
            mock_db_conn.cursor.return_value.execute.return_value = True
            consumer.setup_db(mock_db_conn)
        mock_db_conn.cursor.return_value.execute.assert_called_once()
        mock_db_conn.cursor.return_value.close.assert_called_once()
        
        #simulate the table NOT already exists case
        with mock.patch("psycopg2.connect") as mock_db_conn:
            mock_db_conn.cursor.return_value.fetchone.return_value = [False]
            mock_db_conn.cursor.return_value.execute.return_value = True
            try:
                consumer.setup_db(mock_db_conn)
            except RuntimeError:
                pass #expected
        
        #should be 1 call for the exists check, 1 for the CREATE TABLE, then another exists check
        self.assertEqual(mock_db_conn.cursor.return_value.execute.call_count, 3)
    
    @mock.patch("common.get_kafka_connection")
    @mock.patch("common.connect_to_postgresql")
    @mock.patch("common.check_kafka_ssl_files")
    def test_consumer_main(self, mock_check_ssl_files, mock_connect_to_psql, mock_get_kafka):
        class Message():
            '''exists only to simulate a kafka message object'''
            def __init__(self, value):
                self.value = value
                self.timestamp = 1609207728871

        with mock.patch.object(kafka.KafkaConsumer, '__iter__', return_value=iter([1,2,3])):
            msg = {"url": "some.thing", "response_time": 0, "timestamp":1609207728871,
                        "status_code":200, "matched_string":"hello"}
            json_msg0 = json.dumps(msg)
            msg['url'] = "blah.blah"
            json_msg1 = json.dumps(msg)
            #mocked consumer will produce 2 messages
            mock_get_kafka.return_value = [Message(json_msg0), Message(json_msg1)]
            with mock.patch("psycopg2.connect") as mock_db_conn:
                mock_connect_to_psql.return_value = mock_db_conn
                mock_db_conn.cursor.return_value.execute.return_value = True
                result = consumer.main("kafka.something.com", "some_topic", 
                                        "postgesql.something.com", "a_path")
                                    
        self.assertTrue(result)    
        
        mock_check_ssl_files.assert_called_once_with("a_path")
        mock_connect_to_psql.assert_called_once_with("postgesql.something.com")
        mock_get_kafka.assert_called_once_with(common.KAFKA_CONSUMER, "kafka.something.com",
                                                        "a_path", "some_topic")

        #leaving next line in as useful to see the calls made if needed
        #mock_db_conn.cursor.return_value.execute.assert_called_once()

        #should be 2 executes for the messages and 1 for the table exists check
        self.assertEqual(mock_db_conn.cursor.return_value.execute.call_count, 3)
        #should be 2 commits, one for each message insert
        self.assertEqual(mock_db_conn.commit.call_count, 2)

        

if __name__ == '__main__':
    unittest.main()
