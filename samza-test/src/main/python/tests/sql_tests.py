# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import util
import logging
from io import BytesIO
import zopkio.runtime as runtime
from kafka import SimpleProducer, SimpleConsumer
import avro.schema
from avro.io import DatumReader, DatumWriter, BinaryEncoder, BinaryDecoder
from StringIO import StringIO
import csv
import time


logger = logging.getLogger(__name__)

DEPLOYER = 'samza_job_deployer'
FILTER_JOB_ID = 'sql_filter_test'
PROJECT_JOB_ID = 'sql_project_test'
PACKAGE_ID = 'tests'
FILTER_CONFIG_FILE = 'config/sql-filter.properties'
PROJECT_CONFIG_FILE = 'config/sql-project.properties'
TEST_INPUT_TOPIC = 'orders'
FILTER_TEST_OUTPUT_TOPIC = 'filteredorders'
PROJECT_OUTPUT_TOPIC = 'filteredprojectedorders'
NUM_MESSAGES = 5

ORDERS_AVRO_SCHEMA = """
{
     "type": "record",
     "namespace": "org.apache.samza",
     "name": "Orders",
     "fields": [
       { "name": "id", "type": "int" },
       { "name": "productId", "type": "string" },
       { "name": "units", "type": "int"},
       { "name": "_timestamp", "type": "long"}
     ]
}
"""

PROJECTED_ORDERS_AVRO_SCHEMA = """
{
     "type": "record",
     "namespace": "org.apache.samza",
     "name": "Orders",
     "fields": [
       { "name": "productId", "type": "string" },
       { "name": "quantity", "type": "int"}
     ]
}
"""

ORDERS_CSV = """
1234,product1,10
1235,product1,5
1345,product2,6
1243,product3,7
1236,product2,8
1265,product4,9
1254,product1,3
1287,product5,14
"""

ORDERS_AVRO_SCHEMA_OBJ = avro.schema.parse(ORDERS_AVRO_SCHEMA)
PROJECTED_ORDERS_AVRO_SCHEMA_OBJ = avro.schema.parse(PROJECTED_ORDERS_AVRO_SCHEMA)

def test_sql_filter_query():
    """
    Runs a job that filter incoming messages based on a streaming sql query and send the output to a Kafka topic.
    """
    _load_data()
    util.start_job(PACKAGE_ID, FILTER_JOB_ID, FILTER_CONFIG_FILE)
    util.await_job(PACKAGE_ID, FILTER_JOB_ID)

def validate_sql_filter_query():
    """
    Validates filter-query output.
    """
    logger.info('Running validate_sql_filter_query')
    kafka = util.get_kafka_client()
    kafka.ensure_topic_exists(FILTER_TEST_OUTPUT_TOPIC)
    consumer = SimpleConsumer(kafka, 'sql-filter-query-group', FILTER_TEST_OUTPUT_TOPIC)
    messages = consumer.get_messages(count=NUM_MESSAGES, block=True, timeout=300)
    message_count = len(messages)
    assert NUM_MESSAGES == message_count, 'Expected {0} lines, but found {1}'.format(NUM_MESSAGES, message_count)
    for message in map(lambda m: m.message.value, messages):
        reader = BytesIO(message)
        decoder = BinaryDecoder(reader)
        datum_reader = DatumReader(ORDERS_AVRO_SCHEMA_OBJ)
        output = datum_reader.read(decoder)
        units = output["units"]
        assert units > 6 , 'Expected orders where units > 6, but found an order with units {0}'.format(units)
    kafka.close()

def test_sql_project():
    """
    Runs a job that filter and project incoming messages based on a streaming sql query and send the output to a Kafka topic.
    """
    _load_data()
    util.start_job(PACKAGE_ID, PROJECT_JOB_ID, PROJECT_CONFIG_FILE)
    util.await_job(PACKAGE_ID, PROJECT_JOB_ID)

def validate_sql_project():
    """
    Validates filter and project output.
    """
    logger.info('Running validate_sql_filter_query')
    kafka = util.get_kafka_client()
    kafka.ensure_topic_exists(PROJECT_OUTPUT_TOPIC)
    consumer = SimpleConsumer(kafka, 'sql-filter-query-group', PROJECT_OUTPUT_TOPIC)
    messages = consumer.get_messages(count=NUM_MESSAGES, block=True, timeout=300)
    message_count = len(messages)
    assert NUM_MESSAGES == message_count, 'Expected {0} lines, but found {1}'.format(NUM_MESSAGES, message_count)
    for message in map(lambda m: m.message.value, messages):
        reader = BytesIO(message)
        decoder = BinaryDecoder(reader)
        datum_reader = DatumReader(PROJECTED_ORDERS_AVRO_SCHEMA_OBJ)
        output = datum_reader.read(decoder)
        quantity = output["quantity"]
        assert quantity > 6 , 'Expected orders where units > 6, but found an order with units {0}'.format(quantity)
    kafka.close()


def _load_data():
    logger.info('Running test_sql_filter_query')
    kafka = util.get_kafka_client()
    kafka.ensure_topic_exists(TEST_INPUT_TOPIC)
    producer = SimpleProducer(kafka, async=False, req_acks=SimpleProducer.ACK_AFTER_CLUSTER_COMMIT, ack_timeout=30000)

    ts = long(round(time.time() * 1000)) - 5 * 1000
    csv_reader = csv.reader(StringIO(ORDERS_CSV), delimiter=',')
    for row in csv_reader:
        if len(row) == 3:
            id = int(row[0])
            productId = row[1]
            units = int(row[2])
            order = {"id": id, "productId": productId, "units": units, "_timestamp": ts}
            producer.send_messages(TEST_INPUT_TOPIC, _create_avro_order(order))
            ts = ts + 500

    kafka.close()

def _create_avro_order(order):
    writer = BytesIO()
    encoder = BinaryEncoder(writer)
    datum_writer = DatumWriter(ORDERS_AVRO_SCHEMA_OBJ)
    datum_writer.write(order, encoder)
    encoded_message = writer.getvalue()
    writer.close()
    return encoded_message