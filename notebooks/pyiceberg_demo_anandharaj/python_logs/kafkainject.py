from kafka import KafkaConsumer
import pyarrow as pa
import datetime
import json
import os
from schema import create_table_logs
from pyiceberg import load_catalog_config
namespace = os.getenv("NAME_SPACE", "docs_logs")
table_name = os.getenv("TABLE_NAME", "logs")

def append_data(data, table):
    final = {}
    for key in data['resourceLogs'][0]['resource']['attributes']:
        if key['key'] == 'service.name':
            final.update({
                "servicename": key['value']['stringValue'],
            })
        if key['key'] == 'telemetry.sdk.language':
            final.update({
                "language": key['value']['stringValue'],
                "logsdata": json.dumps(data),
                "createdTime": datetime.datetime.now()
            })
            df = pa.Table.from_pylist([final])
            table.append(df)
            print("Data appended successfully.")
            final = {}

def load_consume():
    consumer = None
    try:
        consumer = KafkaConsumer(
            "apmlogs",
            bootstrap_servers=['telemetry-kafka-external-bootstrap-observability-kafka.apps.zagaopenshift.zagaopensource.com:443'],
            value_deserializer=lambda v: v,  # Keep value as bytes for JSON decoding
            group_id="apmingestion",
            auto_offset_reset='earliest',
            consumer_timeout_ms=10000,
            security_protocol='SSL',
            ssl_cafile='./telemetry_zagaopenshift.pem',
            ssl_certfile='./telemetry_zagaopenshift.crt'
        )
        print("Kafka consumer created successfully.")
    except FileNotFoundError as e:
        print(f"SSL certificate file not found: {e}")
    except Exception as e:
        print(f"Error creating Kafka consumer: {e}")
    return consumer

def kafka_injection():
    consumer = None
    try:
        catalog=load_catalog_config()
        tables = catalog.list_tables(namespace)
        full_table_name = (namespace, table_name)
        if full_table_name in tables:
            print(f"Table {full_table_name} already exists.")
            table = catalog.load_table(full_table_name)
        else:
            table = create_table_logs(namespace, table_name, catalog)

        consumer = load_consume()
        if consumer:
            for message in consumer:
                data = json.loads(message.value.decode('utf-8'))
                append_data(data, table)
        else:
            print("Failed to create Kafka consumer. Exiting.")
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Message consumption closed.")
