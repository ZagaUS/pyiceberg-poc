from kafka import KafkaConsumer
import json
        # Create KafkaConsumer
consumer = KafkaConsumer(
        "apmtraces",
        bootstrap_servers=['telemetry-kafka-external-bootstrap-observability-kafka.apps.zagaopenshift.zagaopensource.com:443'],
        value_deserializer=lambda v: v,  # Keep value as bytes for JSON decoding
        group_id="apmingestion",
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
        security_protocol='SSL',  # Add SSL configuration if needed
        ssl_cafile='./python/trace_ingestion/telemetry_zagaopenshift.pem',
        ssl_certfile='./python/trace_ingestion/telemetry_zagaopenshift.crt'
)
print("Kafka consumer created successfully.")

def consuming_message():
    messages = []
    try:
        for message in consumer:
            json_message = json.loads(message.value.decode('utf-8'))  # Decode and parse JSON
            messages.append(json_message)  # Collect each JSON message
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        consumer.close()
        print("Message consumption completed.",messages)
    return message

consuming_message()