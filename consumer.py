from kafka import KafkaConsumer
import json

# Kafka topic you want to consume
topic_name = 'discount'

# Kafka server address
bootstrap_servers = ['localhost:9092']

def safe_json_deserializer(x):
    try:
        gg = json.loads(x.decode('utf-8')) if x else None
        print("Deserialized Successfully")
        return gg
    except json.decoder.JSONDecodeError:
        print("Error")
        return x.decode('utf-8')  # Return the raw string if JSON decoding fails

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    # auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id=None,  # Do not commit offsets
    enable_auto_commit=False,  # Manual offset management
    value_deserializer=safe_json_deserializer
)

for message in consumer:
    print(f"Raw message: {message.value} or {type(message.value)}")


# Close the consumer connection
consumer.close()
