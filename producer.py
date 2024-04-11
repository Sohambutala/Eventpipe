import random
from kafka import KafkaProducer
import json

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Topic you want to produce to
topic = 'clickstream'

# Message you want to send

# Send the message
for i in range(0, 50):
    message = {'echoflow': random.random()}
    producer.send(topic, message)

# Ensure all messages are sent and connection is closed properly
producer.flush()
producer.close()

print("Message sent to Kafka topic:", topic)