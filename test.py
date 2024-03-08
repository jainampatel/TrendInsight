from confluent_kafka import Producer, Consumer
import socket
import time
import random
import string

def kafka_producer(topic, key, value):
    # Producer example
    producer = Producer({'bootstrap.servers': 'localhost:9092', 'client.id': socket.gethostname()})

    # Produce a message with the given key and value to the specified topic
    producer.produce(topic, key=key, value=value)

    # Make sure to flush to send the message to Kafka
    producer.flush()

def generate_random_string(length=10):
    # Function to generate a random string of given length
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))

# Consumer example
consumer = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': '12345'})
consumer_topic = 'aahuhh'
consumer.subscribe([consumer_topic])

# Infinite loop to produce messages with random strings
while True:
    # Generate a random string
    random_string = generate_random_string()

    # Call the Kafka producer function with the random string
    kafka_producer(consumer_topic, key='key', value=random_string)

    # Sleep for 4 seconds
    time.sleep(4)
    
    # Poll for messages in the consumer
    msg = consumer.poll(1.0)
    if msg is not None:
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
        else:
            print('Received message: {}'.format(msg.value().decode('utf-8')))
