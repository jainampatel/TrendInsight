from confluent_kafka import Consumer, KafkaError
import json

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'reddit_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Create a Kafka consumer instance
consumer = Consumer(conf)

# Subscribe to the 'reddit_data' topic
consumer.subscribe(['reddit_data'])

# Infinite loop to consume messages
while True:
    # Poll for new messages
    msg = consumer.poll(1.0)  # Adjust the timeout as needed

    if msg is None:
        continue
    if msg.error():
        # Handle any errors that occur during message consumption
        if msg.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event, not an error
            continue
        else:
            print('Error: {}'.format(msg.error()))
            break

    # Decode and print the message value (assuming it's in JSON format)
    try:
        message_data = json.loads(msg.value().decode('utf-8'))
        print('Received message:\n', message_data)
        print('\n============================================================')
    except json.JSONDecodeError as e:
        print('Error decoding JSON: {}'.format(e))

# Close the consumer
consumer.close()
