from confluent_kafka import Consumer, KafkaError
import json

# Configuration for the Kafka consumer
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka server
    'group.id': 'transaction_group',          # Consumer group ID
    'auto.offset.reset': 'earliest'          # Start reading at the earliest message
}

# Create a Consumer instance
consumer = Consumer(conf)

# Subscribe to the 'transaction' topic
consumer.subscribe(['transaction'])

def consume_messages():
    try:
        while True:
            # Poll for new messages
            msg = consumer.poll(1.0)  # Timeout of 1 second

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    print(f"Error: {msg.error()}")
                    break

            # Deserialize the message value
            try:
                transaction_data = json.loads(msg.value().decode('utf-8'))
                print(f"Received transaction: {transaction_data}")
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON: {e} - Message: {msg.value()}")

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer to commit final offsets
        consumer.close()

if __name__ == "__main__":
    consume_messages()