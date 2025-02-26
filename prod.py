import json
import time
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer',
    'retries': 5,
    'retry.backoff.ms': 1000,
    'queue.buffering.max.ms': 5000,  # Buffer messages longer
}

producer = Producer(conf)


def delivery_callback(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_messages(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)

        for record in data:
            message = json.dumps(record)
            try:
                producer.produce(
                    'dss-payment-stream',
                    key=str(record["customer_id"]).encode('utf-8'),
                    value=message.encode('utf-8'),
                    callback=delivery_callback
                )
                print(f"Produced: {message[:50]}...")
                producer.poll(0)  # Trigger callbacks
                time.sleep(0.5)
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(2)  # Back off on error
            finally:
                producer.flush(timeout=5)  # Wait up to 5s for delivery


if __name__ == "__main__":
    produce_messages("merged_payments_customers.json")
    print("Producer finished.")