import json
import threading
from confluent_kafka import Consumer, KafkaError

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'dss-payment-group',
    'auto.offset.reset': 'earliest'
}


# Data processing function
def process_record(record):
    # Parse JSON
    data = json.loads(record)

    # Cleaning
    if "payment_type" not in data or data["payment_type"] is None:
        data["payment_type"] = "unknown"
    if "amount" not in data or data["amount"] < 0:
        return None  # Discard negative amounts

    # Category transformation
    payment_type_map = {"cash": 0, "card": 1, "unknown": -1}
    data["payment_type_encoded"] = payment_type_map.get(data["payment_type"], -1)

    data["customer_active_encoded"] = 1 if data.get("activebool", False) else 0

    if data["amount"] < 2:
        data["amount_category"] = 1  # small
    elif 2 <= data["amount"] <= 5:
        data["amount_category"] = 2  # medium
    else:
        data["amount_category"] = 3  # large

    # Business logic: High-value transactions
    data["is_high_value"] = data["amount"] > 8

    return data


# Consumer function
def consume_messages(consumer_id):
    consumer = Consumer(conf)
    consumer.subscribe(['dss-payment-stream'])

    print(f"Consumer {consumer_id} started.")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Consumer {consumer_id}: End of partition {msg.partition()}")
                else:
                    print(f"Consumer {consumer_id} Error: {msg.error()}")
            else:
                record = msg.value().decode('utf-8')
                processed = process_record(record)
                if processed:
                    print(f"Consumer {consumer_id} Processed: {json.dumps(processed, indent=2)}")
    except KeyboardInterrupt:
        print(f"Consumer {consumer_id} interrupted")
    finally:
        consumer.close()


if __name__ == "__main__":
    # Simulate two consumers in threads (in practice, run in separate processes)
    t1 = threading.Thread(target=consume_messages, args=(1,))
    t2 = threading.Thread(target=consume_messages, args=(2,))

    t1.start()
    t2.start()

    t1.join()
    t2.join()