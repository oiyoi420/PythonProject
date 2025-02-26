import json
import csv
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'dss-logging-group',
    'auto.offset.reset': 'earliest'
}


# Logging consumer
def logging_consumer():
    consumer = Consumer(conf)
    consumer.subscribe(['dss-payment-stream'])

    # CSV file setup
    csv_file = "payment_stream_log.csv"
    header = ["timestamp", "customer_id", "amount", "payment_type", "raw_record"]

    # Check if file exists to write header only once
    write_header = not csv_file_exists(csv_file)

    print("Logging Consumer started.")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition {msg.partition()}")
                else:
                    print(f"Error: {msg.error()}")
            else:
                record = json.loads(msg.value().decode('utf-8'))
                timestamp = datetime.now().isoformat()

                # Log to CSV
                with open(csv_file, 'a', newline='') as f:
                    writer = csv.writer(f)
                    if write_header:
                        writer.writerow(header)
                        write_header = False
                    writer.writerow([
                        timestamp,
                        record.get("customer_id", ""),
                        record.get("amount", ""),
                        record.get("payment_type", ""),
                        json.dumps(record)
                    ])
                print(f"Logged record at {timestamp}")
    except KeyboardInterrupt:
        print("Logging Consumer interrupted")
    finally:
        consumer.close()


def csv_file_exists(file_path):
    try:
        with open(file_path, 'r'):
            return True
    except FileNotFoundError:
        return False


if __name__ == "__main__":
    logging_consumer()