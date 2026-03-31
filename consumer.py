import json
import threading
import time
import queue

from kafka import KafkaConsumer

q = queue.Queue()
running = threading.Event()


def consume_messages():
    consumer = KafkaConsumer(
        "register-events",
        bootstrap_servers=['185.185.143.231:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )
    try:
        while running.is_set():
            messages = consumer.poll(timeout_ms=1000, max_records=10)
            for topic_partition, records in messages.items():
                for record in records:
                    print(record)
                    q.put(record)
        print("Stop consuming")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()

running.set()
thread = threading.Thread(target=consume_messages, daemon=True)
thread.start()
time.sleep(1)


def get_message():
    try:
        return q.get(timeout=90)
    except queue.Queue:
        raise AssertionError("Queue is empty")


print(get_message())
running.clear()
time.sleep(2)

print('Stop')
