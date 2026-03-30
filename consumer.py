import json
import threading
import time
import queue
from kafka import KafkaConsumer

q = queue.Queue()

def consume_messages():
    consumer = KafkaConsumer(
        "register-events",
        bootstrap_servers=['185.185.143.231:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )
    try:
        for message in consumer:
            print(message.value["login"])
            q.put(message)
    except Exception as e:
        print(e)
    finally:
        consumer.close()

thread = threading.Thread(target=consume_messages, daemon=True)
thread.start()
time.sleep(1)

def get_message():
    try:
        return q.get(timeout=90)
    except queue.Queue:
        raise AssertionError("Queue is empty")
print(get_message())
print('Stop')
