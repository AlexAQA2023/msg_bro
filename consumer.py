import json
import threading
import time

from kafka import KafkaConsumer


def consume_messages():
    consumer = KafkaConsumer(
        "register-events",
        bootstrap_servers=['185.185.143.231:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )
    try:
        for message in consumer:
            print(message.value["login"])
    except Exception as e:
        print(e)
    finally:
        consumer.close()

thread = threading.Thread(target=consume_messages, daemon=True)
thread.start()
time.sleep(10)
print('Stop')
