import json
import threading
import time
import uuid
from collections import defaultdict

from kafka import KafkaConsumer

from framework.internal.kafka.subscriber import Subscriber
from framework.internal.singleton import Singleton


class Consumer(Singleton):
    _started = False

    def __init__(self, subscribers: list[Subscriber], consumer_group: str = f'python_art_group',
                 bootstrap_servers=['185.185.143.231:9092']):
        self._bootstrap_servers = bootstrap_servers
        self._subscribers = subscribers
        self.consumer_group = consumer_group
        self._consumer: KafkaConsumer | None = None
        self._running = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._watchers: dict[str, list[Subscriber]] = defaultdict(list)

    def register(self):
        if self._subscribers is None:
            raise AssertionError("Subscriber is not initialized")
        if self._started:
            raise AssertionError("Consumer already started")
        for sub in self._subscribers:
            self._watchers[sub.topic].append(sub)

    def start(self):
        self._consumer = KafkaConsumer(
            *self._watchers.keys(),
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset='latest',
            group_id=self.consumer_group,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),

        )
        self._consumer.poll(timeout_ms=100)
        self._running.set()
        self._ready.clear()
        self._thread = threading.Thread(target=self._consume, daemon=True)
        self._thread.start()

        if not self._ready.wait(timeout=10):
            raise RuntimeError("Consumer is not ready yet")
        self._started = True

    def _consume(self):
        self._ready.set()
        print("Consumer started...")
        try:
            while self._running.is_set():
                messages = self._consumer.poll(timeout_ms=1000, max_records=10)
                for topic_partition, records in messages.items():
                    topic = topic_partition.topic
                    for record in records:
                        for watcher in self._watchers[topic]:
                            print(f' {topic}: {record}')
                            try:
                                watcher.handle_message(record)
                            except Exception:
                                print(f' Error while handling message: {record}, watcher {watcher.topic}')
                    time.sleep(0.1)
                if not messages:
                    time.sleep(0.1)

        except Exception as e:
            print(f"Error: {e}")

    def stop(self):
        self._running.clear()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
            if self._thread.is_alive():
                print("Thread is still alive")
        if self._consumer:
            try:
                self._consumer.close(timeout_ms=2000)
                print("Stop consuming")
            except Exception as e:
                print(f"Error while closing consumers: {e}")
        del self._consumer
        self._watchers.clear()
        self._subscribers.clear()
        self._started = False
        print("Consumer stopped")

    def __enter__(self):
        self.register()
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
