import json
import queue
import threading
import time
import uuid

import pika
from framework.internal.singleton import Singleton


class Consumer(Singleton):
    _started = False

    def __init__(self, url='amqp://guest:guest@185.185.143.231:5672'):
        self._url = url
        params = pika.URLParameters(self._url)
        params.heartbeat = 600
        params.blocked_connection_timeout = 300
        self._connection = pika.BlockingConnection(pika.URLParameters(self._url))
        self._channel = self._connection.channel()
        self._running = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._messages: queue.Queue = queue.Queue()
        self._queue_name: str = ''

    @property
    def exchange(self):
        raise NotImplementedError("Set exchange")

    @property
    def routing_key(self):
        raise NotImplementedError("Set routing_key")

    def _start(self):
        result = self._channel.queue_declare(queue='', exclusive=True, auto_delete=True, durable=True)
        self._queue_name = result.method.queue
        print(f"Declare queue with name {self._queue_name}")

        self._channel.queue_bind(queue=self._queue_name, exchange=self.exchange,
                                 routing_key=self.routing_key)
        self._running.set()
        self._ready.clear()
        self._thread = threading.Thread(target=self._consume, daemon=True)
        self._thread.start()

        if not self._ready.wait(timeout=10):
            raise RuntimeError("Consumer is not ready yet")
        self._started = True

    def get_message(self, timeout: float = 10):
        try:
            return self._messages.get(timeout=timeout)
        except queue.Empty:
            raise AssertionError(f"No messages from topic: {self._queue_name}, within timeout: {timeout}")

    # def _consume(self):
    #     print("Consumer RMQ started...")
    #
    #     def on_message_callback(ch, method, properties, body):
    #         try:
    #             body_str = body.decode('utf-8')
    #             try:
    #                 data = json.loads(body_str)
    #             except json.decoder.JSONDecodeError:
    #                 data = body_str
    #             self._messages.put(data)
    #             print("Message received", data)
    #         except Exception as e:
    #             print(f'Error while processing rmq {e}')
    #
    #     self._channel.basic_consume(self._queue_name, on_message_callback, auto_ack=True)
    #
    #     self._ready.set()
    #
    #     try:
    #         while self._running.is_set():
    #             self._connection.process_data_events(time_limit=0.1)
    #             time.sleep(0.01)
    #     except Exception as e:
    #         print(f"Error: {e}")

    # def _stop(self):
    #     self._running.clear()
    #     if self._thread and self._thread.is_alive():
    #         self._thread.join(timeout=2)
    #         if self._thread.is_alive():
    #             print("Thread is still alive")
    #     if self._channel:
    #         try:
    #             self._channel.close()
    #             print("Stop channel")
    #         except Exception as e:
    #             print(f"Error while closing consumers: {e}")
    #     if self._connection:
    #         try:
    #             self._connection.close()
    #             print("Close connection")
    #         except Exception as e:
    #             print(f"Error while closing consumers: {e}")
    #
    #     self._started = False
    #     print("Consumer stopped")
    def _consume(self):
        print(f"Consumer RMQ started on queue: {self._queue_name}")

        def on_message_callback(ch, method, properties, body):
            try:
                body_str = body.decode('utf-8')
                try:
                    data = json.loads(body_str)
                except json.decoder.JSONDecodeError:
                    data = body_str
                self._messages.put(data)
                print(f"Message received in {self._queue_name}: {data}")
            except Exception as e:
                print(f'Error while processing rmq message: {e}')

        try:
            # Регистрируем потребителя
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=on_message_callback,
                auto_ack=True
            )

            # Сигнализируем, что мы готовы (после basic_consume!)
            self._ready.set()

            # Основной цикл обработки событий
            while self._running.is_set():
                # Проверяем, не закрылось ли соединение извне
                if not self._connection or self._connection.is_closed:
                    print("Connection was closed from outside. Exiting consume loop.")
                    break

                # process_data_events сам делает небольшую паузу, sleep не нужен
                # time_limit=1 позволяет pika эффективнее обрабатывать heartbeats
                self._connection.process_data_events(time_limit=1)

        except pika.exceptions.ConnectionClosedByBroker:
            print("RMQ Connection closed by broker.")
        except pika.exceptions.AMQPConnectionError as e:
            print(f"RMQ Connection error: {e}")
        except Exception as e:
            # Это поймает тот самый IndexError('pop from an empty deque')
            if self._running.is_set():
                print(f"Unexpected error in consume loop: {e}")
        finally:
            self._running.clear()
            self._ready.clear()
            print("Consume loop finished.")

    def _stop(self):
        # 1. Сигнализируем потоку о необходимости остановиться
        self._running.clear()

        # 2. Ждем завершения потока (увеличим таймаут для CI)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                print("Warning: Consumer thread is still alive after timeout")

        # 3. Закрываем канал, только если соединение и канал всё еще открыты
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
                print("Stop channel")
        except Exception as e:
            # В тестах нам не важно, если канал уже был закрыт
            print(f"Channel already closed or error: {e}")

        # 4. Закрываем соединение, только если оно открыто
        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
                print("Close connection")
        except Exception as e:
            # Это предотвратит ошибку "called on closed connection"
            print(f"Connection already closed or error: {e}")

        self._started = False
        print("Consumer stopped")
    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop()
