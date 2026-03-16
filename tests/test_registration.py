import json
import time
import uuid
from kafka import KafkaProducer

from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi


def test_failed_registration(account: AccountApi, email: MailApi) -> None:
    expected_mail = "string@mail.ru"
    account.register_user(login="string", password="string", email="string")
    email.find_message(query="string")
    for _ in range(10):
        response = email.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError("Email not found")
        time.sleep(1)


def test_successful_registration(account: AccountApi, email: MailApi) -> None:
    base = uuid.uuid4().hex
    account.register_user(login=base, password="123123", email=f"{base}@email.ru")
    email.find_message(query="string")
    for _ in range(10):
        response = email.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_successful_registration_with_kafka_producer(account: AccountApi, email: MailApi) -> None:
    base = uuid.uuid4().hex
    message = {
        "login": base,
        "password": "123123",
        "email": f"{base}@email.ru",
    }
    producer = KafkaProducer(bootstrap_servers=['185.185.143.231:9092'],
                             value_serializer=lambda m: json.dumps(m).encode("utf-8"),
                             acks="all",
                             retries=5,
                             retry_backoff_ms=5000,
                             request_timeout_ms=70000,
                             connections_max_idle_ms=65000,
                             reconnect_backoff_ms=10000,
                             reconnect_backoff_max_ms=10000,
                             )
    producer.send('register-events', message)
    producer.close()
    for _ in range(10):
        response = email.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
