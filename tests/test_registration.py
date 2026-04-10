import json
import time
import uuid
import pika
import pytest
from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.helpers.kafka.consumers.register_events_errors import RegisterEventsErrorsSubscriber
from framework.helpers.rmq.dm_mail_sending import DmMailSending
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer
from framework.internal.rmq.consumer_rmq import Consumer
from framework.internal.rmq.publisher_rmq import RmqPublisher


@pytest.fixture
def valid_user_data() -> dict[str, str]:
    base = uuid.uuid4().hex
    return {
        "login": base,
        "email": f"{base}@email.ru",
        "password": "123123",
    }


@pytest.fixture
def invalid_user_data() -> dict[str, str]:
    return {
        "login": "string",
        "email": "string",
        "password": "1",
    }


@pytest.fixture
def invalid_data_for_unknown_type_error() -> dict[str, str]:
    return {
        "input_data": {
            "login": "string",
            "email": "string@gmail.com",
            "password": "string"
        },
        "error_message": {
            "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
            "title": "Validation failed",
            "status": 400,
            "errors": {
                "Email": ["Taken"]
            }
        },
        "error_type": "unknown"
    }


def test_failed_registration(account: AccountApi, email: MailApi) -> None:
    expected_mail = "string@mail.ru"
    account.register_user(login="string", password="string", email="string")
    email.find_message(query="string")
    for _ in range(10):
        response = email.find_message(query=expected_mail)
        if response.json()["total"] > 0:
            raise AssertionError("Email not found")
        time.sleep(1)


@pytest.mark.skip(reason="Channel pool is exhausted, only 16 connections allowed per pool', 'status': 500")
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


@pytest.mark.skip(reason="Channel pool is exhausted, only 16 connections allowed per pool', 'status': 500")
def test_successful_registration_with_kafka_producer(valid_user_data: dict[str, str], email: MailApi,
                                                     kafka_producer: Producer) -> None:
    login = valid_user_data["login"]
    kafka_producer.send('register-events', valid_user_data)
    for _ in range(10):
        response = email.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_successful_registration_with_kafka_producer_consumer(kafka_producer: Producer,
                                                              register_events_subscriber: RegisterEventsSubscriber,
                                                              valid_user_data) -> None:
    login = valid_user_data["login"]
    kafka_producer.send('register-events', valid_user_data)
    register_events_subscriber.find_message(login=login)


@pytest.mark.skip(reason="Channel pool is exhausted, only 16 connections allowed per pool', 'status': 500")
def test_successful_registration_via_subscriber(register_events_subscriber: RegisterEventsSubscriber,
                                                valid_user_data,
                                                account: AccountApi, email: MailApi) -> None:
    login = valid_user_data["login"]
    account.register_user(**valid_user_data)
    register_events_subscriber.find_message(login=login)

    for _ in range(10):
        response = email.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_negative_registration_with_validation_type_error(register_events_subscriber: RegisterEventsSubscriber,
                                                          register_events_errors: RegisterEventsErrorsSubscriber,
                                                          invalid_user_data,
                                                          account: AccountApi) -> None:
    login = invalid_user_data["login"]
    account.register_user(**invalid_user_data)
    register_events_subscriber.find_message(login=login)
    register_events_errors.find_error_message(login=login, error_type="validation")


def test_negative_registration_with_unknown_type_error(

        invalid_data_for_unknown_type_error,
        kafka_producer: Producer,
        register_events_errors: RegisterEventsErrorsSubscriber,
        register_events_subscriber: RegisterEventsSubscriber,
) -> None:
    message = invalid_data_for_unknown_type_error
    login = message['input_data']["login"]

    kafka_producer.send('register-events-errors', message)
    print(f"DEBUG: Unknown error is pushed for login {login}")

    register_events_errors.find_error_message(login=login, error_type="unknown")
    register_events_errors.find_error_message(login=login, error_type="validation")


@pytest.mark.parametrize("i", range(10))
def test_send_registration(i, account: AccountApi, valid_user_data) -> None:
    account.register_user(**valid_user_data)


def test_rmq(rmq_publisher: RmqPublisher, email: MailApi) -> None:
    address = f'{uuid.uuid4().hex}@tut.by'
    print(f' my new created address is {address}')
    message = {
        "address": address,
        "subject": "Published message",
        "body": "Published message",
    }
    rmq_publisher.publish("dm.mail.sending", message, "")
    email.find_message(query=address)

    for _ in range(10):
        response = email.find_message(query=address)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_rmq_subscriber():
    with DmMailSending() as consumer:
        message = consumer.get_message()
        print(f' my new created message is {message}')


def test_success_registration_with_rmq(rmq_dm_mail_sending_consumer: DmMailSending,
                                       register_events_subscriber: RegisterEventsSubscriber,
                                       account: AccountApi, email: MailApi,
                                       valid_user_data: dict[str, str]) -> None:
    login = valid_user_data["login"]
    account.register_user(**valid_user_data)
    register_events_subscriber.find_message(login=login)
    rmq_dm_mail_sending_consumer.find_message(login=login)
    for _ in range(30):
        response = email.find_message(query=login)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")
