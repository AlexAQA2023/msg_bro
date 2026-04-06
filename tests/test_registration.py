import time
import uuid
import pytest
from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.helpers.kafka.consumers.register_events_errors import RegisterEventsErrorsSubscriber
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


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
    base = uuid.uuid4().hex
    return {
        "login": base,
        "email": f"{base}@email.ru",
        "password": "1",
    }


@pytest.fixture
def already_taken_user_data() -> dict[str, str]:
    return {
        "login": "test_user_string",
        "email": "test_user_string@gmail.com",
        "password": "string",
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


def test_successful_registration_with_kafka_producer(email: MailApi, kafka_producer: Producer) -> None:
    base = uuid.uuid4().hex
    message = {
        "login": base,
        "password": "123123",
        "email": f"{base}@email.ru",
    }

    kafka_producer.send('register-events', message)
    for _ in range(10):
        response = email.find_message(query=base)
        if response.json()["total"] > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError("Email not found")


def test_successful_registration_with_kafka_producer_consumer(kafka_producer: Producer,
                                                              register_events_subscriber: RegisterEventsSubscriber) -> None:
    time.sleep(2)
    base = uuid.uuid4().hex
    message = {
        "login": base,
        "password": "123123",
        "email": f"{base}@email.ru",
    }

    kafka_producer.send('register-events', message)
    register_events_subscriber.find_message(login=base)


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
        kafka_producer: Producer,
        valid_user_data,
        register_events_errors: RegisterEventsErrorsSubscriber
) -> None:
    login = valid_user_data["login"]

    message = {
        "input_data": {
            "login": login,
            "email": f"{login}@gmail.com",
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

    kafka_producer.send('register-events-errors', message)
    print(f"DEBUG: Unknown error is pushed for login: {login}")

    register_events_errors.find_error_message(login=login, error_type="unknown", timeout=20)

