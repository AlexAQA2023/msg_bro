import time
import uuid
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer
from framework.internal.utils.utils import extract_token_from_email_body


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


def test_register_events_error_consumer_with_kafka(user_data: dict, email: MailApi, kafka_producer: Producer) -> None:
    error_event = {
        "input_data": {
            "login": user_data["login"],
            "email": user_data["email"],
            "password": user_data["password"]
        },
        "error_message": "Registration error example",
        "error_type": "registration_error"
    }

    kafka_producer.send('register-events-errors', error_event)
    base = user_data["login"]

    for _ in range(10):
        response = email.find_message(query=base)
        if response.json().get("total", 0) > 0:
            break
        time.sleep(1)
    else:
        raise AssertionError(f"Email to '{error_event['email']}' not found after waiting")

def test_activate_registered_user_by_email_token(user_data: dict, email: MailApi, kafka_producer: Producer,
                                                 account: AccountApi) -> None:
    unique_user_data = account.register_user(
        login=user_data["login"],
        password=user_data["password"],
        email=user_data["email"]
    )
    assert unique_user_data.status_code == 201

    base = user_data["login"]

    message = None
    for _ in range(15):
        response = email.find_message(query=base)
        data = response.json()
        if data.get("total", 0) > 0:
            message = data["items"][0]
            break
        time.sleep(1)
    else:
        raise AssertionError("Confirmation link not found")

    body = message["Content"]["Body"]
    token = extract_token_from_email_body(body)

    activation_resp = account.activate_user(token)
    assert activation_resp.status_code == 200