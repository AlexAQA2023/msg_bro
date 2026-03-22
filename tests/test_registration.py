import time
import uuid
from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


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


def generate_error_message():
    return {
        "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
        "title": "Validation failed",
        "status": 400,
        "traceId": uuid.uuid4().hex,  # уникальный traceId
        "errors": {
            "Email": ["Invalid"]
        }
    }


def test_register_events_error_consumer(email: MailApi, kafka_producer: Producer) -> None:
     base = uuid.uuid4().hex
     input_data = {
         "login": base,
         "email": f"{base}@mail.ru",
         "password": "123123123"
     }
     message = {
         "input_data": input_data,
         "error_message": generate_error_message(),
         "error_type": "unknown"
     }
     kafka_producer.send('register-events-errors', message)
     for _ in range(10):
         response = email.find_message(query=base)
         if response.json().get("total", 0) > 0:
             break
         time.sleep(1)
     else:
         raise AssertionError(f"Email to '{input_data['email']}' not found after waiting")
