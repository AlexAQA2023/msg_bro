import json
import time
import uuid
from urllib.parse import urlparse

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
def extract_token_from_email_body(body: str) -> str:
    # Парсим JSON из строки body
    data = json.loads(body)
    confirmation_url = data.get("ConfirmationLinkUrl", "")
    # Разбираем URL
    path = urlparse(confirmation_url).path  # например: /activate/49b68497-c0cb-44e0-81aa-c8e8ffc85e62
    token = path.rsplit('/', 1)[-1]  # берём последнюю часть пути после последнего слеша
    return token

def activate_user(self, token: str):
    url = "http://185.185.143.231:8085/register/user/activate"
    params = {"token": token}
    return self.session.put(url, params=params, headers={"accept": "application/json"})

def test_register_events_error_consumer(email: MailApi, kafka_producer: Producer, account: AccountApi) -> None:
    base = uuid.uuid4().hex
    user_email = f"{base}@mail.ru"
    login = base
    password = "123123"

    # Шаг 1: Регистрируем пользователя
    resp = account.register_user(login=login, password=password, email=user_email)
    assert resp.status_code == 201

    # Шаг 2: Ждём письмо с подтверждением
    message = None
    for _ in range(15):
        response = email.find_message(query=base)
        data = response.json()
        if data.get("total", 0) > 0:
            message = data["items"][0]
            break
        time.sleep(1)
    else:
        raise AssertionError("Письмо с подтверждением не найдено")

    # Шаг 3: Извлекаем token из тела письма
    body = message["Content"]["Body"]
    token = extract_token_from_email_body(body)
    print(token)

    # Далее можно использовать token для активации
    activation_resp = account.activate_user(token)
    assert activation_resp.status_code == 200
