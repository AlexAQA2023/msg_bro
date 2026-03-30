import uuid
from typing import Any, Generator

import pytest

from framework.internal.http.account import AccountApi
from framework.internal.http.mail import MailApi
from framework.internal.kafka.producer import Producer


@pytest.fixture(scope='session')
def account() -> AccountApi:
    return AccountApi()


@pytest.fixture(scope='session')
def email() -> MailApi:
    return MailApi()


@pytest.fixture(scope='session')
def kafka_producer() -> Generator[Producer | Any, Any, None]:
    with Producer() as producer:
        yield producer

@pytest.fixture
def user_data() -> dict:
    base = uuid.uuid4().hex
    return {
        "login": base,
        "email": f"{base}@mail.ru",
        "password": "123123"
    }
import pytest

@pytest.fixture
def get_user_status():
    def _get_user_status(login: str) -> str:
        api_client = AccountApi()
        response = api_client.client.get(f"/users/{login}")

        if response.status_code != 200:
            return "unknown"

        user_data = response.json()
        return user_data.get("status", "unknown")
    return _get_user_status