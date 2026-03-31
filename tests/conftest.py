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
