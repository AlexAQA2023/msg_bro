import pytest

from framework.helpers.kafka.consumers.register_events import RegisterEventsSubscriber
from framework.helpers.kafka.consumers.register_events_errors import RegisterEventsErrorsSubscriber
from framework.internal.kafka.consumer import Consumer
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
def kafka_producer() -> Producer:
    with Producer() as producer:
        yield producer

@pytest.fixture(scope='session')
def register_events_subscriber() -> RegisterEventsSubscriber:
    return RegisterEventsSubscriber()

@pytest.fixture(scope='session')
def register_register_events_errors() -> RegisterEventsErrorsSubscriber:
    return RegisterEventsErrorsSubscriber()

@pytest.fixture(scope='session', autouse=True)
def kafka_consumer(
    register_events_subscriber: RegisterEventsSubscriber,
    register_register_events_errors: RegisterEventsErrorsSubscriber # Добавили сюда
) -> Consumer:
    # Теперь передаем ОБА сабскрайбера в список
    with Consumer(subscribers=[
        register_events_subscriber,
        register_register_events_errors
    ]) as consumer:
        yield consumer

