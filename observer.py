from typing import Protocol
class Observer(Protocol):
    def update(self, message: str): ...


class Subject:
    def __init__(self):
        self._subscribers: list = []

    def register(self, observer: 'Observer'):
        self._subscribers.append(observer)

    def notify(self, message: str):
        for observer in self._subscribers:
            observer.update(message)

class RegisterEventsSubscriber:
    def __init__(self):
        self.messages = []

    def update(self, message: str):
        self.messages.append(message)
        print(f'Register event got {message}')

    def get_messages(self):
        return self.messages

class AnotherTopicSubscriber:
    def __init__(self):
        self.messages = []

    def update(self, message: str):
        self.messages.append(message)
        print(f'Another topic got  {message}')

    def get_messages(self):
        return self.messages

if __name__ == '__main__':
    subject = Subject()
    s1 = AnotherTopicSubscriber()
    s2 = RegisterEventsSubscriber()
    subject.register(s1)
    subject.register(s2)
    subject.notify('hello')
    s1.get_messages()
    s2.get_messages()