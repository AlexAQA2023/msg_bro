import time

from framework.internal.kafka.subscriber import Subscriber


class RegisterEventsSubscriber(Subscriber):
    topic: str = "register-events"

    def find_message(self, login: str, timeout: float = 20) -> None:
        start_time = time.time()
        print(f"DEBUG: Searching for login {login} in {self.topic}...")

        while time.time() - start_time < timeout:

            message = self.get_message(timeout=timeout)
            if message.value["login"] == login:
                break
        else:
            raise AssertionError(f"Message for topic:{self.topic} not found. ")
