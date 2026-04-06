import time

from framework.internal.kafka.subscriber import Subscriber


class RegisterEventsErrorsSubscriber(Subscriber):
    topic: str = "register-events-errors"

    def find_error_message(self, login: str, error_type: str = "validation", timeout: float = 20) -> dict:
        start_time = time.time()
        while time.time() - start_time < timeout:
            message = self.get_message(timeout=timeout)
            print(message)
            if message.value["login"] == login and message.value.get("error_message") == error_type:
                break
        raise AssertionError(f"Error message for login '{login}' with error_type '{error_type}' not found.")
