import time

from framework.internal.kafka.subscriber import Subscriber


class RegisterEventsErrorsSubscriber(Subscriber):
    topic: str = "register-events-errors"

    def find_error_message(self, login: str, error_type: str, timeout: float = 20) -> dict:
        start_time = time.time()
        print(f"DEBUG: Searching for login {login} in {self.topic}...")

        while time.time() - start_time < timeout:
            try:

                message = self.get_message(timeout=1)

                data = message.value if hasattr(message, 'value') else message

                input_data = data.get("input_data", {})
                actual_login = input_data.get("login")
                actual_error_type = data.get("error_type")

                if actual_login == login and actual_error_type == error_type:
                    print(f"DEBUG: Found target message for {login}")
                    return data

            except Exception:
                continue

        raise AssertionError(
            f"Message for login '{login}' with error_type '{error_type}' "
            f"not found in topic '{self.topic}' within {timeout} seconds."
        )
