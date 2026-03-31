# framework/helpers/email_helper.py (или в account_helper.py)
import json
from urllib.parse import urlparse


class EmailHelper:
    @staticmethod
    def extract_confirmation_token(body: str) -> str:
        try:
            data = json.loads(body)
            confirmation_url = data.get("ConfirmationLinkUrl", "")
            if not confirmation_url:
                raise ValueError("Поле ConfirmationLinkUrl не найдено в теле письма")

            path = urlparse(confirmation_url).path
            token = path.rsplit('/', 1)[-1]
            return token
        except (json.JSONDecodeError, AttributeError) as e:
            raise ValueError(f"Ошибка при парсинге тела письма: {e}")