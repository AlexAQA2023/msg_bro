import json
from operator import contains
from urllib.parse import urlparse

import httpx


class MailApi:
    asd = '"https://account.broker.yandex.ru/api/v1"'

    def __init__(self, base_url="http://185.185.143.231:8085") -> None:
        self._base_url = base_url
        self._client = httpx.Client(base_url=self._base_url)

    def find_message(self, query: str) -> httpx.Response:
        params = {
            "query": query,
            "limit": 1,
            "kind": "containing",
            "start": 0, }

        response = self._client.get("/mail/mail/search", params=params)
        print(response.content)
        return response

    def search_messages(self, limit: int = 50, kind: str = "containing", start: int = 0) -> dict:

        params = {
            "limit": limit,
            "kind": kind,
            "start": start
        }
        response = self._client.get("/mail/mail/search", params=params)
        response.raise_for_status()
        return response.json()

    def find_message_by_text(self, search_text: str, limit: int = 50, kind: str = "containing",
                             start: int = 0) -> dict | None:

        data = self.search_messages(limit, kind, start)
        items = data.get("items", [])
        for message in items:
            body = message.get("Content", {}).get("Body", "")
            if search_text in body:
                return message
        return None

    def extract_token_from_message(self, message: dict) -> str | None:
        try:
            body = message.get("Content", {}).get("Body", "")
            body_json = json.loads(body)
            url = body_json.get("ConfirmationLinkUrl", "")
            path = urlparse(url).path
            token = path.rsplit("/", 1)[-1]
            return token
        except Exception:
            return None