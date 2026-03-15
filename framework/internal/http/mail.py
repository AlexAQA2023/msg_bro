from operator import contains

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

        response =  self._client.get("/mail/mail/search", params=params)
        print(response.content)
        return response
