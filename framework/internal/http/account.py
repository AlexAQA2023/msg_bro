import httpx


class AccountApi:
    def __init__(self, base_url="http://185.185.143.231:8085") -> None:
        self._base_url = base_url
        self._client = httpx.Client(base_url=self._base_url)

    def register_user(self, login: str, password: str, email: str) -> httpx.Response:
        data = {"login": login,
                "password": password,
                "email": email}
        response = self._client.post("/register/user/async-register", json=data)
        print(response.content)
        return response

    def activate_user(self, token: str) -> httpx.Response:
        data = {"token": token}
        response = self._client.post("/activate/user/async-activate", json=data)
        print(response.content)
        return response
