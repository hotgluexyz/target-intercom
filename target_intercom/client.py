"""REST client helpers for Intercom writes."""

from __future__ import annotations

from typing import Any

import requests
from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.target_sdk.client import HotglueSink

DEFAULT_API_BASE_URL = "https://api.intercom.io"
DEFAULT_INTERCOM_VERSION = "2.14"


class IntercomSink(HotglueSink):
    """Base sink for Intercom passthrough write streams."""

    endpoint = ""

    @property
    def url_base(self) -> str:
        return str(self.config.get("api_base_url", DEFAULT_API_BASE_URL)).rstrip("/")

    @property
    def http_headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.config['access_token']}",
            "Intercom-Version": str(
                self.config.get("intercom_version", DEFAULT_INTERCOM_VERSION)
            ),
        }
        user_agent = self.config.get("user_agent")
        if user_agent:
            headers["User-Agent"] = str(user_agent)
        return headers

    def url(self, endpoint: str | None = None) -> str:
        path = self.endpoint if endpoint is None else endpoint
        return f"{self.url_base}/{str(path).lstrip('/')}"

    def request_api(
        self,
        http_method: str,
        endpoint: str | None = None,
        params: dict[str, Any] | None = None,
        request_data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        raise_on_status: bool = True,
    ) -> requests.Response:
        request_headers = self.http_headers
        if headers:
            request_headers.update(headers)
        response = requests.request(
            method=http_method,
            url=self.url(endpoint),
            params=params,
            headers=request_headers,
            json=request_data,
            timeout=30,
        )
        if raise_on_status:
            self.validate_response(response)
        return response

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == 429 or 500 <= response.status_code < 600:
            raise RetriableAPIError(self.response_error_message(response))
        if response.status_code in (401, 403):
            raise InvalidCredentialsError(
                f"Invalid credentials/permissions for Intercom API: {response.text}"
            )
        if response.status_code in (400, 404, 409, 422):
            raise InvalidPayloadError(
                f"Invalid payload or missing Intercom entity: {response.text}"
            )
        if 400 <= response.status_code < 500:
            raise FatalAPIError(self.response_error_message(response))

    def response_error_message(self, response: requests.Response) -> str:
        error_type = "Client" if 400 <= response.status_code < 500 else "Server"
        return (
            f"{response.status_code} {error_type} Error for path '{self.endpoint}'. "
            f"Response: {response.text}"
        )

