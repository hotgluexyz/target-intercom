"""REST client helpers for Intercom writes."""

import requests
from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
from hotglue_singer_sdk.target_sdk.client import HotglueSink

DEFAULT_API_BASE_URL = "https://api.intercom.io"
DEFAULT_INTERCOM_VERSION = "2.14"


class IntercomSink(HotglueSink):
    """Base sink for Intercom passthrough write streams."""

    endpoint = ""

    @property
    def base_url(self) -> str:
        return str(self.config.get("api_base_url", DEFAULT_API_BASE_URL)).rstrip("/")

    @property
    def http_headers(self) -> dict:
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
    
    def preprocess_record(self, record: dict, context: dict) -> dict:
        return dict(record)

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code in (401, 403):
            raise InvalidCredentialsError(
                f"Invalid credentials/permissions for Intercom API: {response.text}"
            )
        if response.status_code in (400, 404, 409, 422):
            raise InvalidPayloadError(
                f"Invalid payload or missing Intercom entity: {response.text}"
            )

        super().validate_response(response)

