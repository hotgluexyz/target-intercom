"""REST client helpers for Intercom writes."""

from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
from hotglue_singer_sdk.target_sdk.client import HotglueSink


class IntercomSink(HotglueSink):
    """Base sink for Intercom passthrough write streams."""

    @property
    def name(self) -> str:
        return self.stream_name

    @property
    def base_url(self) -> str:
        return str(self.config["api_base_url"]).rstrip("/")

    @property
    def endpoint(self) -> str:
        return f"/{self.stream_name}"

    @property
    def http_headers(self) -> dict:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.config['access_token']}",
            "Intercom-Version": str(self.config["intercom_version"]),
        }
        user_agent = self.config.get("user_agent")
        if user_agent:
            headers["User-Agent"] = str(user_agent)
        return headers
    
    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record

    def validate_response(self, response) -> None:
        if response.status_code in (401, 403):
            raise InvalidCredentialsError(
                f"Invalid credentials/permissions for Intercom API: {response.text}"
            )
        if response.status_code in (400, 404, 409, 422):
            raise InvalidPayloadError(
                f"Invalid payload or missing Intercom entity: {response.text}"
            )

        super().validate_response(response)

