"""Intercom target class."""

from __future__ import annotations

from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.target_sdk.target import TargetHotglue

from target_intercom.sinks import FallbackSink


class TargetIntercom(TargetHotglue):
    """Singer target for Intercom passthrough writes."""

    name = "target-intercom"
    SINK_TYPES = [FallbackSink]
    MAX_PARALLELISM = 1

    config_jsonschema = th.PropertiesList(
        th.Property("access_token", th.StringType, required=True),
        th.Property(
            "api_base_url",
            th.StringType,
            required=False,
            default="https://api.intercom.io",
        ),
        th.Property(
            "intercom_version",
            th.StringType,
            required=False,
            default="2.14",
        ),
        th.Property("user_agent", th.StringType, required=False),
    ).to_dict()

    def get_sink_class(self, stream_name: str):
        return FallbackSink


if __name__ == "__main__":
    TargetIntercom.cli()
