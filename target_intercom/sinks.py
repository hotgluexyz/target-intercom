"""Sink definitions for target-intercom."""

from __future__ import annotations

from target_intercom.client import IntercomSink


class FallbackSink(IntercomSink):
    
    @property
    def name(self) -> str:
        return self.stream_name
    
    @property
    def endpoint(self) -> str:
        return f"/{self.stream_name}"
