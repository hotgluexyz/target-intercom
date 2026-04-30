"""Sink definitions for target-intercom."""

from __future__ import annotations

from typing import Any

from hotglue_etl_exceptions import InvalidPayloadError

from target_intercom.client import IntercomSink


class IntercomCompaniesSink(IntercomSink):
    """Write `companies` stream records to Intercom `POST /companies`."""

    name = "companies"
    endpoint = "/companies"
    tags_endpoint = "/tags"

    def preprocess_record(
        self,
        record: dict[str, Any],
        context: dict | None,
    ) -> dict[str, Any]:
        del context
        intercom_id = record.get("id")
        company_id = record.get("company_id")
        name = record.get("name")
        if (
            (intercom_id is None or str(intercom_id).strip() == "")
            and (company_id is None or str(company_id).strip() == "")
            and (name is None or str(name).strip() == "")
        ):
            raise InvalidPayloadError(
                "Invalid payload: companies record must include non-empty `id`, `company_id`, or `name`."
            )
        return dict(record)

    def _get_company_by_intercom_id(self, intercom_id: str) -> dict[str, Any] | None:
        response = self.request_api(
            "GET",
            endpoint=f"/companies/{intercom_id}",
            raise_on_status=False,
        )
        if response.status_code == 404:
            return None
        self.validate_response(response)
        body = response.json()
        return body or None

    def _search_companies(self, query: dict[str, str]) -> dict[str, Any] | None:
        response = self.request_api(
            "GET",
            endpoint=self.endpoint,
            params=query,
            raise_on_status=False,
        )
        if response.status_code == 404:
            return None
        self.validate_response(response)
        body = response.json()
        if body.get("type") == "company":
            return body
        candidates = body.get("data")
        if isinstance(candidates, list):
            if not candidates:
                return None
            if len(candidates) > 1:
                raise InvalidPayloadError(
                    f"Invalid payload: multiple Intercom companies matched lookup {query}."
                )
            match = candidates[0]
            return match if isinstance(match, dict) else None
        return None

    def _resolve_company(self, record: dict[str, Any]) -> dict[str, Any] | None:
        intercom_id = record.get("id")
        if intercom_id is not None and str(intercom_id).strip() != "":
            match = self._get_company_by_intercom_id(str(intercom_id))
            if match is not None:
                return match

        company_id = record.get("company_id")
        if company_id is not None and str(company_id).strip() != "":
            match = self._search_companies({"company_id": str(company_id)})
            if match is not None:
                return match

        name = record.get("name")
        if name is not None and str(name).strip() != "":
            return self._search_companies({"name": str(name)})

        return None

    def _normalize_tags_input(self, raw_tags: Any) -> list[dict[str, str]]:
        if raw_tags is None:
            return []
        if not isinstance(raw_tags, list):
            raise InvalidPayloadError(
                "Invalid payload: `tags` must be an array of strings or objects."
            )

        normalized: list[dict[str, str]] = []
        for item in raw_tags:
            if isinstance(item, str):
                name = item.strip()
                if not name:
                    raise InvalidPayloadError(
                        "Invalid payload: tag names in `tags` cannot be empty."
                    )
                normalized.append({"name": name})
                continue

            if not isinstance(item, dict):
                raise InvalidPayloadError(
                    "Invalid payload: each `tags` entry must be a string or object."
                )

            tag_id = str(item.get("id", "")).strip()
            name = str(item.get("name", "")).strip()
            if not tag_id and not name:
                raise InvalidPayloadError(
                    "Invalid payload: tag object must include non-empty `id` or `name`."
                )
            normalized_tag: dict[str, str] = {}
            if tag_id:
                normalized_tag["id"] = tag_id
            if name:
                normalized_tag["name"] = name
            normalized.append(normalized_tag)

        return normalized

    def _get_tag_by_id(self, tag_id: str) -> dict[str, Any] | None:
        response = self.request_api(
            "GET",
            endpoint=f"{self.tags_endpoint}/{tag_id}",
            raise_on_status=False,
        )
        if response.status_code == 404:
            return None
        self.validate_response(response)
        body: dict[str, Any] = response.json() if response.text else {}
        return body or None

    def _resolve_desired_company_tags(
        self, normalized_tags: list[dict[str, str]]
    ) -> dict[str, str]:
        resolved: dict[str, str] = {}
        for tag in normalized_tags:
            tag_id = tag.get("id")
            tag_name = tag.get("name")
            if tag_id:
                tag_body = self._get_tag_by_id(tag_id)
                if not tag_body:
                    raise InvalidPayloadError(
                        f"Invalid payload: tag id `{tag_id}` was not found in Intercom."
                    )
                resolved_name = str(tag_body.get("name", "")).strip()
                if not resolved_name:
                    raise InvalidPayloadError(
                        f"Invalid payload: tag id `{tag_id}` has no name in Intercom."
                    )
                if tag_name and tag_name != resolved_name:
                    raise InvalidPayloadError(
                        f"Invalid payload: tag id `{tag_id}` does not match tag name `{tag_name}`."
                    )
                resolved[tag_id] = resolved_name
                continue

            create_response = self.request_api(
                "POST",
                endpoint=self.tags_endpoint,
                request_data={"name": tag_name},
            )
            create_body: dict[str, Any] = (
                create_response.json() if create_response.text else {}
            )
            resolved_id = str(create_body.get("id", "")).strip()
            resolved_name = str(create_body.get("name", "")).strip()
            if not resolved_id or not resolved_name:
                raise InvalidPayloadError(
                    "Invalid payload: Intercom tag creation did not return tag id and name."
                )
            resolved[resolved_id] = resolved_name

        return resolved

    def _extract_current_company_tags(self, company_body: dict[str, Any]) -> dict[str, str]:
        tags_container = company_body.get("tags")
        if not isinstance(tags_container, dict):
            return {}
        tags_list = tags_container.get("tags")
        if not isinstance(tags_list, list):
            return {}

        current: dict[str, str] = {}
        for tag in tags_list:
            if not isinstance(tag, dict):
                continue
            tag_id = str(tag.get("id", "")).strip()
            tag_name = str(tag.get("name", "")).strip()
            if tag_id and tag_name:
                current[tag_id] = tag_name
        return current

    def _sync_company_tags(
        self,
        company_intercom_id: str,
        desired_tags: dict[str, str],
        current_tags: dict[str, str],
    ) -> None:
        desired_ids = set(desired_tags.keys())
        current_ids = set(current_tags.keys())

        for tag_id in sorted(desired_ids - current_ids):
            self.request_api(
                "POST",
                endpoint=self.tags_endpoint,
                request_data={
                    "name": desired_tags[tag_id],
                    "companies": [{"id": company_intercom_id}],
                },
            )

        for tag_id in sorted(current_ids - desired_ids):
            self.request_api(
                "POST",
                endpoint=self.tags_endpoint,
                request_data={
                    "name": current_tags[tag_id],
                    "companies": [{"id": company_intercom_id, "untag": True}],
                },
            )

    def upsert_record(
        self,
        record: dict[str, Any],
        context: dict | None,
    ) -> tuple[str, bool, dict[str, Any]]:
        del context
        tags_were_provided = "tags" in record
        normalized_tags = self._normalize_tags_input(record.get("tags"))
        resolved = self._resolve_company(record)
        request_payload = dict(record)
        request_payload.pop("tags", None)
        state_updates: dict[str, Any] = {}
        existing_company_id = (
            str(resolved.get("company_id"))
            if resolved and resolved.get("company_id") is not None
            else None
        )
        request_company_id = (
            str(record.get("company_id"))
            if record.get("company_id") is not None
            else None
        )
        if (
            existing_company_id
            and request_company_id
            and existing_company_id != request_company_id
        ):
            raise InvalidPayloadError(
                "Invalid payload: `company_id` does not match resolved Intercom company."
            )

        if resolved and resolved.get("id"):
            intercom_id = str(resolved["id"])
            request_payload.pop("id", None)
            response = self.request_api(
                "PUT",
                endpoint=f"/companies/{intercom_id}",
                request_data=request_payload,
            )
            state_updates["is_updated"] = True
        else:
            response = self.request_api(
                "POST",
                endpoint=self.endpoint,
                request_data=request_payload,
            )
        body = response.json()
        if tags_were_provided:
            company_intercom_id = str(body.get("id", "")).strip()
            if not company_intercom_id:
                raise InvalidPayloadError(
                    "Invalid payload: Intercom company id is required to sync tags."
                )
            desired_tags = self._resolve_desired_company_tags(normalized_tags)
            current_tags = self._extract_current_company_tags(body)
            self._sync_company_tags(
                company_intercom_id=company_intercom_id,
                desired_tags=desired_tags,
                current_tags=current_tags,
            )

        state_updates["success"] = True
        record_id = body.get("id") or record.get("id") or record.get("company_id")

        if "id" in body:
            state_updates["id"] = body["id"]
        if "company_id" in body:
            state_updates["company_id"] = body["company_id"]
        else:
            state_updates["company_id"] = record.get("company_id")
        if "type" in body:
            state_updates["type"] = body["type"]

        return str(record_id), True, state_updates


class IntercomTagsSink(IntercomSink):
    """Write `tags` stream records to Intercom `POST /tags` passthrough."""

    name = "tags"
    endpoint = "/tags"

    def preprocess_record(
        self,
        record: dict[str, Any],
        context: dict | None,
    ) -> dict[str, Any]:
        del context
        return dict(record)

    def upsert_record(
        self,
        record: dict[str, Any],
        context: dict | None,
    ) -> tuple[str, bool, dict[str, Any]]:
        del context
        response = self.request_api("POST", endpoint=self.endpoint, request_data=record)
        body= response.json()
        state_updates = {"success": True}
        record_id = body.get("id") or record.get("id") or record.get("name") or ""

        if "id" in body:
            state_updates["id"] = body["id"]
        if "name" in body:
            state_updates["name"] = body["name"]
        elif "name" in record:
            state_updates["name"] = record["name"]
        if "type" in body:
            state_updates["type"] = body["type"]

        return str(record_id), True, state_updates

class FallbackSink(IntercomSink):
    """Fallback sink for streams that are not supported by the target."""
    @property
    def name(self) -> str:
        return self.stream_name
    @property
    def endpoint(self) -> str:
        return f"/{self.stream_name}"
    def preprocess_record(self, record: dict, context: dict | None) -> dict:
        return dict(record)