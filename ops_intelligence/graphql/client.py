from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ops_intelligence.config import EntityConfig
from ops_intelligence.utils import deep_get


class GraphQLError(RuntimeError):
    pass


class GraphQLClient:
    def __init__(self, endpoint: str, api_key: str, timeout_seconds: int = 45) -> None:
        self.endpoint = endpoint
        self.timeout_seconds = timeout_seconds
        self._http = httpx.Client(
            timeout=timeout_seconds,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> GraphQLClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, GraphQLError)),
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        reraise=True,
    )
    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        response = self._http.post(self.endpoint, json={"query": query, "variables": variables})
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise GraphQLError(str(payload["errors"]))
        if "data" not in payload:
            raise GraphQLError("GraphQL response missing 'data'")
        return payload

    def fetch_all_pages(
        self,
        query: str,
        entity_config: EntityConfig,
        window_start: datetime,
        window_end: datetime,
        page_size: int,
        max_pages: int,
    ) -> list[dict[str, Any]]:
        after: str | None = None
        pages = 0
        rows: list[dict[str, Any]] = []

        while True:
            pages += 1
            if pages > max_pages:
                raise GraphQLError(
                    f"Pagination exceeded max pages ({max_pages}) for {entity_config.query_file}"
                )

            variables: dict[str, Any] = {
                entity_config.first_variable: page_size,
                entity_config.after_variable: after,
                entity_config.updated_after_variable: window_start.isoformat(),
                entity_config.updated_before_variable: window_end.isoformat(),
            }

            payload = self.execute(query=query, variables=variables)
            page_rows = deep_get(payload, entity_config.root_path)
            page_info = deep_get(payload, entity_config.page_info_path)

            if page_rows is None or page_info is None:
                raise GraphQLError(
                    "Unable to read expected root/pageInfo path. "
                    f"root_path={entity_config.root_path}, page_info_path={entity_config.page_info_path}"
                )
            if not isinstance(page_rows, list) or not isinstance(page_info, dict):
                raise GraphQLError("Invalid root/pageInfo payload types")

            rows.extend(page_rows)
            has_next = bool(page_info.get("hasNextPage"))
            after = page_info.get("endCursor")
            if not has_next:
                break

        return rows
