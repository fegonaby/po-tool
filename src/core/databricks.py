"""Databricks Statement Execution API client.

Wraps the REST API for submitting SQL, polling status, cancelling,
and fetching paginated INLINE result chunks.
"""

import logging
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

API_PATH = "/api/2.0/sql/statements"


class DatabricksError(Exception):
    """Raised when the Databricks API returns an error."""

    def __init__(self, message: str, state: str | None = None, statement_id: str | None = None):
        self.state = state
        self.statement_id = statement_id
        super().__init__(message)


class DatabricksClient:
    """Thin wrapper around the Databricks SQL Statement Execution API."""

    def __init__(self, host: str, warehouse_id: str, token: str):
        self.host = host.rstrip("/")
        self.warehouse_id = warehouse_id
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    @property
    def _url(self) -> str:
        return f"{self.host}{API_PATH}"

    # ------------------------------------------------------------------
    # Low-level API methods
    # ------------------------------------------------------------------

    def submit_statement(
        self,
        sql: str,
        *,
        wait_timeout: str = "0s",
        disposition: str = "INLINE",
        fmt: str = "JSON_ARRAY",
    ) -> dict:
        """POST /sql/statements — submit a SQL statement.

        Returns the full response dict (contains statement_id, status, etc.).
        """
        payload = {
            "warehouse_id": self.warehouse_id,
            "statement": sql,
            "wait_timeout": wait_timeout,
            "disposition": disposition,
            "format": fmt,
        }
        resp = self._session.post(self._url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        self._check_error(data)
        return data

    def poll_status(self, statement_id: str) -> dict:
        """GET /sql/statements/{id} — check execution status.

        Returns {"state": ..., "error": ...} from the status block.
        """
        resp = self._session.get(f"{self._url}/{statement_id}")
        resp.raise_for_status()
        data = resp.json()
        return data.get("status", {})

    def cancel_statement(self, statement_id: str) -> None:
        """POST /sql/statements/{id}/cancel."""
        resp = self._session.post(f"{self._url}/{statement_id}/cancel")
        resp.raise_for_status()

    def fetch_all_chunks(self, statement_id: str) -> tuple[list[str], list[list]]:
        """Fetch the full INLINE result set for a SUCCEEDED statement.

        Follows ``next_chunk_internal_link`` for paginated results.
        Returns (column_names, rows) where rows is a list of lists.
        """
        resp = self._session.get(f"{self._url}/{statement_id}")
        resp.raise_for_status()
        data = resp.json()

        columns = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
        result = data.get("result", {})
        all_rows: list[list] = list(result.get("data_array", []))

        while "next_chunk_internal_link" in result:
            next_url = f"{self.host}{result['next_chunk_internal_link']}"
            resp = self._session.get(next_url)
            resp.raise_for_status()
            result = resp.json()
            all_rows.extend(result.get("data_array", []))

        return columns, all_rows

    # ------------------------------------------------------------------
    # High-level convenience methods
    # ------------------------------------------------------------------

    def execute_sync(self, sql: str) -> list[dict]:
        """Submit with INLINE + wait_timeout=30s and return rows as dicts.

        For small, bounded result sets (lookups, DML, queue queries).
        """
        data = self.submit_statement(
            sql,
            wait_timeout="30s",
            disposition="INLINE",
        )
        state = data.get("status", {}).get("state", "")

        if state == "SUCCEEDED":
            return self._parse_inline(data)

        statement_id = data.get("statement_id", "")
        if state in ("PENDING", "RUNNING"):
            return self._poll_until_done_inline(statement_id)

        error_msg = data.get("status", {}).get("error", {}).get("message", "Unknown error")
        raise DatabricksError(error_msg, state=state, statement_id=statement_id)

    def submit_async(self, sql: str) -> str:
        """Submit with INLINE + wait_timeout=0s, return statement_id.

        For long-running queries (e-Transfer search, bulk, debit transactions).
        Results are fetched via fetch_all_chunks after polling completes.
        """
        data = self.submit_statement(
            sql,
            wait_timeout="0s",
            disposition="INLINE",
        )
        return data["statement_id"]

    def poll_until_done(
        self,
        statement_id: str,
        interval: float = 5.0,
        timeout: float = 600.0,
        on_running: "callable | None" = None,
    ) -> str:
        """Poll until the statement reaches a terminal state. Returns final state.

        If *on_running* is provided, it is called exactly once when the
        statement first transitions to RUNNING.  This lets callers defer
        queue-status updates until Databricks has actually started executing
        (important for cold-start warehouses where PENDING can last minutes).
        """
        elapsed = 0.0
        running_fired = False
        while elapsed < timeout:
            status = self.poll_status(statement_id)
            state = status.get("state", "")
            if state == "RUNNING" and not running_fired:
                running_fired = True
                if on_running:
                    on_running(statement_id)
            if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
                if state == "FAILED":
                    msg = status.get("error", {}).get("message", "Unknown error")
                    raise DatabricksError(msg, state=state, statement_id=statement_id)
                return state
            time.sleep(interval)
            elapsed += interval
        raise DatabricksError(f"Timed out after {timeout}s", statement_id=statement_id)

    def fetch_results_as_dicts(self, statement_id: str) -> list[dict]:
        """Fetch INLINE results (with pagination) and return as list of dicts."""
        columns, rows = self.fetch_all_chunks(statement_id)
        return [dict(zip(columns, row)) for row in rows]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_inline(self, data: dict) -> list[dict]:
        """Parse INLINE result data into a list of dicts."""
        columns = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
        chunks = data.get("result", {}).get("data_array", [])
        return [dict(zip(columns, row)) for row in chunks]

    def _poll_until_done_inline(self, statement_id: str) -> list[dict]:
        """Fallback: poll then parse INLINE result when wait_timeout was exceeded."""
        self.poll_until_done(statement_id)
        resp = self._session.get(f"{self._url}/{statement_id}")
        resp.raise_for_status()
        return self._parse_inline(resp.json())

    @staticmethod
    def _check_error(data: dict) -> None:
        status = data.get("status", {})
        state = status.get("state", "")
        if state == "FAILED":
            error = status.get("error", {})
            raise DatabricksError(
                error.get("message", "Statement failed"),
                state=state,
                statement_id=data.get("statement_id"),
            )
