"""Queue manager for QUERY_QUEUE operations.

All SQL targets Databricks dialect. Uses DatabricksClient.execute_sync()
for synchronous queue management queries.
"""

import json
from datetime import datetime, timezone
from typing import Any

from config.settings import qualified_table


_TABLE = qualified_table("query_queue")


def get_next_id(client) -> int:
    """Generate the next queue ID via MAX(ID) + 1."""
    rows = client.execute_sync(f"SELECT COALESCE(MAX(ID), 0) + 1 AS next_id FROM {_TABLE}")
    return int(rows[0]["next_id"])


def get_next_id_range(client, count: int) -> tuple[int, int]:
    """Reserve a contiguous range of IDs for bulk inserts.

    Returns (first_id, last_id).
    """
    rows = client.execute_sync(f"SELECT COALESCE(MAX(ID), 0) AS max_id FROM {_TABLE}")
    max_id = int(rows[0]["max_id"])
    first_id = max_id + 1
    last_id = max_id + count
    return first_id, last_id


def insert_queue_row(
    client,
    queue_id: int,
    username: str,
    search_content: dict,
    *,
    status: str = "QUEUED",
) -> None:
    """Insert a single row into QUERY_QUEUE."""
    sc_json = json.dumps(search_content).replace("'", "''")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
        INSERT INTO {_TABLE}
            (ID, USERNAME, REQUEST_DATE, SEARCH_CONTENT, STATUS)
        VALUES
            ({queue_id}, '{username}', '{now}', '{sc_json}', '{status}')
    """
    client.execute_sync(sql)


def update_queue_status(client, queue_id: int, status: str, **fields: Any) -> None:
    """Update a queue row's status and optional extra fields.

    Supported extra fields: statement_id, batch_statement_id, resolved_ref,
    result_count, started_date, completed_date, error_message.
    """
    sets = [f"STATUS = '{status}'"]

    str_fields = {"statement_id", "batch_statement_id", "resolved_ref", "error_message"}
    for key, value in fields.items():
        col = key.upper()
        if value is None:
            sets.append(f"{col} = NULL")
        elif key in str_fields:
            escaped = str(value).replace("'", "''")
            sets.append(f"{col} = '{escaped}'")
        elif key == "result_count":
            sets.append(f"{col} = {int(value)}")
        elif key in ("started_date", "completed_date"):
            sets.append(f"{col} = '{value}'")

    set_clause = ", ".join(sets)
    sql = f"UPDATE {_TABLE} SET {set_clause} WHERE ID = {queue_id}"
    client.execute_sync(sql)


def get_queue_rows(client, username: str | None = None, limit: int = 1000) -> list[dict]:
    """Fetch queue rows, optionally filtered by username."""
    where = f"WHERE USERNAME = '{username}'" if username else ""
    sql = f"SELECT * FROM {_TABLE} {where} ORDER BY REQUEST_DATE DESC LIMIT {limit}"
    return client.execute_sync(sql)


def reset_for_retry(client, queue_id: int) -> None:
    """Reset a FAILED or CANCELED queue row for retry."""
    sql = f"""
        UPDATE {_TABLE}
        SET STATUS = 'QUEUED',
            STATEMENT_ID = NULL,
            BATCH_STATEMENT_ID = NULL,
            STARTED_DATE = NULL,
            COMPLETED_DATE = NULL,
            ERROR_MESSAGE = NULL,
            RESULT_COUNT = NULL
        WHERE ID = {queue_id}
    """
    client.execute_sync(sql)


def get_queue_row(client, queue_id: int) -> dict | None:
    """Fetch a single queue row by ID."""
    rows = client.execute_sync(f"SELECT * FROM {_TABLE} WHERE ID = {queue_id}")
    return rows[0] if rows else None
