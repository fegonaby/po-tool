"""Audit logging for AUDIT_LOG table.

All SQL targets Databricks dialect.
"""

import json
from datetime import datetime, timezone

from config.settings import qualified_table


_TABLE = qualified_table("audit_log")


def _get_next_id(client) -> int:
    rows = client.execute_sync(f"SELECT COALESCE(MAX(ID), 0) + 1 AS next_id FROM {_TABLE}")
    return int(rows[0]["next_id"])


def log_action(
    client,
    username: str,
    action: str,
    search_content: dict | str,
    queue_id: int | None = None,
) -> int:
    """Insert a row into AUDIT_LOG. Returns the generated audit ID."""
    audit_id = _get_next_id(client)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(search_content, dict):
        sc_json = json.dumps(search_content).replace("'", "''")
    else:
        sc_json = str(search_content).replace("'", "''")

    qid_sql = str(queue_id) if queue_id is not None else "NULL"

    sql = f"""
        INSERT INTO {_TABLE}
            (ID, USERNAME, TIMESTAMP, ACTION, SEARCH_CONTENT, QUEUE_ID)
        VALUES
            ({audit_id}, '{username}', '{now}', '{action}', '{sc_json}', {qid_sql})
    """
    client.execute_sync(sql)
    return audit_id
