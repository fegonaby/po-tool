"""e-Transfer query builders and search orchestration — Databricks SQL dialect.

Contains the unified query, FI ref pre-resolution, download query builder,
and the full single-search flow.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import yaml

from config.settings import qualified_table
from src.core import queue as queue_mgr

logger = logging.getLogger(__name__)

_EMT = qualified_table("emt_transfers")
_FI_REF = qualified_table("payment_fi_reference_number")
_FRAUD = qualified_table("fraudulent_emt_transfers")
_SCAM = qualified_table("scammed_emt_transfers")
_FRAUD_CAT = qualified_table("fraud_categories")
_QUEUE = qualified_table("query_queue")
_RESULTS = qualified_table("query_results")

_FI_YAML = (
    Path(__file__).resolve().parent.parent.parent / "config" / "fi_institutions.yaml"
)
_fi_map: dict[str, str] | None = None


def _get_fi_map() -> dict[str, str]:
    global _fi_map
    if _fi_map is None:
        with open(_FI_YAML) as f:
            _fi_map = yaml.safe_load(f)
    return _fi_map


def fi_case_sql(column: str, alias: str) -> str:
    """Generate a SQL CASE expression mapping FI codes to readable names."""
    fi_map = _get_fi_map()
    whens = "\n".join(
        f"        WHEN {column} = '{code}' THEN '{name}'"
        for code, name in fi_map.items()
    )
    return f"""CASE
{whens}
        ELSE {column}
    END AS {alias}"""


# ------------------------------------------------------------------
# Unified query
# ------------------------------------------------------------------


def build_unified_query(first_id: int, last_id: int) -> str:
    """Build the full unified e-Transfer query."""
    sender_fi = fi_case_sql("t.SENDER_FI_ID", "SENDER_FI")
    recipient_fi = fi_case_sql("t.RECIPIENT_FI_ID", "RECIPIENT_FI")

    return f"""
SELECT q.ID AS QUEUE_ID,
       t.REFERENCE_NUMBER              AS PAYMENT_REF_NUMBER,
       CASE t.STATUS_CODE
           WHEN '3'  THEN 'Sent'
           WHEN '8'  THEN 'Cancelled'
           WHEN '10' THEN 'Completed'
           WHEN '11' THEN 'Offline'
           ELSE t.STATUS_CODE
       END AS STATUS_CODE,
       t.AMOUNT                        AS TRANSACTION_AMOUNT,
       t.REQUEST_TIMESTAMP             AS REQUEST_DATE,
       t.RECEIVED_TIMESTAMP            AS RECEIVED_DATE,
       t.SENDER_EMAIL_ADDRESS          AS SENDER_EMAIL,
       t.SENDER_PHONE_NUMBER           AS SENDER_PHONE,
       t.SENDER_NAME                   AS SENDER_NAME,
       CONCAT_WS(' ', t.SENDER_LEGAL_FIRST_NAME,
                      NULLIF(t.SENDER_LEGAL_MIDDLE_NAME, ''),
                      t.SENDER_LEGAL_LAST_NAME) AS SENDER_LEGAL_NAME,
       {sender_fi},
       t.SENDER_FI_USERID              AS SENDER_FI_USERID,
       t.SENDER_ACCOUNT_NUMBER         AS SENDER_ACCOUNT,
       t.SENDER_IP_ADDRESS             AS SENDER_IP,
       t.SENDER_MEMO                   AS SENDER_MEMO,
       t.RECIPIENT_EMAIL_ADDRESS       AS RECIPIENT_EMAIL,
       t.RECIPIENT_PHONE_NUMBER        AS RECIPIENT_PHONE,
       t.RECIPIENT_NAME                AS RECIPIENT_NAME,
       CONCAT_WS(' ', t.RECIPIENT_LEGAL_FIRST_NAME,
                      NULLIF(t.RECIPIENT_LEGAL_MIDDLE_NAME, ''),
                      t.RECIPIENT_LEGAL_LAST_NAME) AS RECIPIENT_LEGAL_NAME,
       {recipient_fi},
       t.RECIPIENT_FI_USERID           AS RECIPIENT_FI_USERID,
       t.RECIPIENT_ACCOUNT_NUMBER      AS RECIPIENT_ACCOUNT,
       t.RECIPIENT_IP_ADDRESS          AS RECIPIENT_IP,
       t.RECIPIENT_MEMO                AS RECIPIENT_MEMO,
       t.CONTACT_EMAIL_ADDRESS         AS CONTACT_EMAIL,
       t.CONTACT_PHONE_NUMBER          AS CONTACT_PHONE,
       t.CONTACT_NAME                  AS CONTACT_NAME,
       t.CONTACT_ACCOUNT_NUMBER        AS CONTACT_ACCOUNT,
       CASE t.CONTACT_NOTIFICATION_INDICATOR
           WHEN 0 THEN 'Email'
           WHEN 1 THEN 'Phone'
           WHEN 2 THEN 'Email and Phone'
           ELSE CAST(t.CONTACT_NOTIFICATION_INDICATOR AS STRING)
       END AS CONTACT_NOTIFICATION,
       t.CONTACT_QUESTION              AS SECURITY_QUESTION,
       CASE t.TRANSFER_TYPE_CODE
           WHEN 0 THEN 'QA'
           WHEN 1 THEN 'MR'
           WHEN 2 THEN 'AD'
           ELSE CAST(t.TRANSFER_TYPE_CODE AS STRING)
       END AS TRANSFER_TYPE,
       CASE WHEN fraud.TRANSFER_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS FRAUDULENT,
       fc.NAME AS SCAM_CATEGORY,
       fi_refs.SENDER_FI_REF,
       fi_refs.RECIPIENT_FI_REF
FROM {_EMT} t
LEFT JOIN {_FRAUD} fraud ON fraud.TRANSFER_ID = t.ID
LEFT JOIN {_SCAM} scam ON scam.TRANSFER_ID = t.ID
LEFT JOIN {_FRAUD_CAT} fc ON fc.ID = scam.FRAUD_CATEGORY_ID
LEFT JOIN (
    SELECT PAYMENT_REFERENCE_NUMBER,
           MAX(CASE WHEN rn = 1 THEN FI_REFERENCE_NUMBER END) AS SENDER_FI_REF,
           MAX(CASE WHEN rn = 2 THEN FI_REFERENCE_NUMBER END) AS RECIPIENT_FI_REF
    FROM (
        SELECT PAYMENT_REFERENCE_NUMBER, FI_REFERENCE_NUMBER,
               ROW_NUMBER() OVER (PARTITION BY PAYMENT_REFERENCE_NUMBER ORDER BY FI_FI_REF_SEQ) rn
        FROM {_FI_REF}
    )
    WHERE rn <= 2
    GROUP BY PAYMENT_REFERENCE_NUMBER
) fi_refs ON fi_refs.PAYMENT_REFERENCE_NUMBER = t.REFERENCE_NUMBER
JOIN {_QUEUE} q
  ON q.STATUS = 'QUEUED'
  AND q.ID BETWEEN {first_id} AND {last_id}
  AND t.REQUEST_TIMESTAMP BETWEEN
      TRY_CAST(q.SEARCH_CONTENT:From AS DATE)
      AND TRY_CAST(q.SEARCH_CONTENT:To AS DATE)
  AND (COALESCE(q.RESOLVED_REF, q.SEARCH_CONTENT:PaymentRefNum) IS NULL
       OR t.REFERENCE_NUMBER = COALESCE(q.RESOLVED_REF, q.SEARCH_CONTENT:PaymentRefNum))
  AND (q.SEARCH_CONTENT:EmailAddress IS NULL OR (
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Sender')    AND t.SENDER_EMAIL_ADDRESS     = q.SEARCH_CONTENT:EmailAddress) OR
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Recipient') AND t.RECIPIENT_EMAIL_ADDRESS  = q.SEARCH_CONTENT:EmailAddress) OR
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Contact')   AND t.CONTACT_EMAIL_ADDRESS    = q.SEARCH_CONTENT:EmailAddress)
  ))
  AND (q.SEARCH_CONTENT:PhoneNumber IS NULL OR (
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Sender')    AND t.SENDER_PHONE_NUMBER      = q.SEARCH_CONTENT:PhoneNumber) OR
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Recipient') AND t.RECIPIENT_PHONE_NUMBER   = q.SEARCH_CONTENT:PhoneNumber) OR
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Contact')   AND t.CONTACT_PHONE_NUMBER     = q.SEARCH_CONTENT:PhoneNumber)
  ))
  AND (q.SEARCH_CONTENT:AccountNumber IS NULL OR (
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Sender')    AND t.SENDER_ACCOUNT_NUMBER    = q.SEARCH_CONTENT:AccountNumber) OR
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Recipient') AND t.RECIPIENT_ACCOUNT_NUMBER = q.SEARCH_CONTENT:AccountNumber)
  ))
  AND (q.SEARCH_CONTENT:Name IS NULL OR (
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Sender')    AND LOWER(t.SENDER_NAME)    = LOWER(q.SEARCH_CONTENT:Name)) OR
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Recipient') AND LOWER(t.RECIPIENT_NAME) = LOWER(q.SEARCH_CONTENT:Name)) OR
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Contact')   AND LOWER(t.CONTACT_NAME)   = LOWER(q.SEARCH_CONTENT:Name))
  ))
  AND (q.SEARCH_CONTENT:Amount IS NULL OR t.AMOUNT = TRY_CAST(q.SEARCH_CONTENT:Amount AS DECIMAL))
  AND (q.SEARCH_CONTENT:IPAddress IS NULL OR (
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Sender')    AND t.SENDER_IP_ADDRESS    = q.SEARCH_CONTENT:IPAddress) OR
    (array_contains(from_json(q.SEARCH_CONTENT:Search, 'ARRAY<STRING>'), 'Recipient') AND t.RECIPIENT_IP_ADDRESS = q.SEARCH_CONTENT:IPAddress)
  ))
"""


# ------------------------------------------------------------------
# FI reference pre-resolution
# ------------------------------------------------------------------


def resolve_fi_ref(client, fi_ref_number: str) -> str | None:
    """Look up FI reference number to get payment reference number.

    Returns the payment ref or None if not found.
    Synchronous — uses INLINE disposition.
    """
    escaped = fi_ref_number.replace("'", "''")
    sql = f"""
        SELECT PAYMENT_REFERENCE_NUMBER
        FROM {_FI_REF}
        WHERE FI_REFERENCE_NUMBER = '{escaped}'
        LIMIT 1
    """
    rows = client.execute_sync(sql)
    if rows:
        return rows[0]["PAYMENT_REFERENCE_NUMBER"]
    return None


# ------------------------------------------------------------------
# Download / export query
# ------------------------------------------------------------------

ALWAYS_FIELDS = [
    "PAYMENT_REF_NUMBER",
    "STATUS_CODE",
    "TRANSACTION_AMOUNT",
    "REQUEST_DATE",
    "RECEIVED_DATE",
]

TOKENIZED_FIELDS = {
    "SENDER_EMAIL",
    "SENDER_PHONE",
    "SENDER_NAME",
    "SENDER_LEGAL_NAME",
    "SENDER_FI_USERID",
    "SENDER_ACCOUNT",
    "RECIPIENT_EMAIL",
    "RECIPIENT_PHONE",
    "RECIPIENT_NAME",
    "RECIPIENT_LEGAL_NAME",
    "RECIPIENT_FI_USERID",
    "RECIPIENT_ACCOUNT",
    "CONTACT_EMAIL",
    "CONTACT_PHONE",
    "CONTACT_NAME",
    "CONTACT_ACCOUNT",
}


def build_download_sql(
    queue_id_clause: str, selected_fields: list[str] | None = None
) -> str:
    """Build download query with dynamic field selection.

    No detokenize() for now — mock data is cleartext.
    When tokenized data is available, wrap TOKENIZED_FIELDS with detokenize().
    """
    always = ", ".join(ALWAYS_FIELDS)

    if selected_fields:
        cols = []
        for f in selected_fields:
            cols.append(f)
        optional = ", " + ", ".join(cols)
    else:
        optional = ""

    return f"SELECT {always}{optional} FROM {_RESULTS} WHERE {queue_id_clause} ORDER BY REQUEST_DATE"


# ------------------------------------------------------------------
# Results storage
# ------------------------------------------------------------------

_RESULT_COLUMNS = [
    "QUEUE_ID",
    "PAYMENT_REF_NUMBER",
    "STATUS_CODE",
    "TRANSACTION_AMOUNT",
    "REQUEST_DATE",
    "RECEIVED_DATE",
    "SENDER_EMAIL",
    "SENDER_PHONE",
    "SENDER_NAME",
    "SENDER_LEGAL_NAME",
    "SENDER_FI",
    "SENDER_FI_USERID",
    "SENDER_ACCOUNT",
    "SENDER_FI_REF",
    "SENDER_IP",
    "SENDER_MEMO",
    "RECIPIENT_EMAIL",
    "RECIPIENT_PHONE",
    "RECIPIENT_NAME",
    "RECIPIENT_LEGAL_NAME",
    "RECIPIENT_FI",
    "RECIPIENT_FI_USERID",
    "RECIPIENT_ACCOUNT",
    "RECIPIENT_FI_REF",
    "RECIPIENT_IP",
    "RECIPIENT_MEMO",
    "CONTACT_EMAIL",
    "CONTACT_PHONE",
    "CONTACT_NAME",
    "CONTACT_ACCOUNT",
    "CONTACT_NOTIFICATION",
    "SECURITY_QUESTION",
    "TRANSFER_TYPE",
    "FRAUDULENT",
    "SCAM_CATEGORY",
]


def _sql_value(val) -> str:
    if val is None:
        return "NULL"
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"


def store_results(client, rows: list[dict]) -> int:
    """INSERT result rows into QUERY_RESULTS. Returns the number of rows inserted."""
    if not rows:
        return 0

    col_list = ", ".join(_RESULT_COLUMNS)
    value_rows = []
    for row in rows:
        vals = ", ".join(_sql_value(row.get(col)) for col in _RESULT_COLUMNS)
        value_rows.append(f"({vals})")

    batch_size = 500
    total = 0
    for i in range(0, len(value_rows), batch_size):
        batch = value_rows[i : i + batch_size]
        sql = f"INSERT INTO {_RESULTS} ({col_list}) VALUES {', '.join(batch)}"
        client.execute_sync(sql)
        total += len(batch)

    return total


# ------------------------------------------------------------------
# Single search orchestration
# ------------------------------------------------------------------


def run_single_search(client, username: str, search_content: dict) -> int:
    """Execute the full single-search flow.

    1. Insert queue row
    2. Resolve FI ref if needed
    3. Submit unified query (async)
    4. Poll until done
    5. Fetch results and store in QUERY_RESULTS
    6. Update queue row

    Returns the queue_id.
    """
    queue_id = queue_mgr.get_next_id(client)
    queue_mgr.insert_queue_row(client, queue_id, username, search_content)
    logger.info("Queue row %d inserted", queue_id)

    if "FIRefNum" in search_content:
        payment_ref = resolve_fi_ref(client, search_content["FIRefNum"])
        if payment_ref is None:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            queue_mgr.update_queue_status(
                client,
                queue_id,
                "FAILED",
                error_message="FI reference number not found",
                completed_date=now,
            )
            logger.warning("Queue %d: FI ref not found", queue_id)
            return queue_id
        queue_mgr.update_queue_status(
            client, queue_id, "QUEUED", resolved_ref=payment_ref
        )
        logger.info("Queue %d: FI ref resolved to %s", queue_id, payment_ref)

    sql = build_unified_query(queue_id, queue_id)
    statement_id = client.submit_async(sql)

    # Store statement_id but keep STATUS as QUEUED.  The unified query
    # JOINs on q.STATUS = 'QUEUED', so the row must stay QUEUED until
    # Databricks has actually started executing (and already read the
    # queue table).  The on_running callback handles the transition.
    queue_mgr.update_queue_status(
        client,
        queue_id,
        "QUEUED",
        statement_id=statement_id,
    )
    logger.info(
        "Queue %d: submitted as %s (staying QUEUED until RUNNING)",
        queue_id,
        statement_id,
    )

    def _on_running(_sid: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        queue_mgr.update_queue_status(
            client,
            queue_id,
            "RUNNING",
            started_date=ts,
        )
        logger.info("Queue %d: Databricks confirmed RUNNING", queue_id)

    try:
        final_state = client.poll_until_done(statement_id, on_running=_on_running)
    except Exception as exc:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        queue_mgr.update_queue_status(
            client,
            queue_id,
            "FAILED",
            error_message=str(exc),
            completed_date=now,
        )
        raise

    if final_state == "SUCCEEDED":
        results = client.fetch_results_as_dicts(statement_id)
        count = store_results(client, results)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        queue_mgr.update_queue_status(
            client,
            queue_id,
            "SUCCEEDED",
            result_count=count,
            completed_date=now,
        )
        logger.info("Queue %d: %d results stored", queue_id, count)
    elif final_state == "CANCELED":
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        queue_mgr.update_queue_status(
            client,
            queue_id,
            "CANCELED",
            completed_date=now,
        )

    return queue_id
