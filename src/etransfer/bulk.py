"""Bulk e-Transfer search — CSV/Excel parsing, validation, and orchestration.

Reads a file from disk, validates each row, inserts valid rows into QUERY_QUEUE,
runs the unified query across the full ID range, stores results, and updates
each queue row with its individual result count.
"""

import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config.settings import qualified_table
from src.core import queue as queue_mgr
from src.etransfer.queries import (
    build_unified_query,
    resolve_fi_ref,
    store_results,
)

logger = logging.getLogger(__name__)

_CSV_COLUMN_MAP = {
    "Sender (x)": "Sender",
    "Recipient (x)": "Recipient",
    "Contact (x)": "Contact",
    "Payment Reference Number": "PaymentRefNum",
    "FI Reference Number": "FIRefNum",
    "Email Address": "EmailAddress",
    "Phone Number (eg 5555555555)": "PhoneNumber",
    "Account Number": "AccountNumber",
    "Name": "Name",
    "Amount": "Amount",
    "IP Address": "IPAddress",
    "From Date (YYYY-MM-DD)": "From",
    "To Date (YYYY-MM-DD)": "To",
}

_SEARCH_ROLES = {"Sender", "Recipient", "Contact"}
_SEARCH_CRITERIA = {"EmailAddress", "PhoneNumber", "AccountNumber", "Name", "Amount", "IPAddress"}
_INTERNAL_COLS = set(_CSV_COLUMN_MAP.values())


def parse_bulk_file(file_path: str | Path) -> tuple[list[dict], list[dict]]:
    """Read a CSV or Excel file and return (valid_rows, error_rows).

    Each item is a dict with the internal column names (matching SEARCH_CONTENT
    format) plus a ``_row`` key indicating the 1-based source row number.

    Error rows have an additional ``_error`` key describing the problem.
    """
    path = Path(file_path)
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, dtype=str)
    else:
        df = pd.read_excel(path, dtype=str)

    df.rename(columns=_CSV_COLUMN_MAP, inplace=True)
    extra = set(df.columns) - _INTERNAL_COLS
    df.drop(columns=list(extra), inplace=True, errors="ignore")

    df = df.where(pd.notnull(df), None)
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    valid: list[dict] = []
    errors: list[dict] = []

    for idx, raw in df.iterrows():
        row_num = int(idx) + 2  # 1-based, header is row 1
        row = {k: v for k, v in raw.to_dict().items() if v is not None and v != ""}

        error = _validate_row(row)
        if error:
            row["_row"] = row_num
            row["_error"] = error
            errors.append(row)
        else:
            sc = _to_search_content(row)
            sc["_row"] = row_num
            valid.append(sc)

    return valid, errors


def _validate_row(row: dict) -> str | None:
    """Return an error message string or None if the row is valid."""
    has_ref = bool(row.get("PaymentRefNum") or row.get("FIRefNum"))
    has_role = any(row.get(r) for r in _SEARCH_ROLES)
    has_criteria = any(row.get(c) for c in _SEARCH_CRITERIA)

    if not has_ref and not has_role:
        return "Missing search type (need PaymentRefNum, FIRefNum, or at least one of Sender/Recipient/Contact)"

    if not has_ref and not has_criteria:
        return "Missing search criteria (need at least one of Email, Phone, Account, Name, Amount, IP)"

    if not has_ref:
        from_date = row.get("From")
        to_date = row.get("To")
        if not from_date or not to_date:
            return "Missing From/To date"
        try:
            datetime.strptime(from_date, "%Y-%m-%d")
            datetime.strptime(to_date, "%Y-%m-%d")
        except ValueError:
            return f"Bad date format (expected YYYY-MM-DD, got From={from_date}, To={to_date})"

    return None


def _to_search_content(row: dict) -> dict:
    """Convert a validated row dict into SEARCH_CONTENT format."""
    if row.get("PaymentRefNum"):
        return {"PaymentRefNum": row["PaymentRefNum"]}

    if row.get("FIRefNum"):
        return {"FIRefNum": row["FIRefNum"]}

    sc: dict = {}
    sc["Search"] = [r for r in ("Sender", "Recipient", "Contact") if row.get(r)]
    for field in ("EmailAddress", "PhoneNumber", "AccountNumber", "Name", "Amount", "IPAddress"):
        if row.get(field):
            sc[field] = row[field]
    sc["From"] = row["From"]
    sc["To"] = row["To"]
    return sc


# ------------------------------------------------------------------
# Bulk search orchestration
# ------------------------------------------------------------------

def run_bulk_search(
    client,
    username: str,
    file_path: str | Path,
) -> dict:
    """End-to-end bulk search from a CSV/Excel file.

    Returns a summary dict:
        valid_count, error_count, first_id, last_id,
        total_results, per_queue_counts, errors
    """
    valid_rows, error_rows = parse_bulk_file(file_path)
    logger.info("Parsed %s: %d valid, %d errors", file_path, len(valid_rows), len(error_rows))

    if not valid_rows:
        return {
            "valid_count": 0,
            "error_count": len(error_rows),
            "first_id": None,
            "last_id": None,
            "total_results": 0,
            "per_queue_counts": {},
            "errors": error_rows,
        }

    # --- Reserve ID range and insert queue rows ---
    first_id, last_id = queue_mgr.get_next_id_range(client, len(valid_rows))
    logger.info("Reserved IDs %d–%d", first_id, last_id)

    queue_id_map: dict[int, dict] = {}  # queue_id → search_content
    for i, row in enumerate(valid_rows):
        qid = first_id + i
        sc = {k: v for k, v in row.items() if not k.startswith("_")}
        queue_mgr.insert_queue_row(client, qid, username, sc)
        queue_id_map[qid] = sc

    logger.info("Inserted %d queue rows", len(queue_id_map))

    # --- Resolve FI refs for any FIRefNum rows ---
    for qid, sc in queue_id_map.items():
        if "FIRefNum" not in sc:
            continue
        payment_ref = resolve_fi_ref(client, sc["FIRefNum"])
        if payment_ref is None:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            queue_mgr.update_queue_status(
                client, qid, "FAILED",
                error_message="FI reference number not found",
                completed_date=now,
            )
            logger.warning("Queue %d: FI ref %s not found", qid, sc["FIRefNum"])
        else:
            queue_mgr.update_queue_status(client, qid, "QUEUED", resolved_ref=payment_ref)
            logger.info("Queue %d: FI ref resolved to %s", qid, payment_ref)

    # Check if any rows are still QUEUED (not all failed during FI resolution)
    queued_rows = [
        qid for qid in queue_id_map
        if (r := queue_mgr.get_queue_row(client, qid)) and r.get("STATUS") == "QUEUED"
    ]

    if not queued_rows:
        logger.warning("All rows failed FI resolution — nothing to submit")
        return {
            "valid_count": len(valid_rows),
            "error_count": len(error_rows),
            "first_id": first_id,
            "last_id": last_id,
            "total_results": 0,
            "per_queue_counts": {},
            "errors": error_rows,
        }

    # --- Submit unified query ---
    sql = build_unified_query(first_id, last_id)
    statement_id = client.submit_async(sql)

    # Store batch statement_id on all QUEUED rows, keep status as QUEUED
    for qid in queued_rows:
        queue_mgr.update_queue_status(
            client, qid, "QUEUED",
            batch_statement_id=statement_id,
        )

    logger.info("Bulk query submitted as %s (IDs %d–%d, %d rows)", statement_id, first_id, last_id, len(queued_rows))

    # --- on_running: transition all rows to RUNNING ---
    def _on_running(_sid: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for qid in queued_rows:
            queue_mgr.update_queue_status(client, qid, "RUNNING", started_date=ts)
        logger.info("Bulk IDs %d–%d: Databricks confirmed RUNNING", first_id, last_id)

    # --- Poll ---
    try:
        final_state = client.poll_until_done(statement_id, on_running=_on_running)
    except Exception as exc:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for qid in queued_rows:
            queue_mgr.update_queue_status(
                client, qid, "FAILED",
                error_message=str(exc),
                completed_date=now,
            )
        raise

    # --- Process results ---
    per_queue_counts: dict[int, int] = {}

    if final_state == "SUCCEEDED":
        results = client.fetch_results_as_dicts(statement_id)
        total = store_results(client, results)
        logger.info("Bulk: %d total results stored", total)

        counts = Counter(int(r["QUEUE_ID"]) for r in results)

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for qid in queued_rows:
            cnt = counts.get(qid, 0)
            per_queue_counts[qid] = cnt
            queue_mgr.update_queue_status(
                client, qid, "SUCCEEDED",
                result_count=cnt,
                completed_date=now,
            )

    elif final_state == "CANCELED":
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for qid in queued_rows:
            queue_mgr.update_queue_status(client, qid, "CANCELED", completed_date=now)

    return {
        "valid_count": len(valid_rows),
        "error_count": len(error_rows),
        "first_id": first_id,
        "last_id": last_id,
        "total_results": sum(per_queue_counts.values()),
        "per_queue_counts": per_queue_counts,
        "errors": error_rows,
    }
