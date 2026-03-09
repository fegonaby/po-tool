"""Debit query builders — Databricks SQL dialect.

All three steps are synchronous. No QUERY_QUEUE or QUERY_RESULTS involvement.
"""

from config.settings import qualified_table


_TERMINALS = qualified_table("idp_mtd_terminals")
_ACQUIRERS = qualified_table("idp_acquirers")
_TRANSACTIONS = qualified_table("idp_mtd_transactions")
_ISSUERS = qualified_table("issuers")
_FPAN_DPAN = qualified_table("tsp_fpan_dpan_history")


def build_terminal_query(terminal_id: str) -> str:
    """Lookup a single terminal by ID. INLINE disposition."""
    escaped = terminal_id.replace("'", "''")
    return f"""
        SELECT t.terminal_id, t.merchant_number, t.name, t.address, t.city,
               t.province, t.postal_code, t.first_transaction_date,
               t.last_transaction_date, a.name AS acquirer
        FROM {_TERMINALS} t
        JOIN {_ACQUIRERS} a ON t.acquirer_number = a.acquirer_number
        WHERE t.terminal_id = '{escaped}'
    """


def build_merchant_query(
    *,
    merchant_number: str | None = None,
    merchant_name: str | None = None,
    address: str | None = None,
    city: str | None = None,
    postal_code: str | None = None,
) -> str:
    """Search merchants with optional AND-combined filters. INLINE disposition."""
    conditions = ["1=1"]

    if merchant_number:
        conditions.append(f"t.merchant_number = '{merchant_number.replace(chr(39), chr(39)*2)}'")
    if merchant_name:
        escaped = merchant_name.replace("'", "''").upper()
        conditions.append(f"UPPER(t.name) LIKE '%{escaped}%'")
    if address:
        escaped = address.replace("'", "''").upper()
        conditions.append(f"UPPER(t.address) LIKE '%{escaped}%'")
    if city:
        escaped = city.replace("'", "''").upper()
        conditions.append(f"UPPER(t.city) LIKE '%{escaped}%'")
    if postal_code:
        escaped = postal_code.replace("'", "''").upper()
        conditions.append(f"UPPER(t.postal_code) LIKE '%{escaped}%'")

    where = " AND ".join(conditions)
    return f"""
        SELECT t.terminal_id, t.merchant_number, t.name, t.address, t.city,
               t.province, t.postal_code, t.first_transaction_date,
               t.last_transaction_date, a.name AS acquirer
        FROM {_TERMINALS} t
        JOIN {_ACQUIRERS} a ON t.acquirer_number = a.acquirer_number
        WHERE {where}
        LIMIT 1000
    """


def build_transaction_query(
    terminals: list[tuple[str, str]],
    from_date: str,
    to_date: str,
    *,
    amount: float | None = None,
    pan: str | None = None,
) -> str:
    """Transaction search for selected terminals. EXTERNAL_LINKS disposition.

    Args:
        terminals: List of (terminal_id, merchant_number) tuples.
        from_date: Start date YYYY-MM-DD.
        to_date: End date YYYY-MM-DD.
        amount: Optional exact amount filter.
        pan: Optional PAN filter (supports LIKE patterns).
    """
    values_rows = ", ".join(
        f"('{tid.replace(chr(39), chr(39)*2)}', '{mid.replace(chr(39), chr(39)*2)}')"
        for tid, mid in terminals
    )

    extra_conditions = ""
    if amount is not None:
        extra_conditions += f"\n  AND t.amount = {amount}"
    if pan:
        escaped = pan.replace("'", "''")
        extra_conditions += f"\n  AND t.pan LIKE '{escaped}'"

    return f"""
        SELECT i.name AS issuer_name,
               t.pan,
               t.terminal_id,
               t.local_transaction_date,
               t.amount,
               (SELECT h.fpan
                FROM {_FPAN_DPAN} h
                WHERE h.dpan = t.pan
                  AND t.local_transaction_date BETWEEN h.min_tkm_operation_timestamp
                                                   AND h.max_tkm_operation_timestamp
                LIMIT 1) AS fpan
        FROM {_TRANSACTIONS} t
        JOIN {_ISSUERS} i ON t.issuer_id = i.issuer_id
        WHERE (t.terminal_id, t.merchant_number) IN ({values_rows})
          AND t.header_capture_date BETWEEN '{from_date}' AND '{to_date}'{extra_conditions}
        ORDER BY t.local_transaction_date ASC
    """
