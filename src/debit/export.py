"""Debit Excel export."""

from po_tool.core.export import build_excel, save_excel_to_file

DEBIT_COLUMN_MAPPING = [
    ("issuer_name", "Issuer"),
    ("fpan", "PAN"),
    ("terminal_id", "Terminal ID"),
    ("local_transaction_date", "Transaction Date"),
    ("amount", "Amount"),
]


def export_debit_results(results: list[dict]) -> bytes:
    """Build a debit Excel file from transaction results.

    Uses FPAN if available, falls back to PAN.
    """
    for row in results:
        if not row.get("fpan"):
            row["fpan"] = row.get("pan", "")

    return build_excel("DPO.xlsx", results, DEBIT_COLUMN_MAPPING)


def export_debit_to_file(results: list[dict], output_path: str) -> str:
    """Export debit results to an Excel file on disk. Returns the path."""
    data = export_debit_results(results)
    path = save_excel_to_file(data, output_path)
    return str(path)
