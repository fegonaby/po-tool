"""e-Transfer Excel export with field selection."""

from config.settings import qualified_table
from src.core.export import build_excel, save_excel_to_file
from src.etransfer.queries import ALWAYS_FIELDS, build_download_sql

_RESULTS = qualified_table("query_results")

ETRANSFER_COLUMN_MAPPING = [
    ("PAYMENT_REF_NUMBER", "Payment Ref Number"),
    ("STATUS_CODE", "Status"),
    ("TRANSACTION_AMOUNT", "Amount"),
    ("REQUEST_DATE", "Request Date"),
    ("RECEIVED_DATE", "Received Date"),
    ("SENDER_EMAIL", "Sender Email"),
    ("SENDER_PHONE", "Sender Phone"),
    ("SENDER_NAME", "Sender Name"),
    ("SENDER_LEGAL_NAME", "Sender Legal Name"),
    ("SENDER_FI", "Sender FI"),
    ("SENDER_FI_USERID", "Sender FI User ID"),
    ("SENDER_ACCOUNT", "Sender Account"),
    ("SENDER_FI_REF", "Sender FI Ref"),
    ("SENDER_IP", "Sender IP"),
    ("SENDER_MEMO", "Sender Memo"),
    ("RECIPIENT_EMAIL", "Recipient Email"),
    ("RECIPIENT_PHONE", "Recipient Phone"),
    ("RECIPIENT_NAME", "Recipient Name"),
    ("RECIPIENT_LEGAL_NAME", "Recipient Legal Name"),
    ("RECIPIENT_FI", "Recipient FI"),
    ("RECIPIENT_FI_USERID", "Recipient FI User ID"),
    ("RECIPIENT_ACCOUNT", "Recipient Account"),
    ("RECIPIENT_FI_REF", "Recipient FI Ref"),
    ("RECIPIENT_IP", "Recipient IP"),
    ("RECIPIENT_MEMO", "Recipient Memo"),
    ("CONTACT_EMAIL", "Contact Email"),
    ("CONTACT_PHONE", "Contact Phone"),
    ("CONTACT_NAME", "Contact Name"),
    ("CONTACT_ACCOUNT", "Contact Account"),
    ("CONTACT_NOTIFICATION", "Contact Notification"),
    ("SECURITY_QUESTION", "Security Question"),
    ("TRANSFER_TYPE", "Transfer Type"),
    ("FRAUDULENT", "Fraudulent"),
    ("SCAM_CATEGORY", "Scam Category"),
]


def export_etransfer_results(
    client,
    queue_id: int,
    selected_fields: list[str] | None = None,
) -> bytes:
    """Download results for a single queue ID and build Excel.

    Args:
        client: DatabricksClient instance.
        queue_id: The queue row to export.
        selected_fields: Optional list of extra field names beyond the always-included ones.
            If None, all fields are included.

    Returns:
        In-memory Excel bytes.
    """
    clause = f"QUEUE_ID = {queue_id}"

    if selected_fields is None:
        all_field_keys = [key for key, _ in ETRANSFER_COLUMN_MAPPING]
        extra = [f for f in all_field_keys if f not in ALWAYS_FIELDS]
    else:
        extra = selected_fields

    sql = build_download_sql(clause, extra)

    rows = client.execute_sync(sql)

    active_keys = set(ALWAYS_FIELDS) | set(extra)
    mapping = [(key, label) for key, label in ETRANSFER_COLUMN_MAPPING if key in active_keys]

    return build_excel("ETPO.xlsx", rows, mapping)


def export_etransfer_to_file(
    client,
    queue_id: int,
    output_path: str,
    selected_fields: list[str] | None = None,
) -> str:
    """Export e-Transfer results to an Excel file on disk. Returns the path."""
    data = export_etransfer_results(client, queue_id, selected_fields)
    path = save_excel_to_file(data, output_path)
    return str(path)
