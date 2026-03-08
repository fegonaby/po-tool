"""
Generate synthetic mock data for the PO-Order future state.
Outputs CSV files to mock_data/csv/ for both DuckDB and Databricks use.
"""

import os
import random
import string
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker("en_CA")
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "csv")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- Constants ---

FI_CODES = [
    "CA000001", "CA000002", "CA000003", "CA000004", "CA000006",
    "CA000010", "CA000011", "CA000016", "CA000219", "CA000352",
    "CA000540", "CA000614", "CA000809", "CA000815",
]

STATUS_CODES = ["3", "8", "10", "11"]
STATUS_WEIGHTS = [0.20, 0.10, 0.60, 0.10]

TRANSFER_TYPES = [0, 1, 2]
TRANSFER_TYPE_WEIGHTS = [0.70, 0.20, 0.10]

CONTACT_NOTIFICATION = [0, 1, 2]

DATE_START = datetime(2021, 1, 1)
DATE_END = datetime(2025, 12, 31)
DATE_RANGE_DAYS = (DATE_END - DATE_START).days

PROVINCES = ["ON", "QC", "BC", "AB", "MB", "SK", "NS", "NB", "NL", "PE", "NT", "YT", "NU"]

CANADIAN_ISSUERS = [
    "BMO Bank of Montreal", "Bank of Nova Scotia", "Royal Bank of Canada",
    "Toronto-Dominion Bank", "National Bank of Canada", "CIBC",
    "President's Choice Financial", "HSBC Canada", "ATB Financial",
    "Desjardins Group",
]

ACQUIRER_NAMES = [
    "Moneris Solutions", "TD Merchant Services", "Global Payments",
    "Chase Paymentech", "Elavon", "First Data", "Worldpay",
    "Square", "Stripe Canada", "PayPal Canada",
    "Nuvei", "Paysafe", "Pivotal Payments", "Helcim", "Bambora",
]

FRAUD_CATEGORIES = [
    (1, "Romance Scam"),
    (2, "Investment Scam"),
    (3, "Phishing"),
    (4, "Employment Scam"),
    (5, "Other"),
]


def random_date(start=DATE_START, end=DATE_END):
    return start + timedelta(days=random.randint(0, (end - start).days))


def random_timestamp(start=DATE_START, end=DATE_END):
    dt = random_date(start, end)
    dt = dt.replace(
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
    )
    return dt


def random_ref_number():
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=16))


def random_fi_userid():
    return "".join(random.choices(string.digits, k=10))


def random_account_number():
    return "".join(random.choices(string.digits, k=random.randint(7, 12)))


def random_pan():
    return "".join(random.choices(string.digits, k=16))


def random_phone():
    return f"{random.randint(200,999)}{random.randint(1000000,9999999)}"


def random_ip():
    return f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


# ============================================================
# e-Transfer: emt_transfers (100,000 rows)
# ============================================================
def generate_emt_transfers(n=100_000):
    print(f"Generating {n} e-Transfer records...")

    # Pre-generate pools of reusable sender/recipient identities
    # so some emails/phones/accounts appear in multiple transfers
    sender_pool_size = n // 5
    recipient_pool_size = n // 4

    sender_emails = [fake.email() for _ in range(sender_pool_size)]
    sender_phones = [random_phone() for _ in range(sender_pool_size)]
    sender_names = [fake.first_name() for _ in range(sender_pool_size)]
    sender_legal_firsts = [fake.first_name() for _ in range(sender_pool_size)]
    sender_legal_middles = [fake.first_name() if random.random() < 0.3 else "" for _ in range(sender_pool_size)]
    sender_legal_lasts = [fake.last_name() for _ in range(sender_pool_size)]
    sender_accounts = [random_account_number() for _ in range(sender_pool_size)]
    sender_fi_userids = [random_fi_userid() for _ in range(sender_pool_size)]

    recipient_emails = [fake.email() for _ in range(recipient_pool_size)]
    recipient_phones = [random_phone() for _ in range(recipient_pool_size)]
    recipient_names = [fake.first_name() for _ in range(recipient_pool_size)]
    recipient_legal_firsts = [fake.first_name() for _ in range(recipient_pool_size)]
    recipient_legal_middles = [fake.first_name() if random.random() < 0.3 else "" for _ in range(recipient_pool_size)]
    recipient_legal_lasts = [fake.last_name() for _ in range(recipient_pool_size)]
    recipient_accounts = [random_account_number() for _ in range(recipient_pool_size)]
    recipient_fi_userids = [random_fi_userid() for _ in range(recipient_pool_size)]

    rows = []
    for i in range(n):
        req_ts = random_timestamp()
        recv_ts = req_ts + timedelta(
            hours=random.randint(0, 72),
            minutes=random.randint(0, 59),
        )
        status = random.choices(STATUS_CODES, STATUS_WEIGHTS)[0]
        # Cancelled/Offline transfers may not have received timestamp
        if status in ("8", "11") and random.random() < 0.5:
            recv_ts = None

        si = random.randint(0, sender_pool_size - 1)
        ri = random.randint(0, recipient_pool_size - 1)

        # Some transfers don't have contact info
        has_contact = random.random() < 0.6

        rows.append({
            "ID": i + 1,
            "REFERENCE_NUMBER": random_ref_number(),
            "STATUS_CODE": status,
            "AMOUNT": round(random.uniform(0.01, 10000.00), 2),
            "REQUEST_TIMESTAMP": req_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "RECEIVED_TIMESTAMP": recv_ts.strftime("%Y-%m-%d %H:%M:%S") if recv_ts else None,
            "SENDER_EMAIL_ADDRESS": sender_emails[si],
            "SENDER_PHONE_NUMBER": sender_phones[si],
            "SENDER_NAME": sender_names[si],
            "SENDER_LEGAL_FIRST_NAME": sender_legal_firsts[si],
            "SENDER_LEGAL_MIDDLE_NAME": sender_legal_middles[si],
            "SENDER_LEGAL_LAST_NAME": sender_legal_lasts[si],
            "SENDER_FI_ID": random.choice(FI_CODES),
            "SENDER_FI_USERID": sender_fi_userids[si],
            "SENDER_ACCOUNT_NUMBER": sender_accounts[si],
            "SENDER_IP_ADDRESS": random_ip(),
            "SENDER_MEMO": fake.sentence(nb_words=6) if random.random() < 0.4 else None,
            "CONTACT_EMAIL_ADDRESS": fake.email() if has_contact else None,
            "CONTACT_PHONE_NUMBER": random_phone() if has_contact and random.random() < 0.5 else None,
            "CONTACT_NAME": fake.name() if has_contact else None,
            "CONTACT_QUESTION": fake.sentence(nb_words=4) + "?" if random.random() < 0.7 else None,
            "CONTACT_NOTIFICATION_INDICATOR": random.choice(CONTACT_NOTIFICATION),
            "CONTACT_ACCOUNT_NUMBER": random_account_number() if has_contact and random.random() < 0.3 else None,
            "RECIPIENT_EMAIL_ADDRESS": recipient_emails[ri],
            "RECIPIENT_NAME": recipient_names[ri],
            "RECIPIENT_LEGAL_FIRST_NAME": recipient_legal_firsts[ri],
            "RECIPIENT_LEGAL_MIDDLE_NAME": recipient_legal_middles[ri],
            "RECIPIENT_LEGAL_LAST_NAME": recipient_legal_lasts[ri],
            "RECIPIENT_FI_ID": random.choice(FI_CODES) if status == "10" else None,
            "RECIPIENT_FI_USERID": recipient_fi_userids[ri] if status == "10" else None,
            "RECIPIENT_ACCOUNT_NUMBER": recipient_accounts[ri] if status == "10" else None,
            "RECIPIENT_IP_ADDRESS": random_ip() if status == "10" else None,
            "RECIPIENT_PHONE_NUMBER": recipient_phones[ri] if random.random() < 0.3 else None,
            "RECIPIENT_MEMO": fake.sentence(nb_words=5) if random.random() < 0.2 else None,
            "TRANSFER_TYPE_CODE": random.choices(TRANSFER_TYPES, TRANSFER_TYPE_WEIGHTS)[0],
        })

        if (i + 1) % 25000 == 0:
            print(f"  ...{i + 1}/{n}")

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUTPUT_DIR, "emt_transfers.csv"), index=False)
    print(f"  Saved emt_transfers.csv ({len(df)} rows)")
    return df


# ============================================================
# payment_fi_reference_number (2 per transfer)
# ============================================================
def generate_fi_refs(transfers_df):
    print("Generating FI reference numbers...")
    rows = []
    for i, row in transfers_df.iterrows():
        req_ts = row["REQUEST_TIMESTAMP"]
        ref_num = row["REFERENCE_NUMBER"]

        # Sender FI ref (seq=1)
        rows.append({
            "FI_FI_REF_SEQ": 1,
            "PAYMENT_REFERENCE_NUMBER": ref_num,
            "FI_REFERENCE_NUMBER": random_ref_number(),
            "REQUEST_DATE": req_ts,
        })
        # Recipient FI ref (seq=2)
        rows.append({
            "FI_FI_REF_SEQ": 2,
            "PAYMENT_REFERENCE_NUMBER": ref_num,
            "FI_REFERENCE_NUMBER": random_ref_number(),
            "REQUEST_DATE": req_ts,
        })

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUTPUT_DIR, "payment_fi_reference_number.csv"), index=False)
    print(f"  Saved payment_fi_reference_number.csv ({len(df)} rows)")
    return df


# ============================================================
# Fraud tables
# ============================================================
def generate_fraud_tables(transfers_df):
    print("Generating fraud/scam tables...")

    # fraud_categories
    df_categories = pd.DataFrame(FRAUD_CATEGORIES, columns=["ID", "NAME"])
    df_categories.to_csv(os.path.join(OUTPUT_DIR, "fraud_categories.csv"), index=False)
    print(f"  Saved fraud_categories.csv ({len(df_categories)} rows)")

    all_ids = transfers_df["ID"].tolist()

    # fraudulent_emt_transfers (~500)
    fraud_ids = random.sample(all_ids, min(500, len(all_ids)))
    df_fraudulent = pd.DataFrame({"TRANSFER_ID": fraud_ids})
    df_fraudulent.to_csv(os.path.join(OUTPUT_DIR, "fraudulent_emt_transfers.csv"), index=False)
    print(f"  Saved fraudulent_emt_transfers.csv ({len(df_fraudulent)} rows)")

    # scammed_emt_transfers (~300, subset of non-fraudulent for variety)
    remaining_ids = list(set(all_ids) - set(fraud_ids))
    scam_ids = random.sample(remaining_ids, min(300, len(remaining_ids)))
    df_scammed = pd.DataFrame({
        "TRANSFER_ID": scam_ids,
        "FRAUD_CATEGORY_ID": [random.choice([c[0] for c in FRAUD_CATEGORIES]) for _ in scam_ids],
    })
    df_scammed.to_csv(os.path.join(OUTPUT_DIR, "scammed_emt_transfers.csv"), index=False)
    print(f"  Saved scammed_emt_transfers.csv ({len(df_scammed)} rows)")


# ============================================================
# Debit: idp_acquirers
# ============================================================
def generate_acquirers():
    print("Generating acquirers...")
    rows = [{"ACQUIRER_NUMBER": i + 1, "NAME": name} for i, name in enumerate(ACQUIRER_NAMES)]
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUTPUT_DIR, "idp_acquirers.csv"), index=False)
    print(f"  Saved idp_acquirers.csv ({len(df)} rows)")
    return df


# ============================================================
# Debit: issuers
# ============================================================
def generate_issuers():
    print("Generating issuers...")
    rows = [{"ISSUER_ID": i + 1, "NAME": name} for i, name in enumerate(CANADIAN_ISSUERS)]
    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUTPUT_DIR, "issuers.csv"), index=False)
    print(f"  Saved issuers.csv ({len(df)} rows)")
    return df


# ============================================================
# Debit: idp_mtd_terminals (~500)
# ============================================================
def generate_terminals(acquirers_df, n=500):
    print(f"Generating {n} terminals...")
    acquirer_nums = acquirers_df["ACQUIRER_NUMBER"].tolist()
    rows = []
    for i in range(n):
        first_txn = random_date()
        last_txn = first_txn + timedelta(days=random.randint(30, 1500))
        if last_txn > DATE_END:
            last_txn = DATE_END

        rows.append({
            "TERMINAL_ID": f"T{str(i + 1).zfill(8)}",
            "MERCHANT_NUMBER": f"M{str(random.randint(1, n // 3)).zfill(6)}",
            "NAME": fake.company(),
            "ADDRESS": fake.street_address(),
            "CITY": fake.city(),
            "PROVINCE": random.choice(PROVINCES),
            "POSTAL_CODE": fake.postalcode().replace(" ", ""),
            "FIRST_TRANSACTION_DATE": first_txn.strftime("%Y-%m-%d"),
            "LAST_TRANSACTION_DATE": last_txn.strftime("%Y-%m-%d"),
            "ACQUIRER_NUMBER": random.choice(acquirer_nums),
        })

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUTPUT_DIR, "idp_mtd_terminals.csv"), index=False)
    print(f"  Saved idp_mtd_terminals.csv ({len(df)} rows)")
    return df


# ============================================================
# Debit: idp_mtd_transactions (~10,000)
# ============================================================
def generate_transactions(terminals_df, issuers_df, n=10_000):
    print(f"Generating {n} debit transactions...")
    issuer_ids = issuers_df["ISSUER_ID"].tolist()
    terminal_rows = terminals_df[["TERMINAL_ID", "MERCHANT_NUMBER", "FIRST_TRANSACTION_DATE", "LAST_TRANSACTION_DATE"]].to_dict("records")

    rows = []
    for i in range(n):
        terminal = random.choice(terminal_rows)
        first_dt = datetime.strptime(terminal["FIRST_TRANSACTION_DATE"], "%Y-%m-%d")
        last_dt = datetime.strptime(terminal["LAST_TRANSACTION_DATE"], "%Y-%m-%d")
        txn_date = random_date(first_dt, last_dt)

        rows.append({
            "TERMINAL_ID": terminal["TERMINAL_ID"],
            "MERCHANT_NUMBER": terminal["MERCHANT_NUMBER"],
            "HEADER_CAPTURE_DATE": txn_date.strftime("%Y-%m-%d"),
            "LOCAL_TRANSACTION_DATE": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
            "AMOUNT": round(random.uniform(0.50, 5000.00), 2),
            "PAN": random_pan(),
            "ISSUER_ID": random.choice(issuer_ids),
        })

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUTPUT_DIR, "idp_mtd_transactions.csv"), index=False)
    print(f"  Saved idp_mtd_transactions.csv ({len(df)} rows)")
    return df


# ============================================================
# Debit: tsp_fpan_dpan_history (~2,000)
# ============================================================
def generate_fpan_dpan(transactions_df, n=2000):
    print(f"Generating {n} FPAN/DPAN history records...")
    # Use a subset of transaction PANs as DPANs
    all_pans = transactions_df["PAN"].tolist()
    selected_pans = random.sample(all_pans, min(n, len(all_pans)))

    rows = []
    for dpan in selected_pans:
        min_ts = random_timestamp()
        max_ts = min_ts + timedelta(days=random.randint(30, 730))

        rows.append({
            "DPAN": dpan,
            "FPAN": random_pan(),
            "MIN_TKM_OPERATION_TIMESTAMP": min_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "MAX_TKM_OPERATION_TIMESTAMP": max_ts.strftime("%Y-%m-%d %H:%M:%S"),
        })

    df = pd.DataFrame(rows)
    df.to_csv(os.path.join(OUTPUT_DIR, "tsp_fpan_dpan_history.csv"), index=False)
    print(f"  Saved tsp_fpan_dpan_history.csv ({len(df)} rows)")


# ============================================================
# Main
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("Generating mock data for PO-Order future state")
    print("=" * 60)

    # e-Transfer
    transfers_df = generate_emt_transfers(100_000)
    generate_fi_refs(transfers_df)
    generate_fraud_tables(transfers_df)

    # Debit
    acquirers_df = generate_acquirers()
    issuers_df = generate_issuers()
    terminals_df = generate_terminals(acquirers_df, 500)
    transactions_df = generate_transactions(terminals_df, issuers_df, 10_000)
    generate_fpan_dpan(transactions_df, 2000)

    print("=" * 60)
    print("All CSVs generated in mock_data/csv/")
    print("=" * 60)
