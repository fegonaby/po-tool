-- Run this once in Databricks SQL editor to create the app-managed tables.
-- Replace catalog.schema with your actual catalog and schema names.

CREATE TABLE IF NOT EXISTS query_queue (
    ID                  BIGINT,
    USERNAME            STRING,
    REQUEST_DATE        TIMESTAMP,
    SEARCH_CONTENT      STRING,
    RESOLVED_REF        STRING,
    STATEMENT_ID        STRING,
    BATCH_STATEMENT_ID  STRING,
    STATUS              STRING,
    RESULT_COUNT        INT,
    STARTED_DATE        TIMESTAMP,
    COMPLETED_DATE      TIMESTAMP,
    ERROR_MESSAGE       STRING
);

CREATE TABLE IF NOT EXISTS query_results (
    QUEUE_ID                BIGINT,
    PAYMENT_REF_NUMBER      STRING,
    STATUS_CODE             STRING,
    TRANSACTION_AMOUNT      DECIMAL(18,2),
    REQUEST_DATE            TIMESTAMP,
    RECEIVED_DATE           TIMESTAMP,
    SENDER_EMAIL            STRING,
    SENDER_PHONE            STRING,
    SENDER_NAME             STRING,
    SENDER_LEGAL_NAME       STRING,
    SENDER_FI               STRING,
    SENDER_FI_USERID        STRING,
    SENDER_ACCOUNT          STRING,
    SENDER_FI_REF           STRING,
    SENDER_IP               STRING,
    SENDER_MEMO             STRING,
    RECIPIENT_EMAIL         STRING,
    RECIPIENT_PHONE         STRING,
    RECIPIENT_NAME          STRING,
    RECIPIENT_LEGAL_NAME    STRING,
    RECIPIENT_FI            STRING,
    RECIPIENT_FI_USERID     STRING,
    RECIPIENT_ACCOUNT       STRING,
    RECIPIENT_FI_REF        STRING,
    RECIPIENT_IP            STRING,
    RECIPIENT_MEMO          STRING,
    CONTACT_EMAIL           STRING,
    CONTACT_PHONE           STRING,
    CONTACT_NAME            STRING,
    CONTACT_ACCOUNT         STRING,
    CONTACT_NOTIFICATION    STRING,
    SECURITY_QUESTION       STRING,
    TRANSFER_TYPE           STRING,
    FRAUDULENT              STRING,
    SCAM_CATEGORY           STRING
);

CREATE TABLE IF NOT EXISTS audit_log (
    ID                  BIGINT,
    USERNAME            STRING,
    TIMESTAMP           TIMESTAMP,
    ACTION              STRING,
    SEARCH_CONTENT      STRING,
    QUEUE_ID            BIGINT
);
