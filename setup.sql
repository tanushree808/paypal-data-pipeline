CREATE DATABASE IF NOT EXISTS PAYPAL_ANALYTICS_DB;

USE DATABASE PAYPAL_ANALYTICS_DB;

CREATE SCHEMA IF NOT EXISTS STREAMING_PIPELINE;
USE SCHEMA STREAMING_PIPELINE;

CREATE OR REPLACE TABLE PAYPAL_TRANSACTIONS(
account_number VARCHAR,
    transaction_id VARCHAR,
    event_code VARCHAR,
    status VARCHAR,
    currency VARCHAR,
    amount FLOAT,
    ending_balance FLOAT
);

CREATE OR REPLACE STORAGE INTEGRATION GCP_PAYPAL_STORAGE
TYPE=EXTERNAL_STAGE
STORAGE_PROVIDER='GCS'
ENABLED=TRUE 
STORAGE_ALLOWED_LOCATIONS = ('gcs://paypal_pipeline/processed_transactions_parquet/');

DESC STORAGE INTEGRATION gcp_paypal_storage;

CREATE OR REPLACE STAGE gcs_paypal_stage
  STORAGE_INTEGRATION = gcp_paypal_storage
  URL = 'gcs://paypal_pipeline/processed_transactions_parquet/';

  LIST @gcs_paypal_stage;

  CREATE OR REPLACE PIPE paypal_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO paypal_transactions
  FROM @gcs_paypal_stage
  FILE_FORMAT = (TYPE = 'PARQUET');

  COPY INTO paypal_transactions
FROM @gcs_paypal_stage
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO paypal_transactions
FROM @gcs_paypal_stage
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'SKIP_FILE';

SELECT COUNT(*) FROM paypal_transactions;

SELECT SYSTEM$PIPE_STATUS('PAYPAL_PIPE');

SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'PAYPAL_TRANSACTIONS',
        START_TIME => DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
    )
)
ORDER BY LAST_LOAD_TIME DESC;

CREATE OR REPLACE PIPE paypal_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO paypal_transactions
  FROM @gcs_paypal_stage
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = 'SKIP_FILE';

  CREATE OR REPLACE STAGE gcs_paypal_stage
  STORAGE_INTEGRATION = gcp_paypal_storage
  URL = 'gcs://paypal_pipeline/processed_transactions_parquet/';

  CREATE OR REPLACE PIPE paypal_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO paypal_transactions
  FROM @gcs_paypal_stage
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = 'SKIP_FILE';


CREATE OR REPLACE NOTIFICATION INTEGRATION gcp_notif_integration
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/finlake-streaming-pipeline/subscriptions/paypal-bucket-topic-sub';

CREATE OR REPLACE PIPE paypal_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = gcp_notif_integration
  AS
  COPY INTO paypal_transactions
  FROM @gcs_paypal_stage
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = 'SKIP_FILE';

  CREATE OR REPLACE PIPE paypal_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = gcp_notif_integration
  AS
  COPY INTO paypal_transactions
  FROM @gcs_paypal_stage
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = 'SKIP_FILE';
  
  DESC NOTIFICATION INTEGRATION gcp_notif_integration;

  DESC STAGE gcs_paypal_stage;

  SHOW NOTIFICATION INTEGRATIONS LIKE 'GCP_NOTIF_INTEGRATION';

  SHOW PIPES LIKE 'PAYPAL_PIPE';

  DESC STORAGE INTEGRATION GCP_PAYPAL_STORAGE;

  CREATE OR REPLACE PIPE paypal_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = gcp_notif_integration
  AS
  COPY INTO paypal_transactions
  FROM @gcs_paypal_stage
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
  ON_ERROR = 'SKIP_FILE';

  SELECT SYSTEM$PIPE_STATUS('PAYPAL_PIPE');

  SELECT * FROM TABLE(information_schema.pipe_execution_history(
  pipe_name=>'PAYPAL_PIPE',
  start_time=>dateadd(hour, -1, current_timestamp())
));

SELECT 
    status,
    COUNT(transaction_id) AS total_transactions,
    SUM(amount) AS total_monetary_volume,
    currency
FROM PAYPAL_TRANSACTIONS
GROUP BY status, currency
ORDER BY total_monetary_volume DESC;

SELECT 
    CASE 
        WHEN event_code LIKE 'T00%' THEN 'Gross Sales'
        WHEN event_code LIKE 'T04%' THEN 'Processing Fees'
        WHEN event_code LIKE 'T11%' THEN 'Refunds/Reversals'
        ELSE 'Other/Transfers'
    END AS transaction_category,
    COUNT(transaction_id) AS event_count,
    SUM(amount) AS net_impact
FROM PAYPAL_TRANSACTIONS
WHERE status = 'COMPLETED'
GROUP BY transaction_category
ORDER BY net_impact DESC;

SELECT 
    transaction_id,
    amount,
    currency,
    ending_balance
FROM PAYPAL_TRANSACTIONS
WHERE amount > 1000  -- Isolate high-value movements
ORDER BY ending_balance DESC;

SELECT 
    currency,
    COUNT(transaction_id) AS total_payments,
    ROUND(AVG(amount), 2) AS average_order_value,
    MAX(amount) AS largest_payment,
    MIN(amount) AS smallest_payment
FROM PAYPAL_TRANSACTIONS
WHERE event_code LIKE 'T00%' -- T00xx codes represent general purchasing/payments
  AND status = 'COMPLETED'
GROUP BY currency;

SELECT 
    transaction_id,
    event_code,
    amount,
    currency,
    status
FROM PAYPAL_TRANSACTIONS
WHERE event_code LIKE 'T11%' -- T11xx codes represent refunds and reversals
ORDER BY amount ASC -- Assuming refunds are recorded as negative numbers
LIMIT 10;

SELECT 
    account_number,
    currency,
    COUNT(transaction_id) AS transaction_count,
    SUM(amount) AS total_volume
FROM PAYPAL_TRANSACTIONS
WHERE status = 'COMPLETED'
GROUP BY account_number, currency
ORDER BY total_volume DESC;

SELECT 
    event_code,
    COUNT(transaction_id) AS occurrence_count,
    SUM(amount) AS total_amount_impact
FROM PAYPAL_TRANSACTIONS
GROUP BY event_code
ORDER BY occurrence_count DESC;

SELECT 
    currency,
    SUM(IFF(event_code LIKE 'T00%', amount, 0)) AS gross_sales,
    SUM(IFF(event_code LIKE 'T04%', amount, 0)) AS total_fees,
    -- Uses ABS() because fees are usually negative numbers, and NULLIF prevents division by zero
    ROUND(ABS(SUM(IFF(event_code LIKE 'T04%', amount, 0))) / NULLIF(SUM(IFF(event_code LIKE 'T00%', amount, 0)), 0) * 100, 2) AS effective_fee_percentage
FROM PAYPAL_TRANSACTIONS
WHERE status = 'COMPLETED'
GROUP BY currency;

SELECT 
    currency,
    SUM(IFF(event_code LIKE 'T00%', amount, 0)) AS gross_sales,
    SUM(IFF(event_code LIKE 'T04%', amount, 0)) AS total_fees,
    -- Uses ABS() because fees are usually negative numbers, and NULLIF prevents division by zero
    ROUND(ABS(SUM(IFF(event_code LIKE 'T04%', amount, 0))) / NULLIF(SUM(IFF(event_code LIKE 'T00%', amount, 0)), 0) * 100, 2) AS effective_fee_percentage
FROM PAYPAL_TRANSACTIONS
WHERE status = 'COMPLETED'
GROUP BY currency;

SELECT 
    event_code,
    COUNT(transaction_id) AS total_attempts,
    SUM(IFF(status = 'COMPLETED', 1, 0)) AS successful_count,
    SUM(IFF(status != 'COMPLETED', 1, 0)) AS failed_or_pending_count,
    ROUND((SUM(IFF(status = 'COMPLETED', 1, 0)) / COUNT(transaction_id)) * 100, 2) AS success_rate_percentage
FROM PAYPAL_TRANSACTIONS
GROUP BY event_code
ORDER BY total_attempts DESC;