# PayPal End-to-End Data Pipeline

An automated data engineering pipeline that extracts PayPal API data, transforms it in the cloud, and loads it into a Snowflake data warehouse for analytics. 

## Pipeline Architecture

1. **Extraction**: A serverless Google Cloud Function extracts transaction data from the PayPal API and lands it in a raw Google Cloud Storage (GCS) bucket.
2. **Orchestration**: Apache Airflow (via DAGs) manages and schedules the pipeline execution.
3. **Transformation**: This pipeline leverages Apache Spark on a virtual machine to efficiently process the raw JSON data stored in Google Cloud Platform storage buckets, converting it into optimized Parquet files.
4. **Ingestion**: A GCP Pub/Sub notification triggers Snowpipe to automatically ingest the new Parquet files into a Snowflake target table.

## Technology Stack

* **Cloud Provider**: Google Cloud Platform (GCP)
* **Compute & Processing**: Cloud Functions, Compute Engine (e2-standard-2), PySpark
* **Storage**: Google Cloud Storage (GCS)
* **Orchestration**: Apache Airflow
* **Data Warehouse**: Snowflake, Snowpipe
* **Messaging**: Pub/Sub

## Security Note
API credentials, virtual environment configurations, and raw data files are intentionally excluded from this repository via `.gitignore` to maintain security.
