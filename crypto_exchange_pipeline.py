import json
import requests
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.http_to_gcs import HttpToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import io

GCP_PROJECT = "airflow-learn-502205"
GCS_BUCKET = "crypto-exchange-pipeline-siddharth"
GCS_RAW_DATA_PATH = "raw_data/crypto_raw_data"
GCS_TRANSFORMED_PATH = "transformed_data/crypto_transformed_data"
BIGQUERY_DATASET = "crypto_db"
BIGQUERY_table = "tbl_crypto"
HTTP_CONN_ID = "coingecko_api"
ENDPOINT = "/api/v3/coins/markets"
PARAMS = {
        'vs_currency' : 'usd',
        'order' : 'market_cap_desc',
        'per_page' : 10,
        'page' : 1,
        'sparkline' : False
    }


BG_SCHEMA = [
    {"name":"id","type":"STRING","mode":"REQUIRED"},
    {"name":"symbol","type":"STRING","mode":"REQUIRED"},
    {"name":"name","type":"STRING","mode":"REQUIRED"},
    {"name":"current_price","type":"FLOAT","mode":"NULLABLE"},
    {"name":"market_cap","type":"FLOAT","mode":"NULLABLE"},
    {"name":"total_volume","type":"FLOAT","mode":"NULLABLE"},
    {"name":"last_updated","type":"TIMESTAMP","mode":"NULLABLE"},
    {"name":"timestamp","type":"TIMESTAMP","mode":"REQUIRED"}
]

# def _fetch_data_from_api():
#     url = "https://api.coingecko.com/api/v3/coins/markets"
#     params = {
#         'vs_currency' : 'usd',
#         'order' : 'market_cap_desc',
#         'per_page' : 10,
#         'page' : 1,
#         'sparkline' : False
#     }

#     response = requests.get(url,params=params)
#     data = response.json()
#     #print(data)
#     with open("crypto_data.json", 'w') as f:
#         json.dump(data,f)



def _transform_data(bucket_name,object_name,**kwargs):
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    file_bytes = gcs_hook.download(bucket_name=bucket_name, object_name=object_name)
    df = pd.read_json(io.BytesIO(file_bytes))

    transformed_df = df[["id", "symbol", "name", "current_price", "market_cap", "total_volume", "last_updated"]].copy()
    transformed_df["timestamp"] = datetime.utcnow().isoformat()

    return transformed_df.to_csv(index=False)


def _upload_transformed_data(bucket_name,destination,**kwargs):
    csv_data = kwargs["ti"].xcom_pull(task_ids="transformed_data_task")
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    gcs_hook.upload(bucket_name=bucket_name, object_name=destination, data=csv_data)

    print(f"Successfully uploaded data to gs://{bucket_name}/{destination}")


default_args = {
    "Owner": "Siddharth",
    "depends_on_past": False
}

dag = DAG(
    dag_id = "crypto_exchange_pipeline",
    default_args = default_args,
    schedule = timedelta(minutes=10),
    start_date= datetime(2026,7,12),
    catchup=False
)

# fetch_data_task = PythonOperator(
#     task_id = "fetch_data_task",
#     python_callable = _fetch_data_from_api,
#     dag=dag
# )

fetch_data_store_gcs = HttpToGCSOperator(
    task_id = "fetch_data_store_gcs",
    http_conn_id = HTTP_CONN_ID,
    endpoint = ENDPOINT,
    data = PARAMS,
    method="GET",
    gcp_conn_id = "google_cloud_default",
    bucket_name = GCS_BUCKET,
    object_name = GCS_RAW_DATA_PATH + "_{{ ts_nodash }}.json",
    dag=dag
)


create_bucket_task = GCSCreateBucketOperator(
    task_id = "create_bucket_task",
    bucket_name = GCS_BUCKET,
    storage_class = "MULTI_REGIONAL",
    location  = "US",
    gcp_conn_id = "google_cloud_default",
    dag=dag
)


# upload_raw_data_to_gcs_task = LocalFilesystemToGCSOperator(
#     task_id = "upload_raw_data_to_gcs_task",
#     src = "crypto_data.json",
#     dst = GCS_RAW_DATA_PATH + "_{{ ts_nodash }}.json",
#     bucket = GCS_BUCKET,
#     gcp_conn_id = "google_cloud_default",
#     dag=dag
# )

transformed_data_task = PythonOperator(
    task_id = "transformed_data_task",
    python_callable = _transform_data,
    op_kwargs={
        "bucket_name": GCS_BUCKET,
        "object_name": GCS_RAW_DATA_PATH + "_{{ ts_nodash }}.json",
    },
    dag=dag
)

upload_transformed_data_to_gcs_task = PythonOperator(
    task_id = "upload_transformed_data_to_gcs_task",
    python_callable = _upload_transformed_data,
    op_kwargs={
        "bucket_name": GCS_BUCKET,
        "destination": GCS_TRANSFORMED_PATH + "_{{ ts_nodash }}.csv",
    },
    dag=dag
)


create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id="create_dataset",
    dataset_id=BIGQUERY_DATASET,
    gcp_conn_id="google_cloud_default",
    dag=dag
)

create_table = BigQueryCreateTableOperator(
    task_id="create_table",
    dataset_id=BIGQUERY_DATASET,
    table_id = BIGQUERY_table,
    table_resource = {
        "schema": {"fields": BG_SCHEMA}
    },
    gcp_conn_id="google_cloud_default",
    dag=dag
)


load_data_to_bigquery = GCSToBigQueryOperator(
    task_id = "load_data_to_bigquery",
    bucket = GCS_BUCKET,
    source_objects = [GCS_TRANSFORMED_PATH + "_{{ ts_nodash }}.csv"],
    destination_project_dataset_table = f'{BIGQUERY_DATASET}.{BIGQUERY_table}',
    schema_fields = BG_SCHEMA,
    source_format="CSV",
    skip_leading_rows=1,
    write_disposition="WRITE_TRUNCATE",
    gcp_conn_id="google_cloud_default",
    dag=dag
)


# fetch_data_task >> create_bucket_task >> upload_raw_data_to_gcs_task >> transformed_data_task >> upload_transformed_data_to_gcs_task
# upload_transformed_data_to_gcs_task >> create_dataset >>  create_table >> load_data_to_bigquery


create_bucket_task >> fetch_data_store_gcs >> transformed_data_task >> upload_transformed_data_to_gcs_task >> create_dataset >> create_table >> load_data_to_bigquery