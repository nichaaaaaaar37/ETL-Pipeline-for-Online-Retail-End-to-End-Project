import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from google.cloud import storage, bigquery
from airflow.providers.standard.operators.python import PythonOperator

# ---------------- CONFIG ----------------
BUCKET_NAME = "your-bucket-name"
DATASET_ID = "your_dataset"
TABLE_ID = "your_table"
CSV_FILE_PATH = "dags/your_file.csv"  # Path inside GCS bucket

# ---------------- TASK FUNCTIONS ----------------

def load_data(**kwargs):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(CSV_FILE_PATH)

    tmp_csv = '/home/airflow/gcs/data/online_retail_II.csv'
    blob.download_to_filename(tmp_csv)

    df = pd.read_csv(tmp_csv)
    df['Customer ID'] = df['Customer ID'].astype(str)

    tmp_parquet = '/home/airflow/gcs/data/raw.parquet'
    df.to_parquet(tmp_parquet, index=False)
    kwargs['ti'].xcom_push(key='raw_path', value=tmp_parquet)

    print("Task 1: Raw data loaded from GCS and saved as Parquet.")

def drop_duplicates(**kwargs):
    ti = kwargs['ti']
    raw_path = ti.xcom_pull(task_ids='load_data', key='raw_path')
    df = pd.read_parquet(raw_path)
    df.drop_duplicates(subset=['Invoice', 'StockCode', 'InvoiceDate'], inplace=True)

    out_path = '/home/airflow/gcs/data/cleaned.parquet'
    df.to_parquet(out_path, index=False)
    kwargs['ti'].xcom_push(key='cleaned_path', value=out_path)
    print(f"Task 2: Duplicates dropped. Shape: {df.shape}")

def handle_missing_values(**kwargs):
    ti = kwargs['ti']
    cleaned_path = ti.xcom_pull(task_ids='drop_duplicates', key='cleaned_path')
    df = pd.read_parquet(cleaned_path)
    df.fillna({'Description': 'Unknown', 'Customer ID': '0', 'Country': 'Unknown'}, inplace=True)

    out_path = '/home/airflow/gcs/data/fillna.parquet'
    df.to_parquet(out_path, index=False)
    kwargs['ti'].xcom_push(key='fillna_path', value=out_path)
    print("Task 3: Missing values handled.")

def feature_engineering(**kwargs):
    ti = kwargs['ti']
    fillna_path = ti.xcom_pull(task_ids='handle_missing_values', key='fillna_path')
    df = pd.read_parquet(fillna_path)

    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
    df['Total Price'] = df['Price'] * df['Quantity']
    df['DayOfWeek Num'] = df['InvoiceDate'].dt.dayofweek
    df['DayOfWeek Name'] = df['InvoiceDate'].dt.day_name()
    df['IsWeekend'] = df['DayOfWeek Num'] >= 5

    out_path = '/home/airflow/gcs/data/features.parquet'
    df.to_parquet(out_path, index=False)
    kwargs['ti'].xcom_push(key='features_path', value=out_path)
    print("Task 4: Features engineered.")

def handle_outliers(**kwargs):
    ti = kwargs['ti']
    features_path = ti.xcom_pull(task_ids='feature_engineering', key='features_path')
    df = pd.read_parquet(features_path)

    df = df[(df['Quantity'] > 0) & (df['Price'] > 0)]
    df['Flag For Review'] = df['Total Price'] > 1000

    out_path = '/home/airflow/gcs/data/outliers.parquet'
    df.to_parquet(out_path, index=False)
    kwargs['ti'].xcom_push(key='outliers_path', value=out_path)
    print("Task 5: Outliers handled.")

def convert_data_types(**kwargs):
    ti = kwargs['ti']
    outliers_path = ti.xcom_pull(task_ids='handle_outliers', key='outliers_path')
    df = pd.read_parquet(outliers_path)

    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    df['Quantity'] = df['Quantity'].astype(int)
    df['Price'] = df['Price'].astype(float)
    df['Total Price'] = df['Total Price'].astype(float)

    out_path = '/home/airflow/gcs/data/type_converted.parquet'
    df.to_parquet(out_path, index=False)
    kwargs['ti'].xcom_push(key='type_converted_path', value=out_path)
    print("Task 6: Data types converted.")

def trim_and_normalize_strings(**kwargs):
    ti = kwargs['ti']
    type_converted_path = ti.xcom_pull(task_ids='convert_data_types', key='type_converted_path')
    df = pd.read_parquet(type_converted_path)

    df['Description'] = df['Description'].astype(str).str.strip().str.capitalize()
    df['Country'] = df['Country'].astype(str).str.title()
    df['StockCode'] = df['StockCode'].astype(str).str.upper()

    out_path = '/home/airflow/gcs/data/final.parquet'
    df.to_parquet(out_path, index=False)
    kwargs['ti'].xcom_push(key='final_path', value=out_path)
    print("Task 7: Strings trimmed and normalized.")

def upload_to_gcs(**kwargs):
    ti = kwargs['ti']
    final_path = ti.xcom_pull(task_ids='trim_and_normalize_strings', key='final_path')

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob('online_retail_processed.parquet')
    blob.upload_from_filename(final_path)
    print(f"Task 8: File uploaded to GCS bucket '{BUCKET_NAME}'.")

def load_to_bigquery(**kwargs):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(DATASET_ID)
    table_ref = dataset_ref.table(TABLE_ID)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    uri = f"gs://{BUCKET_NAME}/online_retail_processed.parquet"
    load_job = bigquery_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Task 9: Data loaded to BigQuery table '{TABLE_ID}'.")

# ---------------- DAG ----------------

with DAG(
    dag_id='etl_retail_gcs_complete',
    start_date=days_ago(1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=['etl', 'retail', 'gcp'],
) as dag:

    t1 = PythonOperator(task_id='load_data', python_callable=load_data)
    t2 = PythonOperator(task_id='drop_duplicates', python_callable=drop_duplicates)
    t3 = PythonOperator(task_id='handle_missing_values', python_callable=handle_missing_values)
    t4 = PythonOperator(task_id='feature_engineering', python_callable=feature_engineering)
    t5 = PythonOperator(task_id='handle_outliers', python_callable=handle_outliers)
    t6 = PythonOperator(task_id='convert_data_types', python_callable=convert_data_types)
    t7 = PythonOperator(task_id='trim_and_normalize_strings', python_callable=trim_and_normalize_strings)
    t8 = PythonOperator(task_id='upload_to_gcs', python_callable=upload_to_gcs)
    t9 = PythonOperator(task_id='load_to_bigquery', python_callable=load_to_bigquery)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9
