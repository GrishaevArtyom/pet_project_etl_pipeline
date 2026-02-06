import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

OWNER = "GrishaevArtyom"
DAG_ID = "raw_from_api_to_s3"

BUCKET = "pet-project-etl"
LAYER = "raw"
SOURCE = "earthquake"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

DESCRIPTION = "Ежедневная загрузка данных о землетрясениях с открытого API и сохранение их в хранилище MinIO в сыром виде в формате Parquet."

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 22, tz="UTC"),
    "end_date": pendulum.datetime(2026, 2, 4, tz="UTC"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()
    try:
        con.sql(
            f"""
            SET TIMEZONE='UTC';
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;
    
            COPY
            (
                SELECT
                    *
                FROM
                    read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
            ) TO 's3://{BUCKET}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.parquet';
    
            """,
        )
        logging.info(f"✅ Download for date success: {start_date}")
    except Exception as e:
        logging.error(f"❌ Ошибка загрузки: {str(e)}")
    finally:
        con.close()


with DAG(
        dag_id=DAG_ID,
        schedule_interval="0 0 * * *",
        default_args=args,
        tags=["s3", "raw"],
        description=DESCRIPTION,
        max_active_runs=1,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3 >> end
