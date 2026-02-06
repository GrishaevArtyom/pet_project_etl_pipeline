import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "GrishaevArtyom"
DAG_ID = "raw_from_s3_to_pg"

BUCKET = "pet-project-etl"
LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

PG_PASSWORD = Variable.get("pg_password")

DESCRIPTION = "Перенос сырых данных из хранилища MinIO в оперативный слой PostgreSQL. DAG запускается ежедневно после завершения загрузки данных из API."

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


def get_and_transfer_raw_data_to_ods_pg(**context):
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
    
            CREATE SECRET dwh_postgres (
                TYPE postgres,
                HOST 'postgres_dwh',
                PORT 5432,
                DATABASE postgres,
                USER 'postgres',
                PASSWORD '{PG_PASSWORD}'
            );
    
            ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
    
            DELETE FROM dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            WHERE time >= '{start_date}' AND time < '{end_date}';
    
            INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            (
                time,
                latitude,
                longitude,
                depth,
                mag,
                mag_type,
                nst,
                gap,
                dmin,
                rms,
                net,
                id,
                updated,
                place,
                type,
                horizontal_error,
                depth_error,
                mag_error,
                mag_nst,
                status,
                location_source,
                mag_source
            )
            SELECT
                time,
                latitude,
                longitude,
                depth,
                mag,
                magType AS mag_type,
                nst,
                gap,
                dmin,
                rms,
                net,
                id,
                updated,
                place,
                type,
                horizontalError AS horizontal_error,
                depthError AS depth_error,
                magError AS mag_error,
                magNst AS mag_nst,
                status,
                locationSource AS location_source,
                magSource AS mag_source
            FROM 's3://{BUCKET}/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.parquet';
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
        tags=["s3", "ods", "pg"],
        description=DESCRIPTION,
        max_active_runs=1,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=7200,
        poke_interval=60,
    )

    get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end
