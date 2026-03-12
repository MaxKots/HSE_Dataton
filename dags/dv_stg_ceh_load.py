from datetime import datetime
from io import BytesIO

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain

import pandas as pd


MINIO_CONN_ID = "minio_default"
POSTGRES_CONN_ID = "postgres_default"

BUCKET_NAME = Variable.get("stg_ceh_minio_bucket", default_var="raw-zone")
PREFIX = Variable.get("stg_ceh_prefix", default_var=None)

TABLE_FILES = {
    "stg_ceh.fct_banners_show": "Fct_banners_show.csv",
    "stg_ceh.fct_actions": "Fct_actions.csv",
    "stg_ceh.cd_banner": "CD_banner.csv",
    "stg_ceh.cd_user": "CD_user.csv",
    "stg_ceh.cd_campaign": "CD_campaign.csv",
    "stg_ceh.installs": "Installs.csv",
}


def load_csv_from_minio_to_pg(table_name: str, filename: str, **context):
    s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
    key = f"{PREFIX}/{filename}" if PREFIX else filename

    print(f"[S3] bucket={BUCKET_NAME}, key={key}")
    print(f"[LOAD] target_table={table_name}")

    obj = s3.get_key(key, bucket_name=BUCKET_NAME)
    if obj is None:
        raise FileNotFoundError(f"Object {key} not found in bucket {BUCKET_NAME}")

    body = obj.get()["Body"].read()


    if table_name == "stg_ceh.cd_banner":
        df = pd.read_csv(BytesIO(body), sep=";")
        df.rename(
            columns={
                "banner_id": "banner_id",
                "creative_type (статика/видео/анимация)": "creative_type",
                "message (сообщение на баннере)": "message",
                "size": "size",
                "target_audience_segment": "target_audience_segment",
            },
            inplace=True,
        )


    elif table_name == "stg_ceh.cd_campaign":
        df = pd.read_csv(BytesIO(body), sep=";")

        df["start_date"] = pd.to_datetime(df["start_date"], format="%d.%m.%Y").dt.date
        df["end_date"] = pd.to_datetime(df["end_date"], format="%d.%m.%Y").dt.date

    elif table_name == "stg_ceh.cd_user":
        df = pd.read_csv(BytesIO(body), sep=";")
        df.rename(
            columns={
                "User_id": "user_id",
                "segment": "segment",
                "tariff": "tariff",
                "date_create": "date_create",
                "date_end": "date_end",
            },
            inplace=True,
        )

        # даты в формате 2025-01-18 -> ISO / mixed
        df["date_create"] = pd.to_datetime(df["date_create"], format="ISO8601").dt.date
        df["date_end"] = pd.to_datetime(
            df["date_end"], format="ISO8601", errors="coerce"
        ).dt.date

    elif table_name == "stg_ceh.fct_actions":
        df = pd.read_csv(BytesIO(body), sep=";")
        df.rename(
            columns={
                "user_id": "user_id",
                "session_start": "session_start",
                "actions (регистрация, первый заказ и т.д.)": "actions",
            },
            inplace=True,
        )
        df["session_start"] = pd.to_datetime(
            df["session_start"],
            format="ISO8601",          
    )


    elif table_name == "stg_ceh.fct_banners_show":
        df = pd.read_csv(BytesIO(body), sep=";")
        df.rename(
            columns={
                "banner_id": "banner_id",
                "campaign_id": "campaign_id",
                "user_id": "user_id",
                "timestamp": "timestamp",
                "placement (сайт/приложение/соцсеть)": "placement",
                "device_type": "device_type",
                "os": "os",
                "geo": "geo",
                "is_clicked (0/1)": "is_clicked",
            },
            inplace=True,
        )

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            format="ISO8601",
        )
        df


    elif table_name == "stg_ceh.installs":
        df = pd.read_csv(BytesIO(body), sep=";")
        df.rename(
            columns={
                "user_id": "user_id",
                "install_timestamp": "install_timestamp",
                "source (баннер / органика / другое)": "source",
            },
            inplace=True,
        )

        df["install_timestamp"] = pd.to_datetime(
            df["install_timestamp"],
            format="ISO8601",   
        )


    else:
        df = pd.read_csv(BytesIO(body), sep=";")

    print(f"[DF] rows={len(df)}, columns={list(df.columns)}")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    schema, table = table_name.split(".")
    print(f"[PG] truncate {table_name}")
    with engine.begin() as conn:
        conn.execute(f"TRUNCATE TABLE {table_name};")
        print(f"[PG] insert into {schema}.{table}")
        df.to_sql(
            name=table,
            schema=schema,
            con=conn,
            if_exists="append",
            index=False,
        )
    print(f"[DONE] loaded {len(df)} rows into {table_name}")


with DAG(
    dag_id="dv_stg_ceh_load",
    start_date=datetime(2025, 2, 1),
    schedule_interval=None,
    catchup=False,
    tags=["staging", "minio", "postgres"],
) as dag:

    tasks = {}
    for table, filename in TABLE_FILES.items():
        tasks[table] = PythonOperator(
            task_id=f"load_{table.replace('.', '_')}",
            python_callable=load_csv_from_minio_to_pg,
            op_kwargs={"table_name": table, "filename": filename},
        )

    chain(
        tasks["stg_ceh.cd_banner"],
        tasks["stg_ceh.cd_user"],
        tasks["stg_ceh.cd_campaign"],
        tasks["stg_ceh.fct_banners_show"],
        tasks["stg_ceh.installs"],
        tasks["stg_ceh.fct_actions"],
        
    )
