from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain

import pandas as pd


POSTGRES_CONN_ID = "postgres_default"


def stg_to_cdl_fct_banners_show(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg.get_sqlalchemy_engine()

    # читаем всё из stg
    df = pd.read_sql(
        """
        SELECT
            banner_id,
            campaign_id,
            user_id,
            "timestamp",
            placement,
            device_type,
            os,
            geo,
            is_clicked,
            load_ts
        FROM stg_ceh.fct_banners_show
        """,
        engine,
    )

    # выкидываем строки, где нет ключевых полей (по сути PK/NOT NULL для CDL)
    df = df.dropna(
        subset=["banner_id", "campaign_id", "user_id", "timestamp", "geo", "os"]
    )

    with engine.begin() as conn:
        conn.execute('TRUNCATE TABLE cdl_ceh.fct_banners_show')
        df.to_sql(
            "fct_banners_show",
            conn,
            schema="cdl_ceh",
            if_exists="append",
            index=False,
        )


def stg_to_cdl_fct_actions(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg.get_sqlalchemy_engine()

    df = pd.read_sql(
        """
        SELECT
            user_id,
            session_start,
            actions,
            load_ts
        FROM stg_ceh.fct_actions
        """,
        engine,
    )

    df = df.dropna(subset=["user_id", "session_start"])

    with engine.begin() as conn:
        conn.execute('TRUNCATE TABLE cdl_ceh.fct_actions')
        df.to_sql(
            "fct_actions",
            conn,
            schema="cdl_ceh",
            if_exists="append",
            index=False,
        )


def stg_to_cdl_cd_banner(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg.get_sqlalchemy_engine()

    df = pd.read_sql(
        """
        SELECT
            banner_id,
            creative_type,
            message,
            "size",
            target_audience_segment,
            load_ts
        FROM stg_ceh.cd_banner
        """,
        engine,
    )

    df = df.dropna(subset=["banner_id"])

    with engine.begin() as conn:
        conn.execute('TRUNCATE TABLE cdl_ceh.cd_banner')
        df.to_sql(
            "cd_banner",
            conn,
            schema="cdl_ceh",
            if_exists="append",
            index=False,
        )


def stg_to_cdl_cd_user(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg.get_sqlalchemy_engine()

    df = pd.read_sql(
        """
        SELECT
            user_id,
            segment,
            tariff,
            date_create,
            date_end,
            load_ts
        FROM stg_ceh.cd_user
        """,
        engine,
    )

    df = df.dropna(subset=["user_id"])

    with engine.begin() as conn:
        conn.execute('TRUNCATE TABLE cdl_ceh.cd_user')
        df.to_sql(
            "cd_user",
            conn,
            schema="cdl_ceh",
            if_exists="append",
            index=False,
        )


def stg_to_cdl_cd_campaign(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg.get_sqlalchemy_engine()

    df = pd.read_sql(
        """
        SELECT
            campaign_id,
            daily_budget,
            start_date,
            end_date,
            load_ts
        FROM stg_ceh.cd_campaign
        """,
        engine,
    )

    df = df.dropna(subset=["campaign_id", "daily_budget", "start_date", "end_date"])

    with engine.begin() as conn:
        conn.execute('TRUNCATE TABLE cdl_ceh.cd_campaign')
        df.to_sql(
            "cd_campaign",
            conn,
            schema="cdl_ceh",
            if_exists="append",
            index=False,
        )


def stg_to_cdl_installs(**context):
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg.get_sqlalchemy_engine()

    df = pd.read_sql(
        """
        SELECT
            user_id,
            install_timestamp,
            "source",
            load_ts
        FROM stg_ceh.installs
        """,
        engine,
    )

    df = df.dropna(subset=["user_id", "install_timestamp", "source"])

    with engine.begin() as conn:
        conn.execute('TRUNCATE TABLE cdl_ceh.installs')
        df.to_sql(
            "installs",
            conn,
            schema="cdl_ceh",
            if_exists="append",
            index=False,
        )


with DAG(
    dag_id="dv_stg_to_cdl_ceh",
    start_date=datetime(2025, 2, 1),
    schedule_interval=None,
    catchup=False,
    tags=["stg", "cdl", "postgres"],
) as dag:

    fct_banners_show = PythonOperator(
        task_id="stg_to_cdl_fct_banners_show",
        python_callable=stg_to_cdl_fct_banners_show,
    )

    fct_actions = PythonOperator(
        task_id="stg_to_cdl_fct_actions",
        python_callable=stg_to_cdl_fct_actions,
    )

    cd_banner = PythonOperator(
        task_id="stg_to_cdl_cd_banner",
        python_callable=stg_to_cdl_cd_banner,
    )

    cd_user = PythonOperator(
        task_id="stg_to_cdl_cd_user",
        python_callable=stg_to_cdl_cd_user,
    )

    cd_campaign = PythonOperator(
        task_id="stg_to_cdl_cd_campaign",
        python_callable=stg_to_cdl_cd_campaign,
    )

    installs = PythonOperator(
        task_id="stg_to_cdl_installs",
        python_callable=stg_to_cdl_installs,
    )

    # можешь запустить параллельно, либо выстроить порядок как нужно
    chain(cd_banner, cd_user, cd_campaign, fct_banners_show, fct_actions, installs)
