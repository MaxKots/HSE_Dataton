from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from dv_load_hubs import (
    load_hub_user,
    load_hub_banner,
    load_hub_campaign,
    load_hub_geo,
    load_hub_session,
)
from dv_load_links import (
    load_lnk_show,
    load_lnk_install,
    load_lnk_action,
    load_lnk_banner_campaign,
)
from dv_load_satellites import (
    load_sat_user,
    load_sat_banner,
    load_sat_campaign,
    load_sat_show_detail,
    load_sat_install_detail,
    load_sat_action_detail,
)
from dv_load_business_vault import (
    load_sat_eff_banner_campaign,
    load_bsat_show_metrics,
)
from dv_quality_checks import run_dv_quality_checks


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["de-team@company.ru"],
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=60),
}


with DAG(
    dag_id="dv_ceh_load",
    default_args=default_args,
    description="Load Data Vault 2.0 from stg_ceh staging tables",
    schedule_interval="0 3 * * *",  # после stg_ceh_csv_load в 02:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-vault", "ceh", "dwh"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── STAGE 1: HUBs (параллельно) ─────────────────
    t_hub_user = PythonOperator(
        task_id="load_hub_user",
        python_callable=load_hub_user,
    )
    t_hub_banner = PythonOperator(
        task_id="load_hub_banner",
        python_callable=load_hub_banner,
    )
    t_hub_campaign = PythonOperator(
        task_id="load_hub_campaign",
        python_callable=load_hub_campaign,
    )
    t_hub_geo = PythonOperator(
        task_id="load_hub_geo",
        python_callable=load_hub_geo,
    )
    t_hub_session = PythonOperator(
        task_id="load_hub_session",
        python_callable=load_hub_session,
    )

    hubs_done = EmptyOperator(task_id="hubs_done")

    # ── STAGE 2: LINKs (после хабов) ────────────────
    t_lnk_show = PythonOperator(
        task_id="load_lnk_show",
        python_callable=load_lnk_show,
    )
    t_lnk_install = PythonOperator(
        task_id="load_lnk_install",
        python_callable=load_lnk_install,
    )
    t_lnk_action = PythonOperator(
        task_id="load_lnk_action",
        python_callable=load_lnk_action,
    )
    t_lnk_bc = PythonOperator(
        task_id="load_lnk_banner_campaign",
        python_callable=load_lnk_banner_campaign,
    )

    links_done = EmptyOperator(task_id="links_done")

    # ── STAGE 3: SATELLITEs (после хабов и линков) ──
    t_sat_user = PythonOperator(
        task_id="load_sat_user",
        python_callable=load_sat_user,
    )
    t_sat_banner = PythonOperator(
        task_id="load_sat_banner",
        python_callable=load_sat_banner,
    )
    t_sat_campaign = PythonOperator(
        task_id="load_sat_campaign",
        python_callable=load_sat_campaign,
    )
    t_sat_show = PythonOperator(
        task_id="load_sat_show_detail",
        python_callable=load_sat_show_detail,
    )
    t_sat_install = PythonOperator(
        task_id="load_sat_install_detail",
        python_callable=load_sat_install_detail,
    )
    t_sat_action = PythonOperator(
        task_id="load_sat_action_detail",
        python_callable=load_sat_action_detail,
    )

    sats_done = EmptyOperator(task_id="sats_done")

    # ── STAGE 4: Business Vault ──────────────────────
    t_sat_eff = PythonOperator(
        task_id="load_sat_eff_banner_campaign",
        python_callable=load_sat_eff_banner_campaign,
    )
    t_bsat_metrics = PythonOperator(
        task_id="load_bsat_show_metrics",
        python_callable=load_bsat_show_metrics,
    )

    bv_done = EmptyOperator(task_id="bv_done")

    # ── STAGE 5: Quality Checks ──────────────────────
    t_qc = PythonOperator(
        task_id="quality_checks",
        python_callable=run_dv_quality_checks,
    )

    end = EmptyOperator(task_id="end")

    # ── DEPENDENCIES ─────────────────────────────────

    # Stage 1: все хабы параллельно
    start >> [t_hub_user, t_hub_banner, t_hub_campaign, t_hub_geo, t_hub_session]
    [t_hub_user, t_hub_banner, t_hub_campaign, t_hub_geo, t_hub_session] >> hubs_done

    # Stage 2: линки после всех хабов, параллельно между собой
    hubs_done >> [t_lnk_show, t_lnk_install, t_lnk_action, t_lnk_bc]
    [t_lnk_show, t_lnk_install, t_lnk_action, t_lnk_bc] >> links_done

    # Stage 3: сателлиты на хабы — после хабов, сателлиты на линки — после линков
    # Для упрощения — все после links_done
    links_done >> [t_sat_user, t_sat_banner, t_sat_campaign]
    links_done >> [t_sat_show, t_sat_install, t_sat_action]
    [t_sat_user, t_sat_banner, t_sat_campaign,
     t_sat_show, t_sat_install, t_sat_action] >> sats_done

    # Stage 4: business vault после всех сателлитов
    sats_done >> [t_sat_eff, t_bsat_metrics]
    [t_sat_eff, t_bsat_metrics] >> bv_done

    # Stage 5: QC
    bv_done >> t_qc >> end
