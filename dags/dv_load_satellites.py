import logging
from dv_utils import (
    execute_sql_returning_count, get_conn,
    STG_SCHEMA, DV_SCHEMA, RECORD_SOURCE, GHOST_DATE,
)

logger = logging.getLogger(__name__)


def _close_and_insert_satellite(
    close_sql: str,
    insert_sql: str,
    sat_name: str,
):
    """
    Универсальный паттерн загрузки сателлита:
    1. Закрыть изменившиеся записи (UPDATE load_end_dts)
    2. Вставить новые/изменённые записи
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        # Закрытие старых версий
        cur.execute(close_sql)
        closed = cur.rowcount
        logger.info(f"{sat_name}: closed {closed} old records")

        # Вставка новых
        cur.execute(insert_sql)
        inserted = cur.rowcount
        logger.info(f"{sat_name}: inserted {inserted} new records")

        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"{sat_name} error: {e}")
        raise
    finally:
        conn.close()


def load_sat_user(**kwargs):
    hash_expr = """md5(
        COALESCE(s.segment, '') || '||' ||
        COALESCE(s.tariff, '') || '||' ||
        COALESCE(CAST(s.date_create AS TEXT), '') || '||' ||
        COALESCE(CAST(s.date_end AS TEXT), '')
    )"""

    close_sql = f"""
    UPDATE {DV_SCHEMA}.sat_user sat
    SET load_end_dts = now()
    WHERE sat.load_end_dts = '{GHOST_DATE}'
      AND EXISTS (
          SELECT 1
          FROM {STG_SCHEMA}.cd_user s
          WHERE md5(CAST(s.user_id AS TEXT)) = sat.hub_user_hk
            AND {hash_expr} <> sat.hash_diff
      )
    """

    insert_sql = f"""
    INSERT INTO {DV_SCHEMA}.sat_user (
        hub_user_hk, load_dts, load_end_dts,
        segment, tariff, date_create, date_end, is_active,
        hash_diff, record_source
    )
    SELECT
        md5(CAST(s.user_id AS TEXT))  AS hub_user_hk,
        now()                          AS load_dts,
        '{GHOST_DATE}'::timestamp      AS load_end_dts,
        s.segment,
        s.tariff,
        s.date_create,
        s.date_end,
        CASE WHEN s.date_end IS NULL THEN TRUE ELSE FALSE END  AS is_active,
        {hash_expr}                    AS hash_diff,
        '{RECORD_SOURCE}'              AS record_source
    FROM {STG_SCHEMA}.cd_user s
    WHERE NOT EXISTS (
        SELECT 1
        FROM {DV_SCHEMA}.sat_user sat
        WHERE sat.hub_user_hk = md5(CAST(s.user_id AS TEXT))
          AND sat.load_end_dts = '{GHOST_DATE}'
          AND sat.hash_diff = {hash_expr}
    )
    """

    _close_and_insert_satellite(close_sql, insert_sql, "sat_user")



def load_sat_banner(**kwargs):
    """SAT_BANNER из stg_ceh.cd_banner."""
    hash_expr = """md5(
        COALESCE(s.creative_type, '') || '||' ||
        COALESCE(s.message, '') || '||' ||
        COALESCE(s.size, '') || '||' ||
        COALESCE(s.target_audience_segment, '')
    )"""

    close_sql = f"""
    UPDATE {DV_SCHEMA}.sat_banner sat
    SET load_end_dts = now()
    WHERE sat.load_end_dts = '{GHOST_DATE}'
      AND EXISTS (
          SELECT 1
          FROM {STG_SCHEMA}.cd_banner s
          WHERE md5(CAST(s.banner_id AS TEXT)) = sat.hub_banner_hk
            AND {hash_expr} != sat.hash_diff
      )
    """

    insert_sql = f"""
    INSERT INTO {DV_SCHEMA}.sat_banner (
        hub_banner_hk, load_dts, load_end_dts,
        creative_type, message, size, target_audience_segment,
        hash_diff, record_source
    )
    SELECT
        md5(CAST(s.banner_id AS TEXT))  AS hub_banner_hk,
        now()                           AS load_dts,
        '{GHOST_DATE}'::timestamp       AS load_end_dts,
        s.creative_type,
        s.message,
        s.size,
        s.target_audience_segment,
        {hash_expr}                     AS hash_diff,
        '{RECORD_SOURCE}'               AS record_source
    FROM {STG_SCHEMA}.cd_banner s
    WHERE NOT EXISTS (
        SELECT 1
        FROM {DV_SCHEMA}.sat_banner sat
        WHERE sat.hub_banner_hk = md5(CAST(s.banner_id AS TEXT))
          AND sat.load_end_dts = '{GHOST_DATE}'
          AND sat.hash_diff = {hash_expr}
    )
    """

    _close_and_insert_satellite(close_sql, insert_sql, "sat_banner")


def load_sat_campaign(**kwargs):
    """SAT_CAMPAIGN из stg_ceh.cd_campaign."""
    hash_expr = """md5(
        COALESCE(CAST(s.daily_budget AS TEXT), '') || '||' ||
        COALESCE(CAST(s.start_date AS TEXT), '') || '||' ||
        COALESCE(CAST(s.end_date AS TEXT), '')
    )"""

    close_sql = f"""
    UPDATE {DV_SCHEMA}.sat_campaign sat
    SET load_end_dts = now()
    WHERE sat.load_end_dts = '{GHOST_DATE}'
      AND EXISTS (
          SELECT 1
          FROM {STG_SCHEMA}.cd_campaign s
          WHERE md5(CAST(s.campaign_id AS TEXT)) = sat.hub_campaign_hk
            AND {hash_expr} != sat.hash_diff
      )
    """

    insert_sql = f"""
    INSERT INTO {DV_SCHEMA}.sat_campaign (
        hub_campaign_hk, load_dts, load_end_dts,
        daily_budget, total_budget, start_date, end_date, status,
        hash_diff, record_source
    )
    SELECT
        md5(CAST(s.campaign_id AS TEXT))  AS hub_campaign_hk,
        now()                             AS load_dts,
        '{GHOST_DATE}'::timestamp         AS load_end_dts,
        s.daily_budget,
        s.daily_budget * (s.end_date - s.start_date + 1)  AS total_budget,
        s.start_date,
        s.end_date,
        CASE
            WHEN now()::date BETWEEN s.start_date AND s.end_date THEN 'active'
            WHEN now()::date > s.end_date THEN 'completed'
            ELSE 'draft'
        END AS status,
        {hash_expr}        AS hash_diff,
        '{RECORD_SOURCE}'  AS record_source
    FROM {STG_SCHEMA}.cd_campaign s
    WHERE NOT EXISTS (
        SELECT 1
        FROM {DV_SCHEMA}.sat_campaign sat
        WHERE sat.hub_campaign_hk = md5(CAST(s.campaign_id AS TEXT))
          AND sat.load_end_dts = '{GHOST_DATE}'
          AND sat.hash_diff = {hash_expr}
    )
    """

    _close_and_insert_satellite(close_sql, insert_sql, "sat_campaign")


def load_sat_show_detail(**kwargs):
    hash_expr = """md5(
        COALESCE(bs.placement, '') || '||' ||
        COALESCE(bs.device_type, '') || '||' ||
        COALESCE(bs.os, '') || '||' ||
        CAST(bs.is_clicked AS TEXT)
    )"""

    lnk_hk_expr = """md5(
        CAST(bs.banner_id AS TEXT) || '||' ||
        CAST(bs.campaign_id AS TEXT) || '||' ||
        CAST(bs.user_id AS TEXT) || '||' ||
        bs.geo || '||' ||
        CAST(bs.timestamp AS TEXT)
    )"""

    sql = f"""
    INSERT INTO {DV_SCHEMA}.sat_show_detail (
        lnk_show_hk, load_dts,
        placement, device_type, os, is_clicked,
        hash_diff, record_source
    )
    SELECT
        {lnk_hk_expr}                                AS lnk_show_hk,
        now()                                        AS load_dts,
        bs.placement,
        bs.device_type,
        bs.os,
        CASE WHEN bs.is_clicked = 1 THEN TRUE ELSE FALSE END AS is_clicked,
        {hash_expr}                                  AS hash_diff,
        '{RECORD_SOURCE}'                            AS record_source
    FROM {STG_SCHEMA}.fct_banners_show bs
    ON CONFLICT (lnk_show_hk, load_dts) DO NOTHING
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"sat_show_detail: inserted {count} new records")



def load_sat_install_detail(**kwargs):
    lnk_hk_expr = """md5(
        CAST(i.user_id AS TEXT) || '||' ||
        CAST(i.install_timestamp AS TEXT)
    )"""

    hash_expr = "md5(COALESCE(i.source, ''))"

    sql = f"""
    INSERT INTO {DV_SCHEMA}.sat_install_detail (
        lnk_install_hk, load_dts,
        source, hash_diff, record_source
    )
    SELECT
        {lnk_hk_expr}          AS lnk_install_hk,
        now()                  AS load_dts,
        i.source,
        {hash_expr}            AS hash_diff,
        '{RECORD_SOURCE}'      AS record_source
    FROM {STG_SCHEMA}.installs i
    ON CONFLICT (lnk_install_hk, load_dts) DO NOTHING
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"sat_install_detail: inserted {count} new records")


def load_sat_action_detail(**kwargs):
    lnk_hk_expr = """md5(
        CAST(fa.user_id AS TEXT) || '||' ||
        CAST(fa.session_start AS TEXT)
    )"""

    hash_expr = """md5(
        COALESCE(fa.actions, '')
    )"""

    sql = f"""
    INSERT INTO {DV_SCHEMA}.sat_action_detail (
        lnk_action_hk, load_dts,
        action_type, action_detail,
        hash_diff, record_source
    )
    SELECT
        {lnk_hk_expr}          AS lnk_action_hk,
        now()                  AS load_dts,
        fa.actions             AS action_type,
        NULL                   AS action_detail,
        {hash_expr}            AS hash_diff,
        '{RECORD_SOURCE}'      AS record_source
    FROM {STG_SCHEMA}.fct_actions fa
    ON CONFLICT (lnk_action_hk, load_dts) DO NOTHING
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"sat_action_detail: inserted {count} new records")

