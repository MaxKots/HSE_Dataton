# dv_load_links.py

import logging
from dv_utils import execute_sql_returning_count, STG_SCHEMA, DV_SCHEMA, RECORD_SOURCE

logger = logging.getLogger(__name__)


def load_lnk_show(**kwargs):
    """
    LNK_SHOW из stg_ceh.fct_banners_show.
    Связывает hub_banner, hub_campaign, hub_user, hub_geo.
    """
    sql = f"""
    INSERT INTO {DV_SCHEMA}.lnk_show (
        lnk_show_hk, hub_banner_hk, hub_campaign_hk,
        hub_user_hk, hub_geo_hk, event_timestamp,
        load_dts, record_source
    )
    SELECT
        md5(
            CAST(bs.banner_id AS TEXT) || '||' ||
            CAST(bs.campaign_id AS TEXT) || '||' ||
            CAST(bs.user_id AS TEXT) || '||' ||
            bs.geo || '||' ||
            CAST(bs.timestamp AS TEXT)
        ) AS lnk_show_hk,
        md5(CAST(bs.banner_id AS TEXT))    AS hub_banner_hk,
        md5(CAST(bs.campaign_id AS TEXT))  AS hub_campaign_hk,
        md5(CAST(bs.user_id AS TEXT))      AS hub_user_hk,
        md5('Россия' || '||' || bs.geo || '||' || bs.geo)  AS hub_geo_hk,
        bs.timestamp                        AS event_timestamp,
        now()                               AS load_dts,
        '{RECORD_SOURCE}'                   AS record_source
    FROM {STG_SCHEMA}.fct_banners_show bs
    ON CONFLICT (lnk_show_hk) DO NOTHING
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"lnk_show: inserted {count} new records")


def load_lnk_install(**kwargs):
    """
    LNK_INSTALL из stg_ceh.installs.
    hub_banner_hk = NULL, так как в installs нет banner_id.
    """
    sql = f"""
    INSERT INTO {DV_SCHEMA}.lnk_install (
        lnk_install_hk, hub_user_hk, hub_banner_hk,
        install_timestamp, load_dts, record_source
    )
    SELECT
        md5(
            CAST(i.user_id AS TEXT) || '||' ||
            CAST(i.install_timestamp AS TEXT)
        ) AS lnk_install_hk,
        md5(CAST(i.user_id AS TEXT))  AS hub_user_hk,
        NULL                          AS hub_banner_hk,
        i.install_timestamp,
        now()                         AS load_dts,
        '{RECORD_SOURCE}'             AS record_source
    FROM {STG_SCHEMA}.installs i
    ON CONFLICT (lnk_install_hk) DO NOTHING
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"lnk_install: inserted {count} new records")


def load_lnk_action(**kwargs):
    """
    LNK_ACTION из stg_ceh.fct_actions.
    Связывает hub_user и hub_session.
    """
    sql = f"""
    INSERT INTO {DV_SCHEMA}.lnk_action (
        lnk_action_hk, hub_user_hk, hub_session_hk,
        load_dts, record_source
    )
    SELECT
        md5(
            CAST(fa.user_id AS TEXT) || '||' ||
            CAST(fa.session_start AS TEXT)
        ) AS lnk_action_hk,
        md5(CAST(fa.user_id AS TEXT))  AS hub_user_hk,
        md5(
            CAST(fa.user_id AS TEXT) || '||' ||
            CAST(fa.session_start AS TEXT)
        ) AS hub_session_hk,
        now()              AS load_dts,
        '{RECORD_SOURCE}'  AS record_source
    FROM {STG_SCHEMA}.fct_actions fa
    ON CONFLICT (lnk_action_hk) DO NOTHING
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"lnk_action: inserted {count} new records")


def load_lnk_banner_campaign(**kwargs):
    """
    LNK_BANNER_CAMPAIGN из stg_ceh.fct_banners_show.
    Уникальные пары (banner_id, campaign_id).
    """
    sql = f"""
    INSERT INTO {DV_SCHEMA}.lnk_banner_campaign (
        lnk_banner_campaign_hk, hub_banner_hk, hub_campaign_hk,
        load_dts, record_source
    )
    SELECT
        md5(
            CAST(bc.banner_id AS TEXT) || '||' ||
            CAST(bc.campaign_id AS TEXT)
        ) AS lnk_banner_campaign_hk,
        md5(CAST(bc.banner_id AS TEXT))    AS hub_banner_hk,
        md5(CAST(bc.campaign_id AS TEXT))  AS hub_campaign_hk,
        now()              AS load_dts,
        '{RECORD_SOURCE}'  AS record_source
    FROM (
        SELECT DISTINCT banner_id, campaign_id
        FROM {STG_SCHEMA}.fct_banners_show
        WHERE banner_id IS NOT NULL
          AND campaign_id IS NOT NULL
    ) bc
    ON CONFLICT (lnk_banner_campaign_hk) DO NOTHING
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"lnk_banner_campaign: inserted {count} new records")

