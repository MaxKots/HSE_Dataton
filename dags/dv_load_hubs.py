import logging
from dv_utils import execute_sql_returning_count, STG_SCHEMA, DV_SCHEMA, RECORD_SOURCE

logger = logging.getLogger(__name__)


def load_hub_user(**kwargs):
    """
    HUB_USER из stg_ceh.cd_user + stg_ceh.fct_banners_show + stg_ceh.fct_actions + stg_ceh.installs
    Собираем все уникальные user_id из всех источников.
    """
    sql = f"""
    INSERT INTO {DV_SCHEMA}.hub_user (hub_user_hk, user_id, load_dts, record_source)
    SELECT
        md5(CAST(u.user_id AS TEXT))  AS hub_user_hk,
        u.user_id,
        now()                         AS load_dts,
        '{RECORD_SOURCE}'             AS record_source
    FROM (
        SELECT user_id FROM {STG_SCHEMA}.cd_user
        UNION
        SELECT user_id FROM {STG_SCHEMA}.fct_banners_show
        UNION
        SELECT user_id FROM {STG_SCHEMA}.fct_actions
        UNION
        SELECT user_id FROM {STG_SCHEMA}.installs
    ) u
    WHERE NOT EXISTS (
        SELECT 1 FROM {DV_SCHEMA}.hub_user h
        WHERE h.hub_user_hk = md5(CAST(u.user_id AS TEXT))
    )
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"hub_user: inserted {count} new records")


def load_hub_banner(**kwargs):
    """
    HUB_BANNER из stg_ceh.cd_banner + stg_ceh.fct_banners_show
    """
    sql = f"""
    INSERT INTO {DV_SCHEMA}.hub_banner (hub_banner_hk, banner_id, load_dts, record_source)
    SELECT
        md5(CAST(b.banner_id AS TEXT))  AS hub_banner_hk,
        b.banner_id,
        now()                           AS load_dts,
        '{RECORD_SOURCE}'               AS record_source
    FROM (
        SELECT banner_id FROM {STG_SCHEMA}.cd_banner
        UNION
        SELECT banner_id FROM {STG_SCHEMA}.fct_banners_show
    ) b
    WHERE NOT EXISTS (
        SELECT 1 FROM {DV_SCHEMA}.hub_banner h
        WHERE h.hub_banner_hk = md5(CAST(b.banner_id AS TEXT))
    )
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"hub_banner: inserted {count} new records")


def load_hub_campaign(**kwargs):
    """
    HUB_CAMPAIGN из stg_ceh.cd_campaign + stg_ceh.fct_banners_show
    """
    sql = f"""
    INSERT INTO {DV_SCHEMA}.hub_campaign (hub_campaign_hk, campaign_id, load_dts, record_source)
    SELECT
        md5(CAST(c.campaign_id AS TEXT))  AS hub_campaign_hk,
        c.campaign_id,
        now()                             AS load_dts,
        '{RECORD_SOURCE}'                 AS record_source
    FROM (
        SELECT campaign_id FROM {STG_SCHEMA}.cd_campaign
        UNION
        SELECT campaign_id FROM {STG_SCHEMA}.fct_banners_show
    ) c
    WHERE NOT EXISTS (
        SELECT 1 FROM {DV_SCHEMA}.hub_campaign h
        WHERE h.hub_campaign_hk = md5(CAST(c.campaign_id AS TEXT))
    )
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"hub_campaign: inserted {count} new records")


def load_hub_geo(**kwargs):
    """
    HUB_GEO из stg_ceh.fct_banners_show.
    geo — это город. country и region ставим дефолтами.
    """
    sql = f"""
    INSERT INTO {DV_SCHEMA}.hub_geo (hub_geo_hk, country, region, city, load_dts, record_source)
    SELECT
        md5('Россия' || '||' || bs.geo || '||' || bs.geo)  AS hub_geo_hk,
        'Россия'                                            AS country,
        bs.geo                                              AS region,
        bs.geo                                              AS city,
        now()                                               AS load_dts,
        '{RECORD_SOURCE}'                                   AS record_source
    FROM (
        SELECT DISTINCT geo FROM {STG_SCHEMA}.fct_banners_show
    ) bs
    WHERE NOT EXISTS (
        SELECT 1 FROM {DV_SCHEMA}.hub_geo h
        WHERE h.hub_geo_hk = md5('Россия' || '||' || bs.geo || '||' || bs.geo)
    )
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"hub_geo: inserted {count} new records")


def load_hub_session(**kwargs):
    """
    HUB_SESSION из stg_ceh.fct_actions.
    BK = (user_id, session_start)
    """
    sql = f"""
    INSERT INTO {DV_SCHEMA}.hub_session (hub_session_hk, user_id, session_start, load_dts, record_source)
    SELECT
        md5(CAST(fa.user_id AS TEXT) || '||' || CAST(fa.session_start AS TEXT))
            AS hub_session_hk,
        fa.user_id,
        fa.session_start,
        now()              AS load_dts,
        '{RECORD_SOURCE}'  AS record_source
    FROM (
        SELECT DISTINCT user_id, session_start FROM {STG_SCHEMA}.fct_actions
    ) fa
    WHERE NOT EXISTS (
        SELECT 1 FROM {DV_SCHEMA}.hub_session h
        WHERE h.hub_session_hk = md5(
            CAST(fa.user_id AS TEXT) || '||' || CAST(fa.session_start AS TEXT)
        )
    )
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"hub_session: inserted {count} new records")
