import logging
from dv_utils import (
    execute_sql_returning_count, get_conn,
    STG_SCHEMA, DV_SCHEMA, RECORD_SOURCE, GHOST_DATE,
)

logger = logging.getLogger(__name__)


def load_sat_eff_banner_campaign(**kwargs):
    """
    SAT_EFF_BANNER_CAMPAIGN — effectivity satellite.
    Определяет, активна ли связь banner-campaign на текущую дату.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()

        # Закрытие старых записей
        cur.execute(f"""
            UPDATE {DV_SCHEMA}.sat_eff_banner_campaign eff
            SET load_end_dts = now()
            WHERE eff.load_end_dts = '{GHOST_DATE}'
        """)
        closed = cur.rowcount
        logger.info(f"sat_eff_banner_campaign: closed {closed} old records")

        # Вставка актуального состояния
        cur.execute(f"""
            INSERT INTO {DV_SCHEMA}.sat_eff_banner_campaign (
                lnk_banner_campaign_hk, load_dts, load_end_dts,
                is_effective, record_source
            )
            SELECT
                lbc.lnk_banner_campaign_hk,
                now()                      AS load_dts,
                '{GHOST_DATE}'::timestamp  AS load_end_dts,
                CASE
                    WHEN sc.status = 'active'
                         AND now()::date BETWEEN sc.start_date AND sc.end_date
                    THEN TRUE
                    ELSE FALSE
                END AS is_effective,
                'calculated.effectivity'   AS record_source
            FROM {DV_SCHEMA}.lnk_banner_campaign lbc
            JOIN {DV_SCHEMA}.hub_campaign hc
                ON lbc.hub_campaign_hk = hc.hub_campaign_hk
            LEFT JOIN {DV_SCHEMA}.sat_campaign sc
                ON sc.hub_campaign_hk = hc.hub_campaign_hk
                AND sc.load_end_dts = '{GHOST_DATE}'
        """)
        inserted = cur.rowcount
        logger.info(f"sat_eff_banner_campaign: inserted {inserted} new records")

        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"sat_eff_banner_campaign error: {e}")
        raise
    finally:
        conn.close()


def load_bsat_show_metrics(**kwargs):
    """
    BSAT_SHOW_METRICS — вычисляемые метрики CPM, CPC, CTR.

    CPM = (daily_budget / impressions_per_day) * 1000
    CPC = daily_budget / clicks_per_day (если clicks > 0)
    CTR = clicks / impressions
    """
    sql = f"""
    INSERT INTO {DV_SCHEMA}.bsat_show_metrics (
        lnk_show_hk, load_dts,
        cpm_calculated, cpc_calculated, ctr,
        record_source
    )
    SELECT
        ls.lnk_show_hk,
        now()  AS load_dts,

        -- CPM = (budget / impressions) * 1000
        CASE
            WHEN agg.impressions > 0
            THEN (sc.daily_budget / agg.impressions) * 1000
            ELSE 0
        END AS cpm_calculated,

        -- CPC = budget / clicks
        CASE
            WHEN agg.clicks > 0
            THEN sc.daily_budget / agg.clicks
            ELSE 0
        END AS cpc_calculated,

        -- CTR = clicks / impressions
        CASE
            WHEN agg.impressions > 0
            THEN CAST(agg.clicks AS DECIMAL) / agg.impressions
            ELSE 0
        END AS ctr,

        'calculated.bsat_show_metrics'  AS record_source

    FROM {DV_SCHEMA}.lnk_show ls

    -- Получаем campaign для бюджета
    JOIN {DV_SCHEMA}.sat_campaign sc
        ON sc.hub_campaign_hk = ls.hub_campaign_hk
        AND sc.load_end_dts = '{GHOST_DATE}'

    -- Агрегация по кампании + дате
    JOIN (
        SELECT
            ls2.hub_campaign_hk,
            ls2.event_timestamp::date AS event_date,
            COUNT(*)                  AS impressions,
            SUM(CASE WHEN sd.is_clicked THEN 1 ELSE 0 END) AS clicks
        FROM {DV_SCHEMA}.lnk_show ls2
        JOIN {DV_SCHEMA}.sat_show_detail sd
            ON sd.lnk_show_hk = ls2.lnk_show_hk
        GROUP BY ls2.hub_campaign_hk, ls2.event_timestamp::date
    ) agg
        ON agg.hub_campaign_hk = ls.hub_campaign_hk
        AND agg.event_date = ls.event_timestamp::date

    -- Не загружать повторно
    WHERE NOT EXISTS (
        SELECT 1
        FROM {DV_SCHEMA}.bsat_show_metrics bsm
        WHERE bsm.lnk_show_hk = ls.lnk_show_hk
    )
    """
    count = execute_sql_returning_count(sql)
    logger.info(f"bsat_show_metrics: inserted {count} new records")
