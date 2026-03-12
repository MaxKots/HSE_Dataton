import logging
from dv_utils import execute_sql, DV_SCHEMA

logger = logging.getLogger(__name__)


def run_dv_quality_checks(**kwargs):
    """Проверки качества после загрузки Data Vault."""

    checks = {
        # Row counts
        "hub_user":               f"SELECT COUNT(*) FROM {DV_SCHEMA}.hub_user",
        "hub_banner":             f"SELECT COUNT(*) FROM {DV_SCHEMA}.hub_banner",
        "hub_campaign":           f"SELECT COUNT(*) FROM {DV_SCHEMA}.hub_campaign",
        "hub_geo":                f"SELECT COUNT(*) FROM {DV_SCHEMA}.hub_geo",
        "hub_session":            f"SELECT COUNT(*) FROM {DV_SCHEMA}.hub_session",
        "lnk_show":               f"SELECT COUNT(*) FROM {DV_SCHEMA}.lnk_show",
        "lnk_install":            f"SELECT COUNT(*) FROM {DV_SCHEMA}.lnk_install",
        "lnk_action":             f"SELECT COUNT(*) FROM {DV_SCHEMA}.lnk_action",
        "lnk_banner_campaign":    f"SELECT COUNT(*) FROM {DV_SCHEMA}.lnk_banner_campaign",
        "sat_user":               f"SELECT COUNT(*) FROM {DV_SCHEMA}.sat_user",
        "sat_banner":             f"SELECT COUNT(*) FROM {DV_SCHEMA}.sat_banner",
        "sat_campaign":           f"SELECT COUNT(*) FROM {DV_SCHEMA}.sat_campaign",
        "sat_show_detail":        f"SELECT COUNT(*) FROM {DV_SCHEMA}.sat_show_detail",
        "sat_install_detail":     f"SELECT COUNT(*) FROM {DV_SCHEMA}.sat_install_detail",
        "sat_action_detail":      f"SELECT COUNT(*) FROM {DV_SCHEMA}.sat_action_detail",
        "sat_eff_banner_campaign": f"SELECT COUNT(*) FROM {DV_SCHEMA}.sat_eff_banner_campaign",
        "bsat_show_metrics":      f"SELECT COUNT(*) FROM {DV_SCHEMA}.bsat_show_metrics",
    }

    logger.info("=" * 60)
    logger.info("Data Vault Quality Checks")
    logger.info("=" * 60)

    has_issues = False
    for name, sql in checks.items():
        rows = execute_sql(sql, fetch=True)
        count = rows[0][0]
        logger.info(f"  {name}: {count} rows")
        if count == 0:
            logger.warning(f"  WARNING: {name} is EMPTY!")
            has_issues = True

    # Orphan checks: линки ссылаются на несуществующие хабы
    orphan_checks = [
        (
            "lnk_show -> hub_user orphans",
            f"""
            SELECT COUNT(*) FROM {DV_SCHEMA}.lnk_show l
            LEFT JOIN {DV_SCHEMA}.hub_user h ON l.hub_user_hk = h.hub_user_hk
            WHERE h.hub_user_hk IS NULL
            """
        ),
        (
            "lnk_show -> hub_banner orphans",
            f"""
            SELECT COUNT(*) FROM {DV_SCHEMA}.lnk_show l
            LEFT JOIN {DV_SCHEMA}.hub_banner h ON l.hub_banner_hk = h.hub_banner_hk
            WHERE h.hub_banner_hk IS NULL
            """
        ),
        (
            "sat_user -> hub_user orphans",
            f"""
            SELECT COUNT(*) FROM {DV_SCHEMA}.sat_user s
            LEFT JOIN {DV_SCHEMA}.hub_user h ON s.hub_user_hk = h.hub_user_hk
            WHERE h.hub_user_hk IS NULL
            """
        ),
        (
            "sat_show_detail -> lnk_show orphans",
            f"""
            SELECT COUNT(*) FROM {DV_SCHEMA}.sat_show_detail s
            LEFT JOIN {DV_SCHEMA}.lnk_show l ON s.lnk_show_hk = l.lnk_show_hk
            WHERE l.lnk_show_hk IS NULL
            """
        ),
    ]

    for name, sql in orphan_checks:
        rows = execute_sql(sql, fetch=True)
        orphans = rows[0][0]
        if orphans > 0:
            logger.warning(f"  ORPHAN CHECK '{name}': {orphans} orphan records!")
            has_issues = True
        else:
            logger.info(f"  ORPHAN CHECK '{name}': OK")

    # Проверка активных записей в сателлитах
    active_checks = [
        ("sat_user active",    f"SELECT COUNT(*) FROM {DV_SCHEMA}.sat_user WHERE load_end_dts = '9999-12-31'"),
        ("sat_banner active",  f"SELECT COUNT(*) FROM {DV_SCHEMA}.sat_banner WHERE load_end_dts = '9999-12-31'"),
        ("sat_campaign active", f"SELECT COUNT(*) FROM {DV_SCHEMA}.sat_campaign WHERE load_end_dts = '9999-12-31'"),
    ]

    for name, sql in active_checks:
        rows = execute_sql(sql, fetch=True)
        count = rows[0][0]
        logger.info(f"  {name}: {count} current records")

    if has_issues:
        logger.warning("Quality checks completed WITH WARNINGS")
    else:
        logger.info("All quality checks PASSED")
