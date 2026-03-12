CREATE SCHEMA IF NOT EXISTS ceh;
CREATE SCHEMA IF NOT EXISTS stg_ceh;



SET search_path TO ceh;

-- ============================================================
-- HUBS
-- ============================================================

CREATE TABLE ceh.hub_user (
    hub_user_hk    CHAR(32)     NOT NULL,
    user_id        INT          NOT NULL,
    load_dts       TIMESTAMP    NOT NULL DEFAULT now(),
    record_source  VARCHAR(100) NOT NULL,

    CONSTRAINT pk_hub_user PRIMARY KEY (hub_user_hk)
);

CREATE UNIQUE INDEX uix_hub_user_bk ON ceh.hub_user (user_id);

-- -----------------------------------------------------------


CREATE TABLE ceh.hub_user (
	hub_user_hk bpchar(32) NOT NULL,
	user_id int4 NOT NULL,
	load_dts timestamp DEFAULT now() NOT NULL,
	record_source varchar(100) NOT NULL,
	CONSTRAINT pk_hub_user PRIMARY KEY (hub_user_hk)
);
CREATE UNIQUE INDEX uix_hub_user_bk ON ceh.hub_user USING btree (user_id);


CREATE TABLE ceh.hub_banner (
    hub_banner_hk  CHAR(32)     NOT NULL,
    banner_id      INT          NOT NULL,
    load_dts       TIMESTAMP    NOT NULL DEFAULT now(),
    record_source  VARCHAR(100) NOT NULL,

    CONSTRAINT pk_hub_banner PRIMARY KEY (hub_banner_hk)
);

CREATE UNIQUE INDEX uix_hub_banner_bk ON ceh.hub_banner (banner_id);

-- -----------------------------------------------------------

CREATE TABLE ceh.hub_campaign (
    hub_campaign_hk CHAR(32)     NOT NULL,
    campaign_id     INT          NOT NULL,
    load_dts        TIMESTAMP    NOT NULL DEFAULT now(),
    record_source   VARCHAR(100) NOT NULL,

    CONSTRAINT pk_hub_campaign PRIMARY KEY (hub_campaign_hk)
);

CREATE UNIQUE INDEX uix_hub_campaign_bk ON ceh.hub_campaign (campaign_id);

-- -----------------------------------------------------------

CREATE TABLE ceh.hub_geo (
    hub_geo_hk     CHAR(32)     NOT NULL,
    country        VARCHAR(255) NOT NULL,
    region         VARCHAR(255) NOT NULL,
    city           VARCHAR(255) NOT NULL,
    load_dts       TIMESTAMP    NOT NULL DEFAULT now(),
    record_source  VARCHAR(100) NOT NULL,

    CONSTRAINT pk_hub_geo PRIMARY KEY (hub_geo_hk)
);

CREATE UNIQUE INDEX uix_hub_geo_bk ON ceh.hub_geo (country, region, city);

-- -----------------------------------------------------------

CREATE TABLE ceh.hub_session (
    hub_session_hk CHAR(32)     NOT NULL,
    user_id        INT          NOT NULL,
    session_start  TIMESTAMP    NOT NULL,
    load_dts       TIMESTAMP    NOT NULL DEFAULT now(),
    record_source  VARCHAR(100) NOT NULL,

    CONSTRAINT pk_hub_session PRIMARY KEY (hub_session_hk)
);

CREATE UNIQUE INDEX uix_hub_session_bk ON ceh.hub_session (user_id, session_start);


-- ============================================================
-- LINKS
-- ============================================================

CREATE TABLE ceh.lnk_show (
    lnk_show_hk      CHAR(32)     NOT NULL,
    hub_banner_hk     CHAR(32)     NOT NULL,
    hub_campaign_hk   CHAR(32)     NOT NULL,
    hub_user_hk       CHAR(32)     NOT NULL,
    hub_geo_hk        CHAR(32)     NOT NULL,
    event_timestamp   TIMESTAMP    NOT NULL,
    load_dts          TIMESTAMP    NOT NULL DEFAULT now(),
    record_source     VARCHAR(100) NOT NULL,

    CONSTRAINT pk_lnk_show PRIMARY KEY (lnk_show_hk),

    CONSTRAINT fk_lnk_show_banner   FOREIGN KEY (hub_banner_hk)   REFERENCES ceh.hub_banner   (hub_banner_hk),
    CONSTRAINT fk_lnk_show_campaign FOREIGN KEY (hub_campaign_hk) REFERENCES ceh.hub_campaign (hub_campaign_hk),
    CONSTRAINT fk_lnk_show_user     FOREIGN KEY (hub_user_hk)     REFERENCES ceh.hub_user     (hub_user_hk),
    CONSTRAINT fk_lnk_show_geo      FOREIGN KEY (hub_geo_hk)      REFERENCES ceh.hub_geo      (hub_geo_hk)
);

CREATE INDEX ix_lnk_show_banner   ON ceh.lnk_show (hub_banner_hk);
CREATE INDEX ix_lnk_show_campaign ON ceh.lnk_show (hub_campaign_hk);
CREATE INDEX ix_lnk_show_user     ON ceh.lnk_show (hub_user_hk);
CREATE INDEX ix_lnk_show_geo      ON ceh.lnk_show (hub_geo_hk);

-- -----------------------------------------------------------

CREATE TABLE ceh.lnk_install (
    lnk_install_hk    CHAR(32)     NOT NULL,
    hub_user_hk        CHAR(32)     NOT NULL,
    hub_banner_hk      CHAR(32),
    install_timestamp  TIMESTAMP    NOT NULL,
    load_dts           TIMESTAMP    NOT NULL DEFAULT now(),
    record_source      VARCHAR(100) NOT NULL,

    CONSTRAINT pk_lnk_install PRIMARY KEY (lnk_install_hk),

    CONSTRAINT fk_lnk_install_user   FOREIGN KEY (hub_user_hk)   REFERENCES ceh.hub_user   (hub_user_hk),
    CONSTRAINT fk_lnk_install_banner FOREIGN KEY (hub_banner_hk) REFERENCES ceh.hub_banner (hub_banner_hk)
);

CREATE INDEX ix_lnk_install_user   ON ceh.lnk_install (hub_user_hk);
CREATE INDEX ix_lnk_install_banner ON ceh.lnk_install (hub_banner_hk);

-- -----------------------------------------------------------

CREATE TABLE ceh.lnk_action (
    lnk_action_hk   CHAR(32)     NOT NULL,
    hub_user_hk      CHAR(32)     NOT NULL,
    hub_session_hk   CHAR(32)     NOT NULL,
    load_dts         TIMESTAMP    NOT NULL DEFAULT now(),
    record_source    VARCHAR(100) NOT NULL,

    CONSTRAINT pk_lnk_action PRIMARY KEY (lnk_action_hk),

    CONSTRAINT fk_lnk_action_user    FOREIGN KEY (hub_user_hk)    REFERENCES ceh.hub_user    (hub_user_hk),
    CONSTRAINT fk_lnk_action_session FOREIGN KEY (hub_session_hk) REFERENCES ceh.hub_session (hub_session_hk)
);

CREATE INDEX ix_lnk_action_user    ON ceh.lnk_action (hub_user_hk);
CREATE INDEX ix_lnk_action_session ON ceh.lnk_action (hub_session_hk);

-- -----------------------------------------------------------

CREATE TABLE ceh.lnk_banner_campaign (
    lnk_banner_campaign_hk CHAR(32)     NOT NULL,
    hub_banner_hk           CHAR(32)     NOT NULL,
    hub_campaign_hk         CHAR(32)     NOT NULL,
    load_dts                TIMESTAMP    NOT NULL DEFAULT now(),
    record_source           VARCHAR(100) NOT NULL,

    CONSTRAINT pk_lnk_banner_campaign PRIMARY KEY (lnk_banner_campaign_hk),

    CONSTRAINT fk_lnk_bc_banner   FOREIGN KEY (hub_banner_hk)   REFERENCES ceh.hub_banner   (hub_banner_hk),
    CONSTRAINT fk_lnk_bc_campaign FOREIGN KEY (hub_campaign_hk) REFERENCES ceh.hub_campaign (hub_campaign_hk)
);

CREATE UNIQUE INDEX uix_lnk_banner_campaign_bk
    ON ceh.lnk_banner_campaign (hub_banner_hk, hub_campaign_hk);


-- ============================================================
-- SATELLITES
-- ============================================================

CREATE TABLE ceh.sat_user (
    hub_user_hk    CHAR(32)     NOT NULL,
    load_dts       TIMESTAMP    NOT NULL,
    load_end_dts   TIMESTAMP    NOT NULL DEFAULT '9999-12-31'::timestamp,
    segment        VARCHAR(100),
    tariff         VARCHAR(100),
    date_create    DATE,
    date_end       DATE,
    is_active      BOOLEAN      NOT NULL DEFAULT TRUE,
    hash_diff      CHAR(32)     NOT NULL,
    record_source  VARCHAR(100) NOT NULL,

    CONSTRAINT pk_sat_user PRIMARY KEY (hub_user_hk, load_dts),
    CONSTRAINT fk_sat_user_hub FOREIGN KEY (hub_user_hk) REFERENCES ceh.hub_user (hub_user_hk)
);

CREATE INDEX ix_sat_user_current ON ceh.sat_user (hub_user_hk)
    WHERE load_end_dts = '9999-12-31';

-- -----------------------------------------------------------

CREATE TABLE ceh.sat_banner (
    hub_banner_hk            CHAR(32)     NOT NULL,
    load_dts                 TIMESTAMP    NOT NULL,
    load_end_dts             TIMESTAMP    NOT NULL DEFAULT '9999-12-31'::timestamp,
    creative_type            VARCHAR(50),
    message                  VARCHAR(500),
    size                     VARCHAR(20),
    target_audience_segment  VARCHAR(100),
    hash_diff                CHAR(32)     NOT NULL,
    record_source            VARCHAR(100) NOT NULL,

    CONSTRAINT pk_sat_banner PRIMARY KEY (hub_banner_hk, load_dts),
    CONSTRAINT fk_sat_banner_hub FOREIGN KEY (hub_banner_hk) REFERENCES ceh.hub_banner (hub_banner_hk)
);

CREATE INDEX ix_sat_banner_current ON ceh.sat_banner (hub_banner_hk)
    WHERE load_end_dts = '9999-12-31';

-- -----------------------------------------------------------

CREATE TABLE ceh.sat_campaign (
    hub_campaign_hk  CHAR(32)       NOT NULL,
    load_dts         TIMESTAMP      NOT NULL,
    load_end_dts     TIMESTAMP      NOT NULL DEFAULT '9999-12-31'::timestamp,
    daily_budget     DECIMAL(15,2),
    total_budget     DECIMAL(15,2),
    start_date       DATE,
    end_date         DATE,
    status           VARCHAR(20),
    hash_diff        CHAR(32)       NOT NULL,
    record_source    VARCHAR(100)   NOT NULL,

    CONSTRAINT pk_sat_campaign PRIMARY KEY (hub_campaign_hk, load_dts),
    CONSTRAINT fk_sat_campaign_hub FOREIGN KEY (hub_campaign_hk) REFERENCES ceh.hub_campaign (hub_campaign_hk)
);

CREATE INDEX ix_sat_campaign_current ON ceh.sat_campaign (hub_campaign_hk)
    WHERE load_end_dts = '9999-12-31';

-- -----------------------------------------------------------

CREATE TABLE ceh.sat_show_detail (
    lnk_show_hk    CHAR(32)     NOT NULL,
    load_dts        TIMESTAMP    NOT NULL,
    placement       VARCHAR(50),
    device_type     VARCHAR(50),
    os              VARCHAR(50),
    is_clicked      BOOLEAN      NOT NULL DEFAULT FALSE,
    hash_diff       CHAR(32)     NOT NULL,
    record_source   VARCHAR(100) NOT NULL,

    CONSTRAINT pk_sat_show_detail PRIMARY KEY (lnk_show_hk, load_dts),
    CONSTRAINT fk_sat_show_lnk FOREIGN KEY (lnk_show_hk) REFERENCES ceh.lnk_show (lnk_show_hk)
);

-- -----------------------------------------------------------

CREATE TABLE ceh.sat_install_detail (
    lnk_install_hk  CHAR(32)     NOT NULL,
    load_dts         TIMESTAMP    NOT NULL,
    source           VARCHAR(50),
    hash_diff        CHAR(32)     NOT NULL,
    record_source    VARCHAR(100) NOT NULL,

    CONSTRAINT pk_sat_install_detail PRIMARY KEY (lnk_install_hk, load_dts),
    CONSTRAINT fk_sat_install_lnk FOREIGN KEY (lnk_install_hk) REFERENCES ceh.lnk_install (lnk_install_hk)
);

-- -----------------------------------------------------------

CREATE TABLE ceh.sat_action_detail (
    lnk_action_hk   CHAR(32)     NOT NULL,
    load_dts         TIMESTAMP    NOT NULL,
    action_type      VARCHAR(100),
    action_detail    VARCHAR(500),
    hash_diff        CHAR(32)     NOT NULL,
    record_source    VARCHAR(100) NOT NULL,

    CONSTRAINT pk_sat_action_detail PRIMARY KEY (lnk_action_hk, load_dts),
    CONSTRAINT fk_sat_action_lnk FOREIGN KEY (lnk_action_hk) REFERENCES ceh.lnk_action (lnk_action_hk)
);


-- ============================================================
-- EFFECTIVITY SATELLITE
-- ============================================================

CREATE TABLE ceh.sat_eff_banner_campaign (
    lnk_banner_campaign_hk CHAR(32)     NOT NULL,
    load_dts                TIMESTAMP    NOT NULL,
    load_end_dts            TIMESTAMP    NOT NULL DEFAULT '9999-12-31'::timestamp,
    is_effective            BOOLEAN      NOT NULL DEFAULT TRUE,
    record_source           VARCHAR(100) NOT NULL,

    CONSTRAINT pk_sat_eff_bc PRIMARY KEY (lnk_banner_campaign_hk, load_dts),
    CONSTRAINT fk_sat_eff_bc_lnk FOREIGN KEY (lnk_banner_campaign_hk)
        REFERENCES ceh.lnk_banner_campaign (lnk_banner_campaign_hk)
);


-- ============================================================
-- BUSINESS VAULT
-- ============================================================

CREATE TABLE ceh.bsat_show_metrics (
    lnk_show_hk    CHAR(32)       NOT NULL,
    load_dts        TIMESTAMP      NOT NULL,
    cpm_calculated  DECIMAL(15,6),
    cpc_calculated  DECIMAL(15,6),
    ctr             DECIMAL(10,6),
    record_source   VARCHAR(100)   NOT NULL,

    CONSTRAINT pk_bsat_show_metrics PRIMARY KEY (lnk_show_hk, load_dts),
    CONSTRAINT fk_bsat_show_lnk FOREIGN KEY (lnk_show_hk) REFERENCES ceh.lnk_show (lnk_show_hk)
);