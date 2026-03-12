CREATE SCHEMA IF NOT EXISTS stg_ceh;

DROP TABLE IF EXISTS stg_ceh.fct_banners_show;
CREATE TABLE stg_ceh.fct_banners_show (
    banner_id INTEGER  NULL,
    campaign_id INTEGER  NULL,
    user_id BIGINT  NULL,
    timestamp TIMESTAMPTZ  NULL,
    placement VARCHAR(20)  NULL,
    device_type VARCHAR(20)  NULL,
    os VARCHAR(10)  NULL,
    geo VARCHAR(100)  NULL,
    is_clicked SMALLINT  NULL,
   
    -- Staging поля
    load_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    load_id SERIAL,
    raw_timestamp VARCHAR(20)
    
);

DROP TABLE IF EXISTS stg_ceh.fct_actions;
CREATE TABLE stg_ceh.fct_actions (
    user_id BIGINT  NULL,
    session_start TIMESTAMPTZ  NULL,
    actions VARCHAR(50)  NULL,
    
    -- Staging поля
    load_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    load_id SERIAL,
    raw_session_start VARCHAR(20)
    
);

DROP TABLE IF EXISTS stg_ceh.cd_banner;
CREATE TABLE stg_ceh.cd_banner (
    banner_id INTEGER  NULL,
    creative_type VARCHAR(20)  NULL,
    message VARCHAR(50)  NULL,
    size VARCHAR(20)  NULL,
    target_audience_segment VARCHAR(20)  NULL,
    
    load_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    load_id SERIAL,
    raw_data TEXT
    
);

DROP TABLE IF EXISTS stg_ceh.cd_user;
CREATE TABLE stg_ceh.cd_user (
    user_id BIGINT  NULL,
    segment VARCHAR(20)  NULL,
    tariff VARCHAR(20)  NULL,
    date_create DATE  NULL,
    date_end DATE,
    
    load_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    load_id SERIAL,
    raw_dates VARCHAR(40)
    
);

DROP TABLE IF EXISTS stg_ceh.cd_campaign;
CREATE TABLE stg_ceh.cd_campaign (
    campaign_id INTEGER NOT NULL,
    daily_budget NUMERIC(12,2) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    
    load_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    load_id SERIAL,
    raw_dates VARCHAR(40)
    
);

DROP TABLE IF EXISTS stg_ceh.installs;
CREATE TABLE stg_ceh.installs (
    user_id BIGINT NOT NULL,
    install_timestamp TIMESTAMPTZ NOT NULL,
    source VARCHAR(20) NOT NULL,
    
    load_ts TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    load_id SERIAL,
    raw_install_timestamp VARCHAR(20)
    
);

