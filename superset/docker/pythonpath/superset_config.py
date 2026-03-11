import os
from dotenv import load_dotenv

load_dotenv()  # загружает .env в os.environ

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://"
    f"{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@"
    f"{os.environ.get('POSTGRES_HOST', 'db')}:"
    f"{os.environ.get('POSTGRES_PORT', '5432')}/"
    f"{os.environ['POSTGRES_DB']}"
)

FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True,
    "ALERT_REPORTS": True,
    "DASHBOARD_VIRTUALIZATION": True,
    "ENABLE_TEMPLATE_PROCESSING": True, #new # For jinja tempalting in SQL
    "ENABLE_EXPLORE_JSON_EDITOR": True,
}


# Allow framing for embeds
ENABLE_PROXY_FIX = True
TALISMAN_ENABLED = False
WTF_CSRF_ENABLED = False   # Optional for guest token
GUEST_TOKEN_JWT_SECRET = "my_super_embedded_secret"
GUEST_ROLE_NAME = "Admin"

ENABLE_PROXY_FIX = True
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "expose_headers": ["*"],
    "origins": ["*"],
}


# ===============================
# Redis (PRODUCTION CACHE)
# ===============================

REDIS_HOST = "redis"          # docker-compose service name
REDIS_PORT = 6379
REDIS_DB_CACHE = 1
REDIS_DB_RATE_LIMIT = 2

REDIS_URL_CACHE = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB_CACHE}"
REDIS_URL_RATE_LIMIT = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB_RATE_LIMIT}"

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": REDIS_URL_CACHE,
    "CACHE_DEFAULT_TIMEOUT": 86400,
}

DATA_CACHE_CONFIG = CACHE_CONFIG

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": REDIS_URL_CACHE,
    "CACHE_DEFAULT_TIMEOUT": 7776000,  # 90 days
    "REFRESH_TIMEOUT_ON_RETRIEVAL": True,
}

# Flask-Limiter (remove in-memory warning)
RATELIMIT_STORAGE_URI = REDIS_URL_RATE_LIMIT


# ===============================
# SQLAlchemy / DB tuning
# ===============================

SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_pre_ping": True,
    "pool_size": 10,
    "max_overflow": 20,
}


HTML_SANITIZATION = False
ENABLE_UI_THEME_ADMINISTRATION = True

# Custom logo for the default theme
THEME_DEFAULT = {
    "token": {
        "brandLogoUrl": "/static/assets/images/centaur_logo.png",
        "brandLogoAlt": "Centaur",
    }
}


