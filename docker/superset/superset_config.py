import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DB_URI"]
# минимальная конфигурация одного узла: без celery/redis
TALISMAN_ENABLED = False
SESSION_COOKIE_SECURE = False
WTF_CSRF_ENABLED = True
SUPERSET_WEBSERVER_ADDRESS = "0.0.0.0"
SUPERSET_WEBSERVER_PORT = 8088
SQL_MAX_ROW = 100_000
# аналитикам не показываем raw-таблицы состояния — доступ только к витринам Gold
# (роли настраиваются в bootstrap; доступ к базе ограничен срезом доступа)
CSV_EXPORT = {"encoding": "utf-8"}
