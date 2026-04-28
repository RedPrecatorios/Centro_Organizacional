import os

# Gunicorn config for Centro_Organizacional
#
# Importante: o EDA Diário mantém estado in-memory (single-user), então
# mantemos workers=1 para evitar inconsistências.

bind = os.getenv("GUNICORN_BIND", "127.0.0.1:8001")
workers = int(os.getenv("GUNICORN_WORKERS", "1"))
threads = int(os.getenv("GUNICORN_THREADS", "4"))

timeout = int(os.getenv("GUNICORN_TIMEOUT", "300"))
graceful_timeout = int(os.getenv("GUNICORN_GRACEFUL_TIMEOUT", "30"))

accesslog = os.getenv("GUNICORN_ACCESSLOG", "-")
errorlog = os.getenv("GUNICORN_ERRORLOG", "-")
loglevel = os.getenv("GUNICORN_LOGLEVEL", "info")

preload_app = False
