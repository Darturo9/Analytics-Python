"""
config.py
---------
Carga las variables de entorno desde el archivo .env
Todas las credenciales y parámetros de conexión se definen aquí.

Uso:
    from core.config import DB_SERVER, DB_NAME, DB_USER, DB_PASS
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Conexión SQL Server ──────────────────────────────────────────────────────
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME   = os.getenv("DB_NAME")
DB_USER   = os.getenv("DB_USER")
DB_PASS   = os.getenv("DB_PASS")
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
