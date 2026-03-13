"""
db.py
-----
Módulo central de conexión a SQL Server.
Usa SQLAlchemy + pyodbc con credenciales desde .env

Funciones disponibles:
    get_engine()          → SQLAlchemy engine (para uso avanzado)
    run_query(sql)        → Ejecuta SQL y retorna un DataFrame
    run_query_file(path)  → Ejecuta un archivo .sql y retorna un DataFrame

Uso básico:
    from core.db import run_query

    df = run_query("SELECT TOP 10 * FROM DW_CIF_CLIENTES")
    print(df.head())
"""

import pandas as pd
import urllib
from sqlalchemy import create_engine, text
from core.config import DB_SERVER, DB_NAME, DB_USER, DB_PASS, DB_DRIVER


def get_engine():
    """Crea y retorna el engine de SQLAlchemy."""
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)
    return engine


def run_query(sql: str, params: dict = None) -> pd.DataFrame:
    """
    Ejecuta una consulta SQL y retorna un DataFrame.

    Args:
        sql    : Consulta SQL como string.
        params : Diccionario de parámetros opcionales para la query.

    Returns:
        pd.DataFrame con los resultados.

    Ejemplo:
        df = run_query("SELECT * FROM tabla WHERE mes = :mes", {"mes": "2026-03"})
    """
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def run_query_file(path: str, params: dict = None) -> pd.DataFrame:
    """
    Lee un archivo .sql y ejecuta su contenido.

    Args:
        path   : Ruta al archivo .sql (ej: "productos/cuenta_digital/2026-03/queries/clientes.sql")
        params : Diccionario de parámetros opcionales.

    Returns:
        pd.DataFrame con los resultados.

    Ejemplo:
        df = run_query_file("productos/app_empresarial/2026-03/queries/transacciones.sql")
    """
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    return run_query(sql, params)
