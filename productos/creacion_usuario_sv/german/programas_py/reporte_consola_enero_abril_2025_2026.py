"""
Reporte en consola: cohorte enero-abril 2025 vs enero-abril 2026 (sin exportaciones).

Bloques:
1) Logins totales y clientes unicos - cohorte 2025 (creados en 2025, eventos ene-abr 2025)
2) Logins totales y clientes unicos - cohorte 2026 (creados en 2026, eventos ene-abr 2026)
3) TRX totales, clientes unicos y monto total - cohorte 2025 (ene-abr 2025)
4) TRX totales, clientes unicos y monto total - cohorte 2026 (ene-abr 2026)

Regla de cohorte:
    anio_evento == anio_creacion
    mes_evento <= 4  (enero a abril)

Uso:
    python3 productos/creacion_usuario_sv/german/programas_py/reporte_consola_enero_abril_2025_2026.py
"""

from pathlib import Path
import sys
import urllib

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.config import DB_SERVER, DB_USER, DB_PASS, DB_DRIVER

BASE_DIR = Path(__file__).resolve().parents[1]
DB_NAME_DASHBOARD = "DWHSV"

ANIOS = [2025, 2026]
MES_CORTE = 4


def run_query_hsv(sql: str, params: dict | None = None) -> pd.DataFrame:
    conn_params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME_DASHBOARD};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_params}", fast_executemany=True)
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def run_query_file_hsv(path: Path, params: dict | None = None) -> pd.DataFrame:
    sql = path.read_text(encoding="utf-8")
    return run_query_hsv(sql, params)


def normalizar_codigo_cliente(valor) -> str:
    if pd.isna(valor):
        return ""
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    return solo_digitos[-8:].zfill(8) if solo_digitos else ""


def cargar_bases() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    print(f"Cargando bases en {DB_NAME_DASHBOARD}...")
    usuarios = run_query_file_hsv(BASE_DIR / "queries" / "base_usuarios_2025_2026.sql")
    trx = run_query_file_hsv(BASE_DIR / "queries" / "trx_usuarios_2025_2026.sql")
    logins = run_query_file_hsv(BASE_DIR / "queries" / "logins_usuarios_2025_2026.sql")
    print(f"  Usuarios base: {len(usuarios):,} filas")
    print(f"  Transacciones: {len(trx):,} filas")
    print(f"  Logins:        {len(logins):,} filas")
    return usuarios, trx, logins


def preparar_cohorte(df_usuarios: pd.DataFrame) -> pd.DataFrame:
    data = df_usuarios.copy()
    data["fecha_creacion_usuario"] = pd.to_datetime(data["fecha_creacion_usuario"], errors="coerce")
    data = data[data["fecha_creacion_usuario"].notna()].copy()
    data["anio_creacion"] = data["fecha_creacion_usuario"].dt.year
    data = data[data["anio_creacion"].isin(ANIOS)].copy()

    data["id_usuario"] = data["nombre_usuario"].astype("string").str.strip()
    data.loc[data["nombre_usuario"].isna(), "id_usuario"] = pd.NA
    data.loc[data["id_usuario"] == "", "id_usuario"] = pd.NA
    data = data[data["id_usuario"].notna()].copy()

    data["codigo_cliente"] = data["codigo_cliente_usuario_creado"].apply(normalizar_codigo_cliente)
    data = data[data["codigo_cliente"] != ""].copy()

    data = (
        data.sort_values(["id_usuario", "fecha_creacion_usuario"])
        .drop_duplicates(subset=["id_usuario"], keep="first")
        .copy()
    )

    return data[["id_usuario", "codigo_cliente", "anio_creacion"]].copy()


def preparar_trx(df_trx: pd.DataFrame) -> pd.DataFrame:
    trx = df_trx.copy()
    trx["fecha_transaccion"] = pd.to_datetime(trx["fecha_transaccion"], errors="coerce")
    trx = trx[trx["fecha_transaccion"].notna()].copy()
    trx["anio_trx"] = trx["fecha_transaccion"].dt.year
    trx["mes_trx"] = trx["fecha_transaccion"].dt.month
    trx = trx[trx["anio_trx"].isin(ANIOS)].copy()
    trx = trx[trx["mes_trx"] <= MES_CORTE].copy()

    trx["codigo_cliente"] = trx["codigo_cliente_transaccion"].apply(normalizar_codigo_cliente)
    trx = trx[trx["codigo_cliente"] != ""].copy()

    trx["valor"] = pd.to_numeric(trx["valor"], errors="coerce").fillna(0.0)
    return trx


def preparar_logins(df_logins: pd.DataFrame) -> pd.DataFrame:
    logins = df_logins.copy()
    logins["codigo_cliente"] = logins["codigo_cliente_login"].apply(normalizar_codigo_cliente)
    logins = logins[logins["codigo_cliente"] != ""].copy()

    logins["anio_login"] = pd.to_numeric(logins["anio_login"], errors="coerce")
    logins = logins[logins["anio_login"].notna()].copy()
    logins["anio_login"] = logins["anio_login"].astype(int)
    logins = logins[logins["anio_login"].isin(ANIOS)].copy()

    logins["mes_login"] = pd.to_numeric(logins["mes_login"], errors="coerce")
    logins = logins[logins["mes_login"].notna()].copy()
    logins["mes_login"] = logins["mes_login"].astype(int)
    logins = logins[logins["mes_login"] <= MES_CORTE].copy()

    return logins


def construir_datasets(
    cohort: pd.DataFrame, trx: pd.DataFrame, logins: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_trx = cohort.merge(trx, how="inner", on="codigo_cliente")
    df_trx = df_trx[df_trx["anio_creacion"] == df_trx["anio_trx"]].copy()

    df_logins = cohort.merge(logins, how="inner", on="codigo_cliente")
    df_logins = df_logins[df_logins["anio_creacion"] == df_logins["anio_login"]].copy()

    return df_trx, df_logins


def imprimir_bloque_logins(df_logins: pd.DataFrame, anio: int) -> None:
    sub = df_logins[df_logins["anio_login"] == anio]
    total_logins = int(len(sub))
    clientes_unicos = int(sub["codigo_cliente"].nunique())
    print(f"\nLOGINS {anio} (usuarios creados en {anio}, enero-abril)")
    print(f"  total_logins:    {total_logins:,}")
    print(f"  clientes_unicos: {clientes_unicos:,}")


def imprimir_bloque_trx(df_trx: pd.DataFrame, anio: int) -> None:
    sub = df_trx[df_trx["anio_trx"] == anio]
    total_trx = int(len(sub))
    clientes_unicos = int(sub["codigo_cliente"].nunique())
    monto_total = float(sub["valor"].sum()) if not sub.empty else 0.0
    print(f"\nTRX {anio} (clientes creados en {anio}, enero-abril)")
    print(f"  total_trx:       {total_trx:,}")
    print(f"  clientes_unicos: {clientes_unicos:,}")
    print(f"  monto_total:     {monto_total:,.2f}")


def main() -> None:
    print("=" * 92)
    print("REPORTE CONSOLA ENERO-ABRIL 2025 vs 2026")
    print("Regla: anio_evento == anio_creacion | mes_evento <= 4")
    print("Sin exportaciones de archivos")
    print("=" * 92)

    usuarios_df, trx_df, logins_df = cargar_bases()
    cohort = preparar_cohorte(usuarios_df)
    trx = preparar_trx(trx_df)
    logins = preparar_logins(logins_df)
    cohort_trx, cohort_logins = construir_datasets(cohort, trx, logins)

    for anio in ANIOS:
        imprimir_bloque_logins(cohort_logins, anio)

    print()

    for anio in ANIOS:
        imprimir_bloque_trx(cohort_trx, anio)


if __name__ == "__main__":
    try:
        main()
    except SQLAlchemyError as exc:
        msg = " ".join(str(exc).split())
        print(f"[ERROR] No se pudo conectar/consultar en {DB_NAME_DASHBOARD}: {msg}")
        sys.exit(1)
