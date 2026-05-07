"""
Reporte en consola: genero y generacion de clientes (producto general).

Periodo:
    2025 completo + 2026 completo

Uso:
    python3 productos/creacion_usuario_sv/german/programas_py/reporte_genero_generacion_2025_2026.py
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

GEN_ORDER = [
    "Generacion silenciosa (<=1945)",
    "Baby Boomers (1946-1964)",
    "Generation X (1965-1980)",
    "Gen Y - Millennials (1981-1996)",
    "Generacion Z (1997-2012)",
    "Generacion Alpha (2013+)",
    "SIN DATO",
]


def run_query_hsv(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Ejecuta SQL forzando DB DWHSV."""
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


def run_query_file_hsv(path: str, params: dict | None = None) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    return run_query_hsv(sql, params)


def normalizar_genero(valor) -> str:
    if pd.isna(valor):
        return "SIN DATO"
    genero = str(valor).strip().upper()
    if genero in {"M", "H", "MALE", "MASCULINO", "HOMBRE", "1"}:
        return "MASCULINO"
    if genero in {"F", "FEMALE", "FEMENINO", "MUJER", "2"}:
        return "FEMENINO"
    return "SIN DATO"


def clasificar_generacion(fecha_nac) -> str:
    if pd.isna(fecha_nac):
        return "SIN DATO"
    anio = int(fecha_nac.year)
    if anio <= 1945:
        return "Generacion silenciosa (<=1945)"
    if 1946 <= anio <= 1964:
        return "Baby Boomers (1946-1964)"
    if 1965 <= anio <= 1980:
        return "Generation X (1965-1980)"
    if 1981 <= anio <= 1996:
        return "Gen Y - Millennials (1981-1996)"
    if 1997 <= anio <= 2012:
        return "Generacion Z (1997-2012)"
    return "Generacion Alpha (2013+)"


def cargar_base() -> pd.DataFrame:
    print(f"Cargando base en {DB_NAME_DASHBOARD}...")
    df = run_query_file_hsv(str(BASE_DIR / "queries" / "base_usuarios_2025_2026.sql"))
    print(f"  {len(df):,} registros base")
    return df


def preparar_datos(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["fecha_creacion_usuario"] = pd.to_datetime(data["fecha_creacion_usuario"], errors="coerce")
    data["fecha_nacimiento_usuario"] = pd.to_datetime(data["fecha_nacimiento_usuario"], errors="coerce")

    data = data[data["fecha_creacion_usuario"].notna()].copy()
    data["anio"] = data["fecha_creacion_usuario"].dt.year
    data = data[data["anio"].isin([2025, 2026])].copy()

    data["id_usuario"] = data["nombre_usuario"].astype("string").str.strip()
    data.loc[data["nombre_usuario"].isna(), "id_usuario"] = pd.NA
    data.loc[data["id_usuario"] == "", "id_usuario"] = pd.NA
    data = data[data["id_usuario"].notna()].copy()

    data["genero"] = data["genero_cliente"].apply(normalizar_genero)
    data["generacion"] = data["fecha_nacimiento_usuario"].apply(clasificar_generacion)

    # Evita duplicar usuarios si hubiera multiples filas por joins en origen.
    data = (
        data.sort_values(["id_usuario", "fecha_creacion_usuario"])
        .drop_duplicates(subset=["id_usuario"], keep="first")
        .copy()
    )

    return data


def imprimir_distribucion(titulo: str, serie: pd.Series, total: int, orden: list[str] | None = None) -> None:
    print(f"\n--- {titulo} ---")
    if total == 0 or serie.empty:
        print("Sin datos")
        return

    if orden:
        serie = serie.reindex(orden, fill_value=0)
    else:
        serie = serie.sort_values(ascending=False)

    for etiqueta, valor in serie.items():
        pct = (valor / total * 100) if total else 0
        print(f"{etiqueta:35} {int(valor):>10,}  ({pct:6.2f}%)")


def imprimir_resumen(data: pd.DataFrame) -> None:
    print("=" * 78)
    print("REPORTE PRODUCTO GENERAL - GENERO Y GENERACION")
    print("Periodo analizado: 2025-01-01 a 2026-12-31")
    print("Criterio: Sin filtro de campañas (solo base de creacion de usuarios)")
    print("=" * 78)

    total = int(data["id_usuario"].nunique())
    print(f"Total usuarios unicos (RECDIST nombre_usuario): {total:,}")

    por_anio = data.groupby("anio")["id_usuario"].nunique().sort_index()
    print("\n--- Usuarios unicos por anio ---")
    for anio, valor in por_anio.items():
        pct = (valor / total * 100) if total else 0
        print(f"{int(anio)}{'':31} {int(valor):>10,}  ({pct:6.2f}%)")

    genero_counts = data["genero"].value_counts()
    imprimir_distribucion("Distribucion por genero (global)", genero_counts, total, ["MASCULINO", "FEMENINO", "SIN DATO"])

    generacion_counts = data["generacion"].value_counts()
    imprimir_distribucion("Distribucion por generacion (global)", generacion_counts, total, GEN_ORDER)

    for anio in [2025, 2026]:
        sub = data[data["anio"] == anio].copy()
        total_sub = int(sub["id_usuario"].nunique())
        print(f"\n{'=' * 78}\nDetalle {anio} - total usuarios: {total_sub:,}\n{'=' * 78}")

        imprimir_distribucion(
            f"Genero {anio}",
            sub["genero"].value_counts(),
            total_sub,
            ["MASCULINO", "FEMENINO", "SIN DATO"],
        )
        imprimir_distribucion(
            f"Generacion {anio}",
            sub["generacion"].value_counts(),
            total_sub,
            GEN_ORDER,
        )


if __name__ == "__main__":
    try:
        df_base = cargar_base()
        df_limpio = preparar_datos(df_base)
        imprimir_resumen(df_limpio)
    except SQLAlchemyError as exc:
        msg = " ".join(str(exc).split())
        print(f"[ERROR] No se pudo conectar/consultar en {DB_NAME_DASHBOARD}: {msg}")
        sys.exit(1)
