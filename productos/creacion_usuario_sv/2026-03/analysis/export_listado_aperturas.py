"""
export_listado_aperturas.py
---------------------------
Exporta listado de usuarios aperturados para comparación con Tableau.

Uso:
    python3 productos/creacion_usuario_sv/2026-03/analysis/export_listado_aperturas.py --anio 2026 --mes 3
    python3 productos/creacion_usuario_sv/2026-03/analysis/export_listado_aperturas.py --anio 2026 --mes 3 --medio "Medios propios"
"""

import argparse
import os
import urllib
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


BASE_DIR = Path(__file__).resolve().parents[1]
DB_NAME = "DWHSV"


def normalizar_codigo_cliente(valor) -> str:
    if pd.isna(valor):
        return ""
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    return solo_digitos[-8:].zfill(8) if solo_digitos else ""


def get_engine_hsv():
    load_dotenv(Path(__file__).resolve().parents[4] / ".env")
    db_server = os.getenv("DB_SERVER")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    db_driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

    params = urllib.parse.quote_plus(
        f"DRIVER={{{db_driver}}};"
        f"SERVER={db_server};"
        f"DATABASE={DB_NAME};"
        f"UID={db_user};"
        f"PWD={db_pass};"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)


def run_query_file_hsv(path: Path) -> pd.DataFrame:
    sql = path.read_text(encoding="utf-8")
    engine = get_engine_hsv()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def construir_listado(anio: int, mes: int, medio: str) -> pd.DataFrame:
    conv = run_query_file_hsv(BASE_DIR / "queries" / "conversion.sql")
    rtm = run_query_file_hsv(BASE_DIR / "queries" / "comunicacionesRTM.sql")

    conv["fecha_creacion_usuario"] = pd.to_datetime(conv["fecha_creacion_usuario"], errors="coerce")
    conv = conv[conv["fecha_creacion_usuario"].notna()].copy()
    conv["anio"] = conv["fecha_creacion_usuario"].dt.year
    conv["mes"] = conv["fecha_creacion_usuario"].dt.month
    conv = conv[(conv["anio"] == anio) & (conv["mes"] == mes)].copy()

    # RECDIST(nombre_usuario): no contar vacíos/nulos.
    conv["nombre_usuario"] = conv["nombre_usuario"].astype("string").str.strip()
    conv = conv[conv["nombre_usuario"].notna() & (conv["nombre_usuario"] != "")].copy()

    conv["codigo_cliente_usuario_creado"] = conv["codigo_cliente_usuario_creado"].apply(normalizar_codigo_cliente)
    rtm["codigo_cliente_usuario_campania"] = rtm["codigo_cliente_usuario_campania"].apply(normalizar_codigo_cliente)
    rtm["fecha_campania"] = pd.to_datetime(rtm["fecha_campania"], errors="coerce")

    rtm = rtm[rtm["codigo_cliente_usuario_campania"] != ""].copy()
    rtm = (
        rtm.sort_values("fecha_campania")
        .drop_duplicates(subset=["codigo_cliente_usuario_campania"], keep="first")
        .copy()
    )

    df = conv.merge(
        rtm[["codigo_cliente_usuario_campania", "fecha_campania"]],
        how="left",
        left_on="codigo_cliente_usuario_creado",
        right_on="codigo_cliente_usuario_campania",
    )
    df["medio"] = df["fecha_campania"].apply(lambda x: "Medios propios" if pd.notna(x) else "Producto")

    if medio in {"Medios propios", "Producto"}:
        df = df[df["medio"] == medio].copy()

    # Deduplicar por nombre_usuario para espejo de RECDIST.
    df = (
        df.sort_values(["nombre_usuario", "fecha_creacion_usuario"])
        .drop_duplicates(subset=["nombre_usuario"], keep="first")
        .copy()
    )

    return df[
        [
            "nombre_usuario",
            "codigo_cliente_usuario_creado",
            "fecha_creacion_usuario",
            "medio",
            "fecha_campania",
            "estado_usuario",
            "estado_cliente",
            "direccion_lvl_1",
            "direccion_lvl_2",
            "direccion_lvl_3",
        ]
    ].sort_values(["medio", "fecha_creacion_usuario", "nombre_usuario"])


def main():
    parser = argparse.ArgumentParser(description="Exporta listado de aperturas para comparar con Tableau.")
    parser.add_argument("--anio", type=int, required=True, help="Año de análisis. Ej: 2026")
    parser.add_argument("--mes", type=int, required=True, help="Mes de análisis (1-12). Ej: 3")
    parser.add_argument(
        "--medio",
        type=str,
        default="Todos",
        choices=["Todos", "Medios propios", "Producto"],
        help="Filtro de medio",
    )
    args = parser.parse_args()

    df = construir_listado(args.anio, args.mes, args.medio)
    conteos = df.groupby("medio")["nombre_usuario"].nunique() if not df.empty else pd.Series(dtype="int64")

    out_dir = BASE_DIR / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)
    medio_tag = args.medio.lower().replace(" ", "_")
    out_file = out_dir / f"listado_usuarios_apertura_{args.anio}_{args.mes:02d}_{medio_tag}.csv"
    df.to_csv(out_file, index=False, encoding="utf-8-sig")

    print(f"Listado exportado: {out_file}")
    print(f"Total usuarios (RECDIST nombre_usuario): {df['nombre_usuario'].nunique():,}")
    if not conteos.empty:
        print("Detalle por medio:")
        for m, v in conteos.items():
            print(f"  {m}: {v:,}")


if __name__ == "__main__":
    main()
