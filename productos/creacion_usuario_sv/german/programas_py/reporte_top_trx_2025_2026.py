"""
Top 5 transacciones con monto y top 5 sin monto por cohorte de creacion de usuario.

Cohorte 2025: usuarios creados en 2025. Trx del ano 2025.
Cohorte 2026: usuarios creados en 2026. Trx 2026-01-01 a 2026-04-30 (excluye mayo).

Fuente de trx: Journal (dw_BEL_IBJOUR).

Columnas exportadas: nombre_transaccion, total_trx, clientes_unicos, monto_total (si aplica).

Uso:
    python3 productos/creacion_usuario_sv/german/programas_py/reporte_top_trx_2025_2026.py
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
DB_NAME = "DWHSV"
EXPORTS_DIR = BASE_DIR / "exports"


def get_engine():
    conn_params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={conn_params}", fast_executemany=True)


def run_query(sql: str) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


def run_query_file(path: Path) -> pd.DataFrame:
    return run_query(path.read_text(encoding="utf-8"))


def normalizar_codigo_cliente(valor) -> str:
    if pd.isna(valor):
        return ""
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    return solo_digitos[-8:].zfill(8) if solo_digitos else ""


def cargar_bases() -> tuple[pd.DataFrame, pd.DataFrame]:
    print(f"Cargando datos desde {DB_NAME}...")
    usuarios = run_query_file(BASE_DIR / "queries" / "base_usuarios_2025_2026.sql")
    trx_journal = run_query_file(BASE_DIR / "queries" / "trx_usuarios_2025_2026.sql")
    print(f"  Usuarios:    {len(usuarios):,}")
    print(f"  Journal trx: {len(trx_journal):,}")
    return usuarios, trx_journal


def preparar_cohorte(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["fecha_creacion_usuario"] = pd.to_datetime(data["fecha_creacion_usuario"], errors="coerce")
    data = data[data["fecha_creacion_usuario"].notna()].copy()
    data["anio_creacion"] = data["fecha_creacion_usuario"].dt.year
    data = data[data["anio_creacion"].isin([2025, 2026])].copy()

    data["id_usuario"] = data["nombre_usuario"].astype("string").str.strip()
    data.loc[data["id_usuario"] == "", "id_usuario"] = pd.NA
    data = data[data["id_usuario"].notna()].copy()

    data["codigo_cliente"] = data["codigo_cliente_usuario_creado"].apply(normalizar_codigo_cliente)
    data = data[data["codigo_cliente"] != ""].copy()

    # Un registro por usuario, el mas antiguo.
    data = (
        data.sort_values(["id_usuario", "fecha_creacion_usuario"])
        .drop_duplicates(subset=["id_usuario"], keep="first")
        .copy()
    )
    return data[["id_usuario", "codigo_cliente", "anio_creacion"]].copy()


def preparar_trx(df_journal: pd.DataFrame) -> pd.DataFrame:
    out = df_journal.copy()
    out["fecha_transaccion"] = pd.to_datetime(out["fecha_transaccion"], errors="coerce")
    out = out[out["fecha_transaccion"].notna()].copy()
    out["codigo_cliente"] = out["codigo_cliente_transaccion"].apply(normalizar_codigo_cliente)
    out = out[out["codigo_cliente"] != ""].copy()
    out["valor"] = pd.to_numeric(out["valor"], errors="coerce")
    out["nombre_transaccion"] = (
        out["descripcion_transaccion"]
        .fillna("SIN_DESCRIPCION")
        .astype(str)
        .str.strip()
        .replace("", "SIN_DESCRIPCION")
    )
    return out[["fecha_transaccion", "codigo_cliente", "nombre_transaccion", "valor"]].copy()


def calcular_top5(merged: pd.DataFrame, con_monto: bool) -> pd.DataFrame:
    if con_monto:
        sub = merged[merged["valor"].notna() & (merged["valor"] > 0)].copy()
    else:
        sub = merged[merged["valor"].isna() | (merged["valor"] <= 0)].copy()

    if sub.empty:
        cols = ["nombre_transaccion", "total_trx", "clientes_unicos"]
        if con_monto:
            cols.append("monto_total")
        return pd.DataFrame(columns=cols)

    agg = (
        sub.groupby("nombre_transaccion", as_index=False)
        .agg(
            total_trx=("codigo_cliente", "size"),
            clientes_unicos=("id_usuario", "nunique"),
            monto_total=("valor", "sum"),
        )
        .sort_values("total_trx", ascending=False)
        .head(6)
        .reset_index(drop=True)
    )

    if not con_monto:
        agg = agg.drop(columns=["monto_total"])

    return agg


def reporte_cohorte(
    cohort: pd.DataFrame, trx: pd.DataFrame, anio: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    cohort_anio = cohort[cohort["anio_creacion"] == anio].copy()

    if anio == 2026:
        trx_anio = trx[
            (trx["fecha_transaccion"].dt.year == 2026)
            & (trx["fecha_transaccion"] < pd.Timestamp("2026-05-01"))
        ].copy()
    else:
        trx_anio = trx[trx["fecha_transaccion"].dt.year == anio].copy()

    merged = cohort_anio.merge(trx_anio, how="inner", on="codigo_cliente")

    print(
        f"\nCohorte {anio}: {cohort_anio['id_usuario'].nunique():,} usuarios | "
        f"{len(merged):,} trx apareadas"
    )

    return calcular_top5(merged, con_monto=True), calcular_top5(merged, con_monto=False)


def imprimir_top(label: str, df: pd.DataFrame) -> None:
    print(f"\n{label}:")
    if df.empty:
        print("  Sin datos.")
    else:
        print(df.to_string(index=False))


def main() -> None:
    usuarios_df, journal_df = cargar_bases()
    cohort = preparar_cohorte(usuarios_df)
    trx = preparar_trx(journal_df)

    top5_2025_con, top5_2025_sin = reporte_cohorte(cohort, trx, 2025)
    top5_2026_con, top5_2026_sin = reporte_cohorte(cohort, trx, 2026)

    imprimir_top("Top 5 CON MONTO - Cohorte 2025", top5_2025_con)
    imprimir_top("Top 5 SIN MONTO - Cohorte 2025", top5_2025_sin)
    imprimir_top("Top 5 CON MONTO - Cohorte 2026 (ene-abr)", top5_2026_con)
    imprimir_top("Top 5 SIN MONTO - Cohorte 2026 (ene-abr)", top5_2026_sin)

    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = EXPORTS_DIR / "top5_trx_por_cohorte_2025_2026.xlsx"
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        top5_2025_con.to_excel(writer, sheet_name="2025_con_monto", index=False)
        top5_2025_sin.to_excel(writer, sheet_name="2025_sin_monto", index=False)
        top5_2026_con.to_excel(writer, sheet_name="2026_con_monto", index=False)
        top5_2026_sin.to_excel(writer, sheet_name="2026_sin_monto", index=False)

    print(f"\nExcel generado: {out}")


if __name__ == "__main__":
    try:
        main()
    except SQLAlchemyError as exc:
        msg = " ".join(str(exc).split())
        print(f"[ERROR] DB {DB_NAME}: {msg}")
        sys.exit(1)
