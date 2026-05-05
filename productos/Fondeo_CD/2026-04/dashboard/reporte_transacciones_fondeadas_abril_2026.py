"""
reporte_transacciones_fondeadas_abril_2026.py
---------------------------------------------------
Reporte de consola para transacciones/uso de dinero de clientes
con cuentas fondeadas en abril 2026 (mes completo).

Ejecucion:
    python3 productos/Fondeo_CD/2026-04/dashboard/reporte_transacciones_fondeadas_abril_2026.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import get_engine


QUERY_PATH = (
    PROJECT_ROOT
    / "productos"
    / "Fondeo_CD"
    / "2026-04"
    / "queries"
    / "UsoDineroCuentasFondeadasAbril2026_TEMP.sql"
)

TOP_N = 10


def cargar_datos() -> pd.DataFrame:
    with open(QUERY_PATH, "r", encoding="utf-8") as f:
        sql = f.read()

    engine = get_engine()
    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columnas: list[str] | None = None
        filas = []

        while True:
            if cursor.description is not None:
                columnas = [str(c[0]).strip().lower() for c in cursor.description]
                filas = cursor.fetchall()

            if not cursor.nextset():
                break

        if columnas is None:
            return pd.DataFrame()

        df = pd.DataFrame.from_records(filas, columns=columnas)
    finally:
        cursor.close()
        conn.close()
        engine.dispose()

    df.columns = [str(c).strip().lower() for c in df.columns]

    columnas = ["origen", "tipo_uso", "total_transacciones", "clientes_unicos", "monto_total", "monto_promedio"]
    faltantes = [c for c in columnas if c not in df.columns]
    if faltantes:
        raise ValueError(f"La query no devolvio columnas requeridas: {faltantes}")

    df["origen"] = df["origen"].astype(str).str.strip().str.upper()
    df["tipo_uso"] = df["tipo_uso"].astype(str).str.strip()
    df["total_transacciones"] = pd.to_numeric(df["total_transacciones"], errors="coerce").fillna(0).astype(int)
    df["clientes_unicos"] = pd.to_numeric(df["clientes_unicos"], errors="coerce").fillna(0).astype(int)
    df["monto_total"] = pd.to_numeric(df["monto_total"], errors="coerce").fillna(0.0)
    df["monto_promedio"] = pd.to_numeric(df["monto_promedio"], errors="coerce").fillna(0.0)

    df = df.sort_values(["origen", "total_transacciones", "tipo_uso"], ascending=[True, False, True]).reset_index(drop=True)
    return df


def _imprimir_bloque(df_origen: pd.DataFrame, origen: str, total_tx_global: int) -> None:
    top = df_origen.head(TOP_N).copy()
    total_tx = int(df_origen["total_transacciones"].sum())
    monto_total = float(df_origen["monto_total"].sum())
    top["pct_tx"] = (top["total_transacciones"] / total_tx * 100.0).round(2) if total_tx > 0 else 0.0

    print(f"\n{'=' * 110}")
    print(f"  {origen}  —  Top {TOP_N} categorias por transacciones")
    print(f"{'=' * 110}")
    print(f"  Transacciones en este canal:   {total_tx:>12,}   ({total_tx / total_tx_global * 100:.1f}% del total)")
    print(f"  Monto total (L):               {monto_total:>12,.2f}")
    print(f"{'-' * 110}")
    print(
        top[
            ["tipo_uso", "total_transacciones", "pct_tx", "clientes_unicos", "monto_total", "monto_promedio"]
        ].to_string(
            index=False,
            formatters={
                "total_transacciones": "{:,.0f}".format,
                "pct_tx": "{:,.2f}%".format,
                "clientes_unicos": "{:,.0f}".format,
                "monto_total": "{:,.2f}".format,
                "monto_promedio": "{:,.2f}".format,
            },
        )
    )


def imprimir_reporte(df: pd.DataFrame) -> None:
    if df.empty:
        print("No se encontraron transacciones para cuentas fondeadas (abril 2026 completo).")
        return

    total_tx = int(df["total_transacciones"].sum())
    monto_total = float(df["monto_total"].sum())

    print("=" * 110)
    print("TRANSACCIONES / USO DE DINERO - CUENTAS FONDEADAS ABRIL 2026 (MES COMPLETO)")
    print("=" * 110)
    print(f"  Total transacciones (todos los canales):  {total_tx:>12,}")
    print(f"  Monto total agregado (L):                 {monto_total:>12,.2f}")

    for origen in ["BXI", "MULTIPAGO"]:
        bloque = df[df["origen"] == origen].copy()
        if bloque.empty:
            print(f"\n  [Sin datos para {origen}]")
            continue
        _imprimir_bloque(bloque, origen, total_tx)

    print("=" * 110)


def main() -> None:
    print(f"Cargando datos desde: {QUERY_PATH}")
    try:
        inicio = time.perf_counter()
        df = cargar_datos()
        duracion = time.perf_counter() - inicio
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    imprimir_reporte(df)
    print(f"Tiempo de ejecucion: {duracion:.2f} segundos")


if __name__ == "__main__":
    main()
