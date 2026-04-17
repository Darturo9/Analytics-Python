"""
reporte_movimientos_cuentas_creadas_marzo_2026.py
-------------------------------------------------
Imprime en consola:
- cuantas cuentas de Cuenta Digital creadas en marzo 2026 tuvieron movimientos
- cuantas transacciones acumulan esas cuentas

Ejecucion:
    python3 productos/Fondeo_CD/2026-03/programas_py/reporte_movimientos_cuentas_creadas_marzo_2026.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query_file


RUTA_QUERY = (
    PROJECT_ROOT
    / "productos"
    / "Fondeo_CD"
    / "2026-03"
    / "queries"
    / "CuentasCreadasMarzoMovimientos.sql"
)


def _fmt_int(value: float | int) -> str:
    return f"{int(round(float(value or 0))):,}"


def _fmt_dec(value: float | int) -> str:
    return f"{float(value or 0):,.2f}"


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(str(RUTA_QUERY))
    df.columns = [str(c) for c in df.columns]
    columnas_numericas = [
        "cuentas_creadas_marzo",
        "cuentas_con_movimiento",
        "cuentas_sin_movimiento",
        "pct_cuentas_con_movimiento",
        "total_transacciones",
        "promedio_transacciones_por_cuenta",
        "max_transacciones_en_una_cuenta",
    ]
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def imprimir_reporte(df: pd.DataFrame) -> None:
    if df.empty:
        print("No se devolvieron registros para marzo 2026.")
        return

    row = df.iloc[0]
    creadas = row.get("cuentas_creadas_marzo", 0)
    con_mov = row.get("cuentas_con_movimiento", 0)
    sin_mov = row.get("cuentas_sin_movimiento", 0)
    pct_mov = row.get("pct_cuentas_con_movimiento", 0.0)
    total_tx = row.get("total_transacciones", 0)
    prom_tx = row.get("promedio_transacciones_por_cuenta", 0.0)
    max_tx = row.get("max_transacciones_en_una_cuenta", 0)

    print("=" * 78)
    print("MOVIMIENTOS EN CUENTAS CREADAS EN MARZO 2026 - CUENTA DIGITAL")
    print("=" * 78)
    print(f"Cuentas creadas en marzo 2026:                 {_fmt_int(creadas)}")
    print(f"Cuentas con movimiento (> 0 trx):              {_fmt_int(con_mov)}")
    print(f"Cuentas sin movimiento:                         {_fmt_int(sin_mov)}")
    print(f"% cuentas con movimiento:                       {_fmt_dec(pct_mov)}%")
    print("-" * 78)
    print(f"Total de transacciones (ctctrx):               {_fmt_int(total_tx)}")
    print(f"Promedio de transacciones por cuenta:          {_fmt_dec(prom_tx)}")
    print(f"Maximo de transacciones en una cuenta:         {_fmt_int(max_tx)}")
    print("=" * 78)


def main() -> None:
    print(f"Cargando query: {RUTA_QUERY}")
    try:
        df = cargar_datos()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    imprimir_reporte(df)


if __name__ == "__main__":
    main()

