"""
reporte_fondeo_abril_2025_1a15.py
---------------------------------
Reporte en consola para medir:
- cuentas de Cuenta Digital creadas del 1 al 15 de abril 2025
- cuantas de esas cuentas tuvieron saldo > 0 al menos una vez
  dentro del mismo rango (1 al 15 de abril 2025)

Ejecucion:
    python3 productos/Fondeo_CD/2026-04/programas_py/reporte_fondeo_abril_2025_1a15.py
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
    / "2026-04"
    / "queries"
    / "CuentasCreadasVsFondeadasAbril2025_1a15.sql"
)


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(str(RUTA_QUERY))
    df.columns = [str(c) for c in df.columns]
    columnas_numericas = [
        "cuentas_creadas_1_15",
        "cuentas_fondeadas_1_15",
        "cuentas_sin_fondear_1_15",
        "tasa_fondeo_pct",
    ]
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def imprimir_reporte(df: pd.DataFrame) -> None:
    if df.empty:
        print("No se devolvieron registros para abril 2025 (1-15).")
        return

    row = df.iloc[0]
    periodo = str(row.get("periodo", "Abril 2025 (1-15)"))
    creadas = int(row.get("cuentas_creadas_1_15", 0) or 0)
    fondeadas = int(row.get("cuentas_fondeadas_1_15", 0) or 0)
    sin_fondear = int(row.get("cuentas_sin_fondear_1_15", 0) or 0)
    tasa = float(row.get("tasa_fondeo_pct", 0.0) or 0.0)

    print("=" * 78)
    print("FONDEO CUENTA DIGITAL - ABRIL 2025 (1-15)")
    print("=" * 78)
    print(f"Periodo:                                           {periodo}")
    print(f"Cuentas creadas del 1 al 15:                        {creadas:>10,}")
    print(f"Cuentas fondeadas >=1 vez del 1 al 15:              {fondeadas:>10,}")
    print(f"Cuentas sin fondeo del 1 al 15:                     {sin_fondear:>10,}")
    print(f"Tasa de fondeo:                                     {tasa:>9.2f}%")
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
