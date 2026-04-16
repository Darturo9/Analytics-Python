"""
reporte_fondeo_quincena_feb_mar_2026.py
---------------------------------------
Reporte en consola para medir, por quincena (1-15):
- cuentas de Cuenta Digital creadas en febrero 2026 y marzo 2026
- cuantas de esas cuentas tuvieron saldo > 0 al menos una vez
  durante esa misma quincena

Ejecucion:
    python3 productos/Fondeo_CD/2026-04/programas_py/reporte_fondeo_quincena_feb_mar_2026.py
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
    / "CuentasCreadasVsFondeadasQuincenaFebMar2026.sql"
)


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(str(RUTA_QUERY))
    df.columns = [str(c) for c in df.columns]
    columnas_numericas = [
        "cuentas_creadas_quincena",
        "cuentas_fondeadas_al_menos_una_vez_quincena",
        "cuentas_sin_fondear_quincena",
        "tasa_fondeo_pct",
    ]
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def imprimir_reporte(df: pd.DataFrame) -> None:
    if df.empty:
        print("No se devolvieron registros para las quincenas consultadas.")
        return

    print("=" * 78)
    print("FONDEO CUENTA DIGITAL - QUINCENA (1-15) FEBRERO Y MARZO 2026")
    print("=" * 78)

    for _, row in df.iterrows():
        mes = str(row.get("mes", "Mes sin nombre"))
        creadas = int(row.get("cuentas_creadas_quincena", 0) or 0)
        fondeadas = int(row.get("cuentas_fondeadas_al_menos_una_vez_quincena", 0) or 0)
        sin_fondear = int(row.get("cuentas_sin_fondear_quincena", 0) or 0)
        tasa = float(row.get("tasa_fondeo_pct", 0.0) or 0.0)

        print(f"\n{mes}")
        print("-" * 78)
        print(f"Cuentas creadas en la quincena:                     {creadas:>10,}")
        print(f"Cuentas con fondos al menos 1 vez en quincena:      {fondeadas:>10,}")
        print(f"Cuentas sin fondear en la quincena:                 {sin_fondear:>10,}")
        print(f"Tasa de fondeo de la quincena:                      {tasa:>9.2f}%")

    print("\n" + "=" * 78)


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
