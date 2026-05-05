"""
reporte_fondeo_comparativo_feb_mar_abr_2026.py
-----------------------------------------------
Comparativo en consola para febrero, marzo y abril 2026:
- cuentas de Cuenta Digital creadas por mes
- cuantas de esas cuentas tuvieron al menos 1 dia con fondos
  dentro del mismo mes de creacion

Ejecucion:
    python3 productos/Fondeo_CD/2026-04/programas_py/reporte_fondeo_comparativo_feb_mar_abr_2026.py
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
    / "CuentasCreadasVsFondeadasComparativoFebMarAbr2026.sql"
)


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(str(RUTA_QUERY))
    df.columns = [str(c) for c in df.columns]

    columnas_numericas = [
        "cuentas_creadas_mes",
        "cuentas_fondeadas_al_menos_1_dia_mes",
        "cuentas_sin_fondear_mes",
        "tasa_fondeo_pct",
    ]
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def imprimir_reporte(df: pd.DataFrame) -> None:
    if df.empty:
        print("No se devolvieron registros para febrero, marzo y abril 2026.")
        return

    print("=" * 92)
    print("COMPARATIVO FONDEO CUENTA DIGITAL - FEBRERO, MARZO Y ABRIL 2026")
    print("=" * 92)

    for _, row in df.iterrows():
        mes = str(row.get("mes", "Mes sin nombre"))
        creadas = int(row.get("cuentas_creadas_mes", 0) or 0)
        fondeadas = int(row.get("cuentas_fondeadas_al_menos_1_dia_mes", 0) or 0)
        sin_fondear = int(row.get("cuentas_sin_fondear_mes", 0) or 0)
        tasa = float(row.get("tasa_fondeo_pct", 0.0) or 0.0)

        print(f"\n{mes}")
        print("-" * 92)
        print(f"Cuentas creadas en el mes:                          {creadas:>12,}")
        print(f"Cuentas con fondos >= 1 dia en el mismo mes:       {fondeadas:>12,}")
        print(f"Cuentas sin fondeo en el mismo mes:                {sin_fondear:>12,}")
        print(f"Tasa de fondeo:                                     {tasa:>11.2f}%")

    print("\n" + "=" * 92)


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
