"""
reporte_fondeo_mensual_feb_mar_2026.py
--------------------------------------
Reporte en consola para medir:
- cuentas de Cuenta Digital creadas del dia 1 al 15 de febrero y marzo 2026
- cuantas de esas cuentas tuvieron saldo > 0 al menos una vez
  durante el mes completo de su creacion

Ejecucion:
    python3 productos/Fondeo_CD/2026-04/programas_py/reporte_fondeo_mensual_feb_mar_2026.py
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
    / "CuentasCreadasVsFondeadasMensualFebMar2026.sql"
)


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(str(RUTA_QUERY))
    df.columns = [str(c) for c in df.columns]
    columnas_numericas = [
        "cuentas_creadas_1_15",
        "cuentas_fondeadas_al_menos_una_vez_en_mes_creacion",
        "cuentas_sin_fondear_en_mes_creacion",
        "tasa_fondeo_pct",
    ]
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def imprimir_reporte(df: pd.DataFrame) -> None:
    if df.empty:
        print("No se devolvieron registros para los periodos consultados.")
        return

    print("=" * 78)
    print("FONDEO CUENTA DIGITAL - ALTAS 1-15 Y FONDEO EN MES DE CREACION")
    print("=" * 78)

    for _, row in df.iterrows():
        mes = str(row.get("mes", "Mes sin nombre"))
        creadas = int(row.get("cuentas_creadas_1_15", 0) or 0)
        fondeadas = int(row.get("cuentas_fondeadas_al_menos_una_vez_en_mes_creacion", 0) or 0)
        sin_fondear = int(row.get("cuentas_sin_fondear_en_mes_creacion", 0) or 0)
        tasa = float(row.get("tasa_fondeo_pct", 0.0) or 0.0)

        print(f"\n{mes}")
        print("-" * 78)
        print(f"Cuentas creadas del 1 al 15:                        {creadas:>10,}")
        print(f"Cuentas fondeadas >=1 vez en mes de creacion:       {fondeadas:>10,}")
        print(f"Cuentas sin fondeo en mes de creacion:              {sin_fondear:>10,}")
        print(f"Tasa de fondeo:                                     {tasa:>9.2f}%")

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
