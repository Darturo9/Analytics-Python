"""
reporte_fondeadas_genero_qbr1.py
--------------------------------
Reporte de consola para cuentas fondeadas por genero en Q1 2026.

Ejecucion:
    python3 productos/Fondeo_CD/Dashboards/reporte_fondeadas_genero_qbr1.py
"""

import sys

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query_file


QUERY_PATH = "productos/Fondeo_CD/Queries/FondeadasGeneroQ1.sql"


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH)
    df.columns = [str(c).strip().lower() for c in df.columns]
    df["genero"] = df.get("genero", pd.Series(dtype="string")).astype(str).str.strip().str.upper()
    df["cuentas_fondeadas"] = pd.to_numeric(df.get("cuentas_fondeadas"), errors="coerce").fillna(0).astype(int)
    return df.sort_values("cuentas_fondeadas", ascending=False).reset_index(drop=True)


def obtener_conteo(df: pd.DataFrame, genero: str) -> int:
    base = df[df["genero"] == genero]
    if base.empty:
        return 0
    return int(base["cuentas_fondeadas"].sum())


def main() -> None:
    print(f"Cargando datos desde: {QUERY_PATH}")
    try:
        df = cargar_datos()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    total = int(df["cuentas_fondeadas"].sum()) if not df.empty else 0
    mujeres = obtener_conteo(df, "MUJER")
    hombres = obtener_conteo(df, "HOMBRE")
    sin_dato = obtener_conteo(df, "SIN_DATO")

    pct_mujeres = (mujeres / total * 100) if total > 0 else 0.0
    pct_hombres = (hombres / total * 100) if total > 0 else 0.0
    pct_sin_dato = (sin_dato / total * 100) if total > 0 else 0.0

    print("\n==================================================")
    print("   CUENTAS FONDEADAS Q1 2026 - GENERO")
    print("==================================================")
    print(f"Total cuentas fondeadas: {total:,}")
    print(f"Mujeres:                 {mujeres:,} ({pct_mujeres:,.1f}%)")
    print(f"Hombres:                 {hombres:,} ({pct_hombres:,.1f}%)")
    print(f"Sin dato:                {sin_dato:,} ({pct_sin_dato:,.1f}%)")
    print("==================================================\n")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
