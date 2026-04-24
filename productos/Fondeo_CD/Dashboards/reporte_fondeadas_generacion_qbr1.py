"""
reporte_fondeadas_generacion_qbr1.py
------------------------------------
Reporte de consola para clientes/cuentas fondeadas por generacion en Q1 2026.

Ejecucion:
    python3 productos/Fondeo_CD/Dashboards/reporte_fondeadas_generacion_qbr1.py
"""

import sys

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query_file


QUERY_PATH = "productos/Fondeo_CD/Queries/FondeadasGeneracionQ1.sql"


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH)
    df.columns = [str(c).strip().lower() for c in df.columns]

    df["generacion"] = (
        df.get("generacion", pd.Series(dtype="string"))
        .astype(str)
        .str.strip()
        .replace("", "SIN DATO")
    )
    df["clientes_fondeadores"] = pd.to_numeric(
        df.get("clientes_fondeadores"), errors="coerce"
    ).fillna(0).astype(int)
    df["cuentas_fondeadas"] = pd.to_numeric(
        df.get("cuentas_fondeadas"), errors="coerce"
    ).fillna(0).astype(int)

    return df.sort_values(["cuentas_fondeadas", "generacion"], ascending=[False, True]).reset_index(drop=True)


def preparar_tabla(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    total_clientes = int(out["clientes_fondeadores"].sum()) if not out.empty else 0
    total_cuentas = int(out["cuentas_fondeadas"].sum()) if not out.empty else 0

    out["pct_clientes"] = (
        (out["clientes_fondeadores"] / total_clientes * 100).round(2) if total_clientes else 0.0
    )
    out["pct_cuentas"] = (
        (out["cuentas_fondeadas"] / total_cuentas * 100).round(2) if total_cuentas else 0.0
    )

    out_show = out.copy()
    out_show["clientes_fondeadores"] = out_show["clientes_fondeadores"].map(lambda x: f"{int(x):,}")
    out_show["cuentas_fondeadas"] = out_show["cuentas_fondeadas"].map(lambda x: f"{int(x):,}")
    out_show["pct_clientes"] = out_show["pct_clientes"].map(lambda x: f"{x:.2f}%")
    out_show["pct_cuentas"] = out_show["pct_cuentas"].map(lambda x: f"{x:.2f}%")

    return out_show


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

    total_clientes = int(df["clientes_fondeadores"].sum()) if not df.empty else 0
    total_cuentas = int(df["cuentas_fondeadas"].sum()) if not df.empty else 0

    print("\n============================================================")
    print("   FONDEO Q1 2026 - GENERACION DE CLIENTES")
    print("============================================================")
    print(f"Total clientes fondeadores unicos: {total_clientes:,}")
    print(f"Total cuentas fondeadas:          {total_cuentas:,}")
    print("============================================================\n")

    if df.empty:
        print("Sin datos para mostrar.")
        return

    tabla = preparar_tabla(df)
    print(tabla.to_string(index=False))


if __name__ == "__main__":
    main()
