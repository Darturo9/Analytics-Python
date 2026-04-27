"""
top5_departamentos_clientes_q1.py
----------------------------------
Carga el listado de clientes empresariales del Q1 2026 y muestra en
consola el Top 5 de departamentos segun la columna DW_NIVEL_GEO2 (Depto).

Ejecucion:
    python3 productos/app_empresarial/2026-03/analysis/top5_departamentos_clientes_q1.py
"""

import sys

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query_file


QUERY_PATH = "productos/app_empresarial/2026-03/queries/query1.sql"


def cargar_clientes() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH)
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def main() -> None:
    print(f"Cargando datos desde: {QUERY_PATH}")

    try:
        df = cargar_clientes()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    if df.empty:
        print("[INFO] La query no devolvio filas.")
        raise SystemExit(0)

    if "depto" not in df.columns:
        print(f"[ERROR] Columna 'depto' no encontrada. Columnas disponibles: {list(df.columns)}")
        raise SystemExit(1)

    top5 = (
        df["depto"]
        .fillna("SIN DATOS")
        .str.strip()
        .value_counts()
        .head(5)
        .reset_index()
    )
    top5.columns = ["departamento", "clientes"]
    top5.index = range(1, len(top5) + 1)

    total = len(df)

    print("\n============================================================")
    print(" TOP 5 DEPARTAMENTOS - CLIENTES EMPRESARIALES Q1 2026")
    print("============================================================")
    print(f"{'#':<4} {'Departamento':<30} {'Clientes':>10} {'%':>8}")
    print("------------------------------------------------------------")
    for rank, row in top5.iterrows():
        pct = row["clientes"] / total * 100
        print(f"{rank:<4} {row['departamento']:<30} {row['clientes']:>10,} {pct:>7.1f}%")
    print("------------------------------------------------------------")
    print(f"{'Total clientes en listado Q1:':<35} {total:>10,}")
    print("============================================================\n")


if __name__ == "__main__":
    main()
