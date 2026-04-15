"""
reporte_saldos_empresariales_q1.py
----------------------------------
Ejecuta el resumen de saldos Q1 2026 para clientes empresariales y
muestra los resultados en consola.

Ejecucion:
    python3 productos/app_empresarial/2026-03/analysis/reporte_saldos_empresariales_q1.py
"""

import sys

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query_file


QUERY_PATH = "productos/app_empresarial/2026-03/queries/saldos_empresariales_q1_2026.sql"


def _fmt_int(value: float) -> str:
    return f"{int(round(value or 0)):,}"


def _fmt_dec(value: float) -> str:
    return f"{float(value or 0):,.2f}"


def cargar_resumen() -> pd.DataFrame:
    df = run_query_file(QUERY_PATH)
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def main() -> None:
    print(f"Cargando datos desde: {QUERY_PATH}")

    try:
        df = cargar_resumen()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    if df.empty:
        print("[INFO] La query no devolvio filas.")
        raise SystemExit(0)

    row = df.iloc[0]

    universo = row.get("clientes_empresariales_universo", 0)
    con_saldo = row.get("clientes_empresariales_con_saldo_q1", 0)
    sin_saldo = row.get("clientes_sin_saldo_q1", 0)
    pct_con_saldo = row.get("pct_clientes_con_saldo_q1", 0)
    saldo_cierre_total = row.get("saldo_total_cierre_31_mar", 0)
    saldo_cierre_prom = row.get("saldo_promedio_cierre_31_mar_por_cliente", 0)
    saldo_prom_q1 = row.get("promedio_q1_saldo_cierre_por_cliente", 0)
    saldo_prom_contable_q1 = row.get("promedio_q1_saldo_promedio_contable_por_cliente", 0)
    saldo_max_q1 = row.get("promedio_q1_saldo_maximo_por_cliente", 0)
    saldo_acumulado_q1 = row.get("saldo_acumulado_q1_todos_los_dias", 0)
    dias_con_saldo_q1 = row.get("total_dias_con_saldo_en_q1", 0)

    print("\n============================================================")
    print(" SALDOS Q1 2026 - CLIENTES EMPRESARIALES")
    print("============================================================")
    print(f"Universo empresarial evaluado:           {_fmt_int(universo)}")
    print(f"Clientes con saldo en Q1:                {_fmt_int(con_saldo)}")
    print(f"Clientes sin saldo en Q1:                {_fmt_int(sin_saldo)}")
    print(f"% clientes con saldo Q1:                 {_fmt_dec(pct_con_saldo)}%")
    print("------------------------------------------------------------")
    print(f"Saldo total al cierre (31-mar):          L {_fmt_dec(saldo_cierre_total)}")
    print(f"Saldo promedio al cierre por cliente:    L {_fmt_dec(saldo_cierre_prom)}")
    print(f"Promedio Q1 de saldo cierre por cliente: L {_fmt_dec(saldo_prom_q1)}")
    print(f"Promedio Q1 saldo contable por cliente:  L {_fmt_dec(saldo_prom_contable_q1)}")
    print(f"Promedio Q1 saldo maximo por cliente:    L {_fmt_dec(saldo_max_q1)}")
    print(f"Saldo acumulado Q1 (todos los dias):     L {_fmt_dec(saldo_acumulado_q1)}")
    print(f"Total dias con saldo en Q1:              {_fmt_int(dias_con_saldo_q1)}")
    print("============================================================\n")

    print("Detalle tecnico (fila devuelta por SQL):")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
