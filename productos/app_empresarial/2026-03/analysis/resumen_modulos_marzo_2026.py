"""
resumen_modulos_marzo_2026.py
-----------------------------
Imprime en consola, para marzo 2026:
- Cantidad de consultas y clientes unicos
- Cantidad de gestiones CRM y clientes unicos
- Cantidad de logins y clientes unicos
- Cantidad de transacciones y clientes unicos

Regla de datos:
- Actividad principal desde query1 filtrada a marzo 2026.
- Universo de clientes desde query2 con inicio 2025-05-01.

Ejecucion:
    python3 productos/app_empresarial/2026-03/analysis/resumen_modulos_marzo_2026.py
"""

from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query_file

QUERY2_CLIENTES_PATH = (
    PROJECT_ROOT
    / "productos"
    / "app_empresarial"
    / "2026-03"
    / "queries"
    / "query2_clientes_desde_2025_05_01.sql"
)


def cargar_modulo_cruce():
    script_cruce = Path(__file__).resolve().parent / "cruce_query1_query2_modulos.py"
    spec = importlib.util.spec_from_file_location("cruce_modulos", script_cruce)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar el modulo: {script_cruce}")
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


def construir_resumen(df: pd.DataFrame) -> pd.DataFrame:
    categorias = ["Consulta", "Gestiones CRM", "Login", "Transacción"]
    resumen = (
        df[df["modulo_regla"].isin(categorias)]
        .groupby("modulo_regla", as_index=False)
        .agg(
            cantidad=("padded_codigo_cliente", "size"),
            clientes_unicos=("padded_codigo_cliente", "nunique"),
        )
    )

    base = pd.DataFrame({"modulo_regla": categorias})
    resumen = base.merge(resumen, on="modulo_regla", how="left").fillna(0)
    resumen["cantidad"] = resumen["cantidad"].astype(int)
    resumen["clientes_unicos"] = resumen["clientes_unicos"].astype(int)
    return resumen


def preparar_query1_minimo(df1: pd.DataFrame, modulo) -> pd.DataFrame:
    col_cliente = modulo.buscar_columna(df1, "padded_codigo_cliente", "codigo_cliente", "clccli")
    col_fecha = modulo.buscar_columna(df1, "fecha")
    col_modulo = modulo.buscar_columna(df1, "modulo", "modulo (grupo) (grupo)", "modulo (grupo)")
    col_operacion = modulo.buscar_columna(df1, "operación", "operacion", "operación (consulta sql personalizada)")

    out = pd.DataFrame(
        {
            "padded_codigo_cliente": df1[col_cliente].astype(str).str.strip(),
            "fecha_q1": pd.to_datetime(df1[col_fecha], errors="coerce"),
            "modulo_original": df1[col_modulo],
            "operacion_original": df1[col_operacion],
        }
    )
    out = out[out["padded_codigo_cliente"] != ""].copy()
    out = out.dropna(subset=["fecha_q1"]).copy()
    return out


def main() -> int:
    try:
        t0 = time.perf_counter()
        modulo = cargar_modulo_cruce()

        print(f"Leyendo query1: {modulo.QUERY1_PATH}")
        print(f"Leyendo universo de clientes (query2): {QUERY2_CLIENTES_PATH}")
        raw_q1 = modulo.cargar_query(modulo.QUERY1_PATH)
        raw_q2 = run_query_file(str(QUERY2_CLIENTES_PATH))
        t1 = time.perf_counter()

        # Optimizacion: no calculamos modulo_regla sobre todo query1.
        # Primero reducimos a columnas minimas y filtramos; luego clasificamos.
        df1 = preparar_query1_minimo(raw_q1, modulo)
        df2 = modulo.preparar_query2(raw_q2)

        inicio = pd.Timestamp("2026-03-01")
        fin = pd.Timestamp("2026-04-01")

        # Logica solicitada:
        # 1) query principal = query1 (actividad)
        # 2) filtro de mes sobre fecha de query1 (marzo 2026)
        # 3) mantener solo clientes que existan en query2
        marzo_q1 = df1[(df1["fecha_q1"] >= inicio) & (df1["fecha_q1"] < fin)].copy()
        clientes_q2 = set(df2["padded_codigo_cliente"].astype(str).str.strip())
        marzo = marzo_q1[marzo_q1["padded_codigo_cliente"].isin(clientes_q2)].copy()
        marzo["modulo_regla"] = marzo.apply(
            lambda r: modulo.clasificar_modulo(r["modulo_original"], r["operacion_original"]),
            axis=1,
        )
        t2 = time.perf_counter()

        print()
        print("Resumen marzo 2026")
        print("Base principal: Query1 | Universo de clientes: Query2")
        print(f"Registros marzo en Query1 (antes de cruce): {len(marzo_q1):,}")
        print(f"Clientes unicos marzo Query1 (antes de cruce): {marzo_q1['padded_codigo_cliente'].nunique():,}")
        print(f"Clientes unicos disponibles en Query2: {len(clientes_q2):,}")
        print(f"Registros marzo tras filtrar clientes de Query2: {len(marzo):,}")
        print(f"Clientes unicos marzo tras filtrar clientes de Query2: {marzo['padded_codigo_cliente'].nunique():,}")
        print()

        resumen = construir_resumen(marzo)
        print(resumen.to_string(index=False))
        print()
        print(f"Tiempo carga SQL:        {t1 - t0:.2f}s")
        print(f"Tiempo proceso Python:   {t2 - t1:.2f}s")
        print(f"Tiempo total:            {t2 - t0:.2f}s")
        return 0
    except SQLAlchemyError as exc:
        print(f"[ERROR] Fallo de base de datos: {exc}")
        return 1
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
