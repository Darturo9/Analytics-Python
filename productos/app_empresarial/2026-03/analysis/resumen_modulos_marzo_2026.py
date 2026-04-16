"""
resumen_modulos_marzo_2026.py
-----------------------------
Imprime en consola, para marzo 2026:
- Cantidad de consultas y clientes unicos
- Cantidad de gestiones CRM y clientes unicos
- Cantidad de logins y clientes unicos
- Cantidad de transacciones y clientes unicos

Ejecucion:
    python3 productos/app_empresarial/2026-03/analysis/resumen_modulos_marzo_2026.py
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


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


def main() -> int:
    try:
        modulo = cargar_modulo_cruce()

        print(f"Leyendo query1: {modulo.QUERY1_PATH}")
        print(f"Leyendo query2: {modulo.QUERY2_PATH}")
        df1 = modulo.preparar_query1(modulo.cargar_query(modulo.QUERY1_PATH))
        df2 = modulo.preparar_query2(modulo.cargar_query(modulo.QUERY2_PATH))

        inicio = pd.Timestamp("2026-03-01")
        fin = pd.Timestamp("2026-04-01")

        # Logica solicitada:
        # 1) query principal = query1 (actividad)
        # 2) filtro de mes sobre fecha de query1 (marzo 2026)
        # 3) mantener solo clientes que existan en query2
        marzo_q1 = df1[(df1["fecha_q1"] >= inicio) & (df1["fecha_q1"] < fin)].copy()
        clientes_q2 = set(df2["padded_codigo_cliente"].astype(str).str.strip())
        marzo = marzo_q1[marzo_q1["padded_codigo_cliente"].isin(clientes_q2)].copy()

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
        return 0
    except SQLAlchemyError as exc:
        print(f"[ERROR] Fallo de base de datos: {exc}")
        return 1
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
