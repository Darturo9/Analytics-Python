"""
diagnostico_clientes_conmodulos_vs_validar.py
---------------------------------------------
Compara dos archivos de clientes:
- Clientes_Marzo2026_ConModulos.xlsx
- Clientes_Marzo2026_ConModulos_Validar.xlsx

Objetivo:
1) Identificar clientes que estan en Validar y no en ConModulos.
2) Diagnosticar esos clientes faltantes con filtros de las queries:
   - Actividad en query1 para marzo 2026.
   - Existencia en universo query2 (clientes desde 2025-05-01).
   - Modulo objetivo tras aplicar los filtros.

Ejecucion:
    python3 productos/app_empresarial/2026-03/analysis/diagnostico_clientes_conmodulos_vs_validar.py
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query_file
from core.utils import exportar_excel_multi


EXPORTS_DIR = PROJECT_ROOT / "productos" / "app_empresarial" / "2026-03" / "exports"
FILE_CONMOD = EXPORTS_DIR / "Clientes_Marzo2026_ConModulos.xlsx"
FILE_VALIDAR = EXPORTS_DIR / "Clientes_Marzo2026_ConModulos_Validar.xlsx"
FILE_SALIDA = EXPORTS_DIR / "Diagnostico_Diferencias_ConModulos_vs_Validar.xlsx"

QUERY2_CLIENTES_PATH = (
    PROJECT_ROOT
    / "productos"
    / "app_empresarial"
    / "2026-03"
    / "queries"
    / "query2_clientes_desde_2025_05_01.sql"
)

MODULOS_OBJETIVO = ["Consulta", "Gestiones CRM", "Login", "Transacción"]


def cargar_modulo_cruce():
    script_cruce = Path(__file__).resolve().parent / "cruce_query1_query2_modulos.py"
    spec = importlib.util.spec_from_file_location("cruce_modulos", script_cruce)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar el modulo: {script_cruce}")
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


def normalizar_codigo_cliente(value: object) -> object:
    if pd.isna(value):
        return pd.NA

    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return pd.NA

    text = re.sub(r"\.0$", "", text)
    letter_match = re.search(r"[A-Za-z]", text)
    if letter_match:
        if letter_match.start() == 0:
            return pd.NA
        text = text[: letter_match.start()]

    text = re.sub(r"[^0-9]", "", text)
    if text == "":
        return pd.NA

    return text.zfill(8)[-8:]


def detectar_columna_cliente(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    cols_map = {str(c).strip().lower(): str(c) for c in cols}

    candidatos = [
        "codigo_cliente",
        "cod_cliente",
        "padded_codigo_cliente",
        "clccli",
        "cif",
        "codigo",
        "cliente",
    ]
    for c in candidatos:
        if c in cols_map:
            return cols_map[c]

    for c in cols:
        low = str(c).strip().lower()
        if "cliente" in low or "codigo" in low or "cif" in low:
            return str(c)

    raise ValueError(f"No se encontro una columna de cliente. Columnas disponibles: {cols}")


def cargar_clientes_unicos_excel(path: Path) -> tuple[pd.DataFrame, str]:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")

    df = pd.read_excel(path, dtype=str)
    if df.empty:
        raise ValueError(f"El archivo esta vacio: {path}")

    col_cliente = detectar_columna_cliente(df)
    out = pd.DataFrame(
        {
            "codigo_cliente_raw": df[col_cliente],
        }
    )
    out["codigo_cliente"] = out["codigo_cliente_raw"].apply(normalizar_codigo_cliente)
    out = out.dropna(subset=["codigo_cliente"]).copy()
    out = out.drop_duplicates(subset=["codigo_cliente"]).copy()
    out = out.sort_values("codigo_cliente").reset_index(drop=True)
    return out, col_cliente


def preparar_query1_minimo(df1: pd.DataFrame, modulo) -> pd.DataFrame:
    col_cliente = modulo.buscar_columna(df1, "padded_codigo_cliente", "codigo_cliente", "clccli")
    col_fecha = modulo.buscar_columna(df1, "fecha")
    col_modulo = modulo.buscar_columna(df1, "modulo", "modulo (grupo) (grupo)", "modulo (grupo)")
    col_operacion = modulo.buscar_columna(df1, "operación", "operacion", "operación (consulta sql personalizada)")

    out = pd.DataFrame(
        {
            "codigo_cliente": df1[col_cliente].astype(str).str.strip(),
            "fecha_q1": pd.to_datetime(df1[col_fecha], errors="coerce"),
            "modulo_original": df1[col_modulo],
            "operacion_original": df1[col_operacion],
        }
    )
    out = out[out["codigo_cliente"] != ""].copy()
    out = out.dropna(subset=["fecha_q1"]).copy()
    return out


def join_modulos(values: pd.Series) -> str:
    vals = sorted({str(v).strip() for v in values if pd.notna(v) and str(v).strip() != ""})
    return ", ".join(vals)


def construir_diagnostico_filtros(faltantes: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    modulo = cargar_modulo_cruce()
    raw_q1 = modulo.cargar_query(modulo.QUERY1_PATH)
    raw_q2 = run_query_file(str(QUERY2_CLIENTES_PATH))

    df1 = preparar_query1_minimo(raw_q1, modulo)
    df2 = modulo.preparar_query2(raw_q2)

    inicio = pd.Timestamp("2026-03-01")
    fin = pd.Timestamp("2026-04-01")

    q1_marzo = df1[(df1["fecha_q1"] >= inicio) & (df1["fecha_q1"] < fin)].copy()
    q1_marzo["modulo_regla"] = q1_marzo.apply(
        lambda r: modulo.clasificar_modulo(r["modulo_original"], r["operacion_original"]),
        axis=1,
    )

    clientes_q2 = set(df2["padded_codigo_cliente"].astype(str).str.strip())
    q1_marzo_q2 = q1_marzo[q1_marzo["codigo_cliente"].isin(clientes_q2)].copy()
    q1_marzo_q2_obj = q1_marzo_q2[q1_marzo_q2["modulo_regla"].isin(MODULOS_OBJETIVO)].copy()

    agg_q1 = (
        q1_marzo.groupby("codigo_cliente", as_index=False)
        .agg(registros_q1_marzo=("codigo_cliente", "size"))
    )
    agg_q1_q2 = (
        q1_marzo_q2.groupby("codigo_cliente", as_index=False)
        .agg(registros_q1_marzo_q2=("codigo_cliente", "size"))
    )
    agg_obj = (
        q1_marzo_q2_obj.groupby("codigo_cliente", as_index=False)
        .agg(
            registros_modulo_objetivo=("codigo_cliente", "size"),
            modulos_detectados=("modulo_regla", join_modulos),
        )
    )

    diag = faltantes[["codigo_cliente"]].copy()
    diag = diag.merge(agg_q1, on="codigo_cliente", how="left")
    diag = diag.merge(agg_q1_q2, on="codigo_cliente", how="left")
    diag = diag.merge(agg_obj, on="codigo_cliente", how="left")

    diag["en_query1_marzo"] = diag["registros_q1_marzo"].fillna(0).gt(0).astype(int)
    diag["en_query2_universo"] = diag["codigo_cliente"].isin(clientes_q2).astype(int)
    diag["pasa_filtro_query1_y_query2"] = diag["registros_q1_marzo_q2"].fillna(0).gt(0).astype(int)
    diag["tiene_modulo_objetivo"] = diag["registros_modulo_objetivo"].fillna(0).gt(0).astype(int)

    condiciones = [
        diag["en_query1_marzo"].eq(0),
        diag["pasa_filtro_query1_y_query2"].eq(0),
        diag["tiene_modulo_objetivo"].eq(0),
    ]
    valores = [
        "No aparece en query1 durante marzo 2026",
        "Tiene actividad en query1 marzo, pero no existe en universo query2",
        "Pasa query1+query2, pero no cae en modulo objetivo",
    ]
    diag["motivo_probable"] = np.select(
        condiciones,
        valores,
        default="Pasa todos los filtros (revisar diferencias de archivo/normalizacion)",
    )

    diag["registros_q1_marzo"] = diag["registros_q1_marzo"].fillna(0).astype(int)
    diag["registros_q1_marzo_q2"] = diag["registros_q1_marzo_q2"].fillna(0).astype(int)
    diag["registros_modulo_objetivo"] = diag["registros_modulo_objetivo"].fillna(0).astype(int)
    diag["modulos_detectados"] = diag["modulos_detectados"].fillna("")

    resumen_filtros = {
        "clientes_faltantes_total": int(len(diag)),
        "faltantes_sin_query1_marzo": int((diag["en_query1_marzo"] == 0).sum()),
        "faltantes_fuera_query2": int(
            ((diag["en_query1_marzo"] == 1) & (diag["pasa_filtro_query1_y_query2"] == 0)).sum()
        ),
        "faltantes_sin_modulo_objetivo": int(
            ((diag["pasa_filtro_query1_y_query2"] == 1) & (diag["tiene_modulo_objetivo"] == 0)).sum()
        ),
        "faltantes_que_pasan_todo": int((diag["tiene_modulo_objetivo"] == 1).sum()),
    }

    diag = diag.sort_values(
        ["motivo_probable", "registros_q1_marzo", "registros_q1_marzo_q2", "codigo_cliente"],
        ascending=[True, False, False, True],
    ).reset_index(drop=True)
    return diag, resumen_filtros


def main() -> int:
    try:
        print(f"Leyendo ConModulos: {FILE_CONMOD}")
        conmod, col_conmod = cargar_clientes_unicos_excel(FILE_CONMOD)
        print(f"Columna detectada en ConModulos: {col_conmod}")
        print(f"Clientes unicos ConModulos: {len(conmod):,}")

        print(f"Leyendo Validar: {FILE_VALIDAR}")
        validar, col_validar = cargar_clientes_unicos_excel(FILE_VALIDAR)
        print(f"Columna detectada en Validar: {col_validar}")
        print(f"Clientes unicos Validar: {len(validar):,}")

        set_conmod = set(conmod["codigo_cliente"])
        set_validar = set(validar["codigo_cliente"])

        faltantes = pd.DataFrame({"codigo_cliente": sorted(set_validar - set_conmod)})
        extras = pd.DataFrame({"codigo_cliente": sorted(set_conmod - set_validar)})
        comunes = len(set_conmod & set_validar)

        print("\nResumen diferencias de archivos")
        print(f"- En Validar y NO en ConModulos (faltantes): {len(faltantes):,}")
        print(f"- En ConModulos y NO en Validar (extras):    {len(extras):,}")
        print(f"- Clientes comunes:                            {comunes:,}")

        print("\nAplicando diagnostico con filtros de query1/query2...")
        diagnostico_faltantes, resumen_filtros = construir_diagnostico_filtros(faltantes)

        print("\nResumen diagnostico de faltantes")
        for k, v in resumen_filtros.items():
            print(f"- {k}: {v:,}")

        resumen_general = pd.DataFrame(
            [
                {"metrica": "clientes_unicos_conmodulos", "valor": int(len(conmod))},
                {"metrica": "clientes_unicos_validar", "valor": int(len(validar))},
                {"metrica": "clientes_comunes", "valor": int(comunes)},
                {"metrica": "faltantes_en_conmodulos", "valor": int(len(faltantes))},
                {"metrica": "extras_en_conmodulos", "valor": int(len(extras))},
                {
                    "metrica": "pct_faltantes_sobre_validar",
                    "valor": round(100.0 * len(faltantes) / len(validar), 4) if len(validar) else 0.0,
                },
                {"metrica": "columna_detectada_conmodulos", "valor": col_conmod},
                {"metrica": "columna_detectada_validar", "valor": col_validar},
            ]
        )

        resumen_filtros_df = pd.DataFrame(
            [{"metrica": k, "valor": v} for k, v in resumen_filtros.items()]
        )
        resumen = pd.concat([resumen_general, resumen_filtros_df], ignore_index=True)

        sheets = {
            "resumen": resumen,
            "faltantes_en_conmodulos": faltantes,
            "extras_en_conmodulos": extras,
            "diagnostico_filtros": diagnostico_faltantes,
        }
        sheets = {k: v.rename(columns={c: str(c) for c in v.columns}) for k, v in sheets.items()}

        exportar_excel_multi(sheets, str(FILE_SALIDA))
        print(f"\nArchivo diagnostico generado: {FILE_SALIDA}")
        return 0
    except SQLAlchemyError as exc:
        print(f"[ERROR] Fallo de base de datos: {exc}")
        return 1
    except Exception as exc:
        print(f"[ERROR] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
