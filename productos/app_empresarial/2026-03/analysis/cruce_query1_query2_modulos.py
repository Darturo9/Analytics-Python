"""
cruce_query1_query2_modulos.py
------------------------------
1) Ejecuta query1.sql y query2.sql.
2) Clasifica el tipo de modulo segun la regla compartida (Login/Consulta/Transaccion/Gestiones/Gestiones CRM).
3) Muestra primero en consola el valor calculado de esos modulos.
4) Cruza ambos resultados con:
   - codigo_cliente igual
   - fecha_query1 >= fecha_query2

Ejecucion:
    python3 productos/app_empresarial/2026-03/analysis/cruce_query1_query2_modulos.py
"""

from __future__ import annotations

import re
import sys
import time
import unicodedata
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query_file


QUERY1_PATH = (
    PROJECT_ROOT
    / "productos"
    / "app_empresarial"
    / "2026-03"
    / "queries"
    / "query1.sql"
)

QUERY2_PATH = (
    PROJECT_ROOT
    / "productos"
    / "app_empresarial"
    / "2026-03"
    / "queries"
    / "query2.sql"
)


CONSULTA_CODIGOS = {
    "app-hisptm",
    "app-edocta",
    "app-extcns",
    "app-cnsdiv",
    "adq-cnstrx",
    "ptr-cnssal",
    "cns-chqtrc",
    "chp-cnschq",
    "con-sal",
    "dei-cnpag",
    "mpg-ccnhp",
    "mpg-ccnspg",
    "con-sal2",
    "dei-cnsapg",
    "ptr-cnsptm",
    "cpr-lincre",
    "ptr-cnspar",
    "adq-edocta",
    "estado-cta",
    "res-ctacor",
    "seg-cnhtrx",
    "pym-cnsinf",
    "pym-cnsmov",
    "adq-cnsrec",
    "res-edoct2",
    "adq-voltrx",
    "app-cnasps",
    "app-cnshte",
    "app-cntigo",
    "app-cnenee",
    "app-ccnspg",
    "app-grptmo",
    "cns-ptmcap",
    "cns-ptmos",
    "adm-cnsamn",
    "cns-cdv2",
    "cns-asps",
    "cns-enee",
    "cns-tigo",
    "cns-hndtl",
    "adm-cnsilp",
    "ptr-grptmo",
    "cns-ptmos2",
    "cns-vdv2",
    "pym-desemb",
}

GESTIONES_CODIGOS = {
    "transf-int",
    "prf-recosm",
    "prf-receml",
    "app-traint",
}

MODULOS_TRANSACCION = {
    "ach",
    "ach qr",
    "dividelo todo",
    "limites de transferencia",
    "limite por usuario y categoria",
    "planilla",
    "proveedores",
    "multipagos",
}

MODULOS_GESTIONES = {
    "aprovisionamiento tc-td",
    "bloqueo y desbloqueo tc",
}


def normalizar_texto(valor: object) -> str:
    if valor is None:
        return ""
    texto = str(valor).strip()
    if texto.lower() == "nan":
        return ""
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = texto.lower()
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def extraer_codigo_operacion(operacion: object) -> str:
    op = normalizar_texto(operacion)
    if not op:
        return ""
    partes = [p.strip() for p in op.split(" - ") if p.strip()]
    if not partes:
        return ""
    candidato = partes[-1]
    if re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)+", candidato):
        return candidato
    return ""


def buscar_columna(df: pd.DataFrame, *candidatos: str) -> str:
    mapa = {normalizar_texto(c): str(c) for c in df.columns}
    for c in candidatos:
        key = normalizar_texto(c)
        if key in mapa:
            return mapa[key]
    raise ValueError(f"No se encontro ninguna columna candidata {candidatos}. Columnas: {list(df.columns)}")


def clasificar_modulo(modulo: object, operacion: object) -> str:
    modulo_norm = normalizar_texto(modulo)
    codigo_op = extraer_codigo_operacion(operacion)

    if modulo_norm == "login":
        return "Login"
    if codigo_op in CONSULTA_CODIGOS:
        return "Consulta"
    if modulo_norm in MODULOS_TRANSACCION:
        return "Transacción"
    if modulo_norm in MODULOS_GESTIONES or codigo_op in GESTIONES_CODIGOS:
        return "Gestiones"
    if modulo_norm == "gestiones crm":
        return "Gestiones CRM"
    if modulo_norm == "trx journal":
        return "Transacción"
    if normalizar_texto(operacion) == "":
        return "Transacción"
    return "Sin clasificar"


def cargar_query(path_sql: Path) -> pd.DataFrame:
    df = run_query_file(str(path_sql))
    df.columns = [str(c).strip() for c in df.columns]
    return df


def preparar_query1(df1: pd.DataFrame) -> pd.DataFrame:
    col_cliente = buscar_columna(df1, "padded_codigo_cliente", "codigo_cliente", "clccli")
    col_fecha = buscar_columna(df1, "fecha")
    col_modulo = buscar_columna(df1, "modulo", "modulo (grupo) (grupo)", "modulo (grupo)")
    col_operacion = buscar_columna(df1, "operación", "operacion", "operación (consulta sql personalizada)")

    out = df1.copy()
    out["padded_codigo_cliente"] = out[col_cliente].astype(str).str.strip()
    out["fecha_q1"] = pd.to_datetime(out[col_fecha], errors="coerce")
    out["modulo_original"] = out[col_modulo]
    out["operacion_original"] = out[col_operacion]
    out["modulo_regla"] = out.apply(
        lambda r: clasificar_modulo(r["modulo_original"], r["operacion_original"]),
        axis=1,
    )

    out = out[out["padded_codigo_cliente"] != ""].copy()
    out = out.dropna(subset=["fecha_q1"]).copy()
    return out


def preparar_query2(df2: pd.DataFrame) -> pd.DataFrame:
    col_cliente = buscar_columna(df2, "padded_codigo_cliente")
    col_fecha = buscar_columna(df2, "fecha")

    out = df2.copy()
    out["padded_codigo_cliente"] = out[col_cliente].astype(str).str.strip()
    out["fecha_q2"] = pd.to_datetime(out[col_fecha], errors="coerce")
    out = out[out["padded_codigo_cliente"] != ""].copy()
    out = out.dropna(subset=["fecha_q2"]).copy()
    return out[["padded_codigo_cliente", "fecha_q2"]]


def imprimir_valores_modulo(df1: pd.DataFrame) -> None:
    print("\n============================================================")
    print(" VALOR CALCULADO DE MODULOS (SEGUN REGLA INDICADA)")
    print("============================================================")
    resumen = (
        df1["modulo_regla"]
        .value_counts(dropna=False)
        .rename_axis("modulo_regla")
        .reset_index(name="registros")
    )
    total = int(resumen["registros"].sum())
    resumen["porcentaje"] = (resumen["registros"] / total * 100.0).round(2) if total > 0 else 0.0
    print(resumen.to_string(index=False))
    print("============================================================\n")


def left_join_tableau(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Emula el LEFT JOIN de Tableau:
    query1 LEFT JOIN query2
      ON q1.padded_codigo_cliente = q2.padded_codigo_cliente
     AND q1.fecha_q1 >= q2.fecha_q2

    Comportamiento:
    - Conserva todos los registros de query1.
    - Si una fila de query1 no cumple la condicion con ninguna fila de query2,
      se conserva con columnas de query2 en NULL.
    - Si cumple con varias filas de query2, mantiene multiples filas (como SQL).
    """
    left = df1.reset_index(drop=True).reset_index().rename(columns={"index": "_q1_id"})
    base = left.merge(df2, on="padded_codigo_cliente", how="left")

    cumple_condicion = base["fecha_q2"].notna() & (base["fecha_q1"] >= base["fecha_q2"])
    empates = base[cumple_condicion].copy()

    ids_con_match = set(empates["_q1_id"].tolist())
    sin_match = left[~left["_q1_id"].isin(ids_con_match)].copy()
    sin_match["fecha_q2"] = pd.NaT

    resultado = pd.concat([empates, sin_match], ignore_index=True, sort=False)
    resultado["match_q2"] = resultado["fecha_q2"].notna().astype(int)
    return resultado


def imprimir_resumen_cruce(df1: pd.DataFrame, df2: pd.DataFrame, cruce_left: pd.DataFrame) -> None:
    print("============================================================")
    print(" RESUMEN CRUCE QUERY1 LEFT JOIN QUERY2")
    print("============================================================")
    print(f"Registros Query1:                             {len(df1):>12,}")
    print(f"Clientes unicos Query1:                       {df1['padded_codigo_cliente'].nunique():>12,}")
    print(f"Registros Query2:                             {len(df2):>12,}")
    print(f"Clientes unicos Query2:                       {df2['padded_codigo_cliente'].nunique():>12,}")
    print("-" * 60)
    total_left = len(cruce_left)
    total_match = int((cruce_left["match_q2"] == 1).sum())
    total_sin_match = total_left - total_match
    print(f"Registros resultado LEFT JOIN:                {total_left:>12,}")
    print(f"Registros con match q2 (cliente+fecha):       {total_match:>12,}")
    print(f"Registros sin match q2:                       {total_sin_match:>12,}")
    print(f"Clientes unicos en resultado final:           {cruce_left['padded_codigo_cliente'].nunique():>12,}")
    print("============================================================\n")

    if cruce_left.empty:
        print("[INFO] El LEFT JOIN final no devolvio filas.")
        return

    resumen_modulo_cruce = (
        cruce_left["modulo_regla"]
        .value_counts(dropna=False)
        .rename_axis("modulo_regla")
        .reset_index(name="registros")
    )
    print("Distribucion de modulo_regla en resultado LEFT JOIN:")
    print(resumen_modulo_cruce.to_string(index=False))

    cruce_tmp = cruce_left.copy()
    cruce_tmp["periodo_mes"] = cruce_tmp["fecha_q1"].dt.to_period("M").astype(str)
    resumen_mensual = (
        cruce_tmp.groupby("periodo_mes", as_index=False)
        .agg(
            registros=("padded_codigo_cliente", "size"),
            clientes_unicos=("padded_codigo_cliente", "nunique"),
            con_match_q2=("match_q2", "sum"),
        )
        .sort_values("periodo_mes")
        .reset_index(drop=True)
    )
    resumen_mensual["sin_match_q2"] = resumen_mensual["registros"] - resumen_mensual["con_match_q2"]
    resumen_mensual["pct_match_q2"] = (
        resumen_mensual["con_match_q2"] / resumen_mensual["registros"] * 100.0
    ).round(2)

    print("\nResumen por meses (segun fecha de Query1):")
    print(
        resumen_mensual[
            ["periodo_mes", "registros", "clientes_unicos", "con_match_q2", "sin_match_q2", "pct_match_q2"]
        ].to_string(
            index=False,
            formatters={
                "registros": "{:,.0f}".format,
                "clientes_unicos": "{:,.0f}".format,
                "con_match_q2": "{:,.0f}".format,
                "sin_match_q2": "{:,.0f}".format,
                "pct_match_q2": "{:,.2f}%".format,
            },
        )
    )


def main() -> None:
    print(f"Cargando Query1 desde: {QUERY1_PATH}")
    print(f"Cargando Query2 desde: {QUERY2_PATH}")

    try:
        t0 = time.perf_counter()
        raw_q1 = cargar_query(QUERY1_PATH)
        raw_q2 = cargar_query(QUERY2_PATH)
        t1 = time.perf_counter()

        df1 = preparar_query1(raw_q1)
        df2 = preparar_query2(raw_q2)

        imprimir_valores_modulo(df1)  # <- lo primero solicitado en consola
        cruce_left = left_join_tableau(df1, df2)
        imprimir_resumen_cruce(df1, df2, cruce_left)

        print(f"\nTiempo total de ejecucion: {time.perf_counter() - t0:.2f} segundos")
        print(f"Tiempo de carga SQL:       {t1 - t0:.2f} segundos")
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar alguna query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo en el proceso: {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
