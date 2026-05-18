"""
Reporte quincenal configurable - App Empresarial.

Flujo de datos:
1) Actividad desde query1_quincena.sql
2) Universo de clientes contactados desde clientes_rtm_quincena.sql
3) Regla de cruce tipo Tableau: cliente y fecha_q1 >= fecha_q2
4) Clasificacion de modulo y resumen en consola

Ejecucion:
    python3 productos/app_empresarial/reporte_quincena/programas_py/reporte_quincena_app_empresarial.py
"""

from __future__ import annotations

import re
import sys
import unicodedata
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query_file


BASE_DIR = Path(__file__).resolve().parents[1]
QUERY1_PATH = BASE_DIR / "queries" / "query1_quincena.sql"
QUERY2_PATH = BASE_DIR / "queries" / "clientes_rtm_quincena.sql"
EXPORTS_DIR = BASE_DIR / "exports"

# Configuracion editable desde codigo.
CONFIG_ANIO = 2026
CONFIG_MES = 5
CONFIG_DIA_INICIO = 1
CONFIG_DIA_FIN = 15
CONFIG_RTM_FECHA_INICIO = "2025-05-01"
CONFIG_EXPORTAR_CLIENTES = True

MODULOS_OBJETIVO = ["Consulta", "Gestiones CRM", "Login", "Transacción"]

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


def validar_rango(anio: int, mes: int, dia_inicio: int, dia_fin: int) -> tuple[date, date]:
    if mes < 1 or mes > 12:
        raise ValueError("CONFIG_MES debe estar entre 1 y 12.")
    if dia_inicio < 1:
        raise ValueError("CONFIG_DIA_INICIO debe ser mayor o igual a 1.")
    if dia_fin < dia_inicio:
        raise ValueError("CONFIG_DIA_FIN debe ser mayor o igual a CONFIG_DIA_INICIO.")

    fecha_inicio = date(anio, mes, dia_inicio)
    if mes == 12:
        primer_dia_mes_siguiente = date(anio + 1, 1, 1)
    else:
        primer_dia_mes_siguiente = date(anio, mes + 1, 1)
    ultimo_dia_mes = primer_dia_mes_siguiente - timedelta(days=1)

    if dia_fin > ultimo_dia_mes.day:
        raise ValueError(
            f"CONFIG_DIA_FIN ({dia_fin}) excede el ultimo dia del mes ({ultimo_dia_mes.day}) para {anio}-{mes:02d}."
        )

    fecha_fin_exclusiva = date(anio, mes, dia_fin) + timedelta(days=1)
    return fecha_inicio, fecha_fin_exclusiva


def normalizar_codigo_cliente(valor: object) -> str:
    if valor is None or pd.isna(valor):
        return ""
    texto = str(valor).strip()
    if texto.lower() == "nan" or texto == "":
        return ""
    solo_digitos = re.sub(r"\D", "", texto)
    if solo_digitos == "":
        return ""
    return solo_digitos[-8:].zfill(8)


def normalizar_texto(valor: object) -> str:
    if valor is None or pd.isna(valor):
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


def cargar_query(path_sql: Path, params: dict | None = None) -> pd.DataFrame:
    df = run_query_file(str(path_sql), params=params)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def preparar_query1(df1: pd.DataFrame) -> pd.DataFrame:
    col_cliente = buscar_columna(df1, "padded_codigo_cliente", "codigo_cliente", "clccli")
    col_fecha = buscar_columna(df1, "fecha")
    col_modulo = buscar_columna(df1, "modulo", "modulo (grupo) (grupo)", "modulo (grupo)")
    col_operacion = buscar_columna(df1, "operación", "operacion", "operación (consulta sql personalizada)")

    out = pd.DataFrame(
        {
            "padded_codigo_cliente": df1[col_cliente].apply(normalizar_codigo_cliente),
            "fecha_q1": pd.to_datetime(df1[col_fecha], errors="coerce"),
            "modulo_original": df1[col_modulo],
            "operacion_original": df1[col_operacion],
        }
    )
    out = out[out["padded_codigo_cliente"] != ""].copy()
    out = out.dropna(subset=["fecha_q1"]).copy()
    out["modulo_regla"] = out.apply(
        lambda r: clasificar_modulo(r["modulo_original"], r["operacion_original"]),
        axis=1,
    )
    return out


def preparar_query2(df2: pd.DataFrame) -> pd.DataFrame:
    col_cliente = buscar_columna(df2, "padded_codigo_cliente")
    col_fecha = buscar_columna(df2, "fecha_q2", "fecha")

    out = pd.DataFrame(
        {
            "padded_codigo_cliente": df2[col_cliente].apply(normalizar_codigo_cliente),
            "fecha_q2": pd.to_datetime(df2[col_fecha], errors="coerce"),
        }
    )
    out = out[(out["padded_codigo_cliente"] != "") & (out["fecha_q2"].notna())].copy()
    out = out.drop_duplicates(subset=["padded_codigo_cliente"], keep="first")
    return out


def construir_resumen_modulos(df: pd.DataFrame) -> pd.DataFrame:
    resumen = (
        df[df["modulo_regla"].isin(MODULOS_OBJETIVO)]
        .groupby("modulo_regla", as_index=False)
        .agg(
            cantidad=("padded_codigo_cliente", "size"),
            clientes_unicos=("padded_codigo_cliente", "nunique"),
        )
    )

    base = pd.DataFrame({"modulo_regla": MODULOS_OBJETIVO})
    resumen = base.merge(resumen, on="modulo_regla", how="left").fillna(0)
    resumen["cantidad"] = resumen["cantidad"].astype(int)
    resumen["clientes_unicos"] = resumen["clientes_unicos"].astype(int)
    return resumen


def imprimir_resumen(
    df_q1_rango: pd.DataFrame,
    df_q2: pd.DataFrame,
    df_match: pd.DataFrame,
    fecha_inicio: date,
    fecha_fin_exclusiva: date,
) -> None:
    fecha_fin_inclusiva = fecha_fin_exclusiva - timedelta(days=1)

    total_q1 = len(df_q1_rango)
    clientes_q1 = df_q1_rango["padded_codigo_cliente"].nunique()
    clientes_q2 = df_q2["padded_codigo_cliente"].nunique()

    total_match = len(df_match)
    clientes_match = df_match["padded_codigo_cliente"].nunique()

    resumen_rango = construir_resumen_modulos(df_q1_rango)
    resumen_match = construir_resumen_modulos(df_match)

    print("=" * 92)
    print("APP EMPRESARIAL - REPORTE QUINCENAL CONFIGURABLE")
    print("=" * 92)
    print(f"Rango actividad Query1:      {fecha_inicio.isoformat()} a {fecha_fin_inclusiva.isoformat()}")
    print(f"Inicio universo RTM Query2: {CONFIG_RTM_FECHA_INICIO}")
    print("-" * 92)
    print(f"Registros Query1 en rango:                    {total_q1:>12,}")
    print(f"Clientes unicos Query1 en rango:              {clientes_q1:>12,}")
    print(f"Clientes unicos en Query2 (universo RTM):     {clientes_q2:>12,}")
    print(f"Registros con match (cliente y fecha_q1>=q2): {total_match:>12,}")
    print(f"Clientes unicos con match:                    {clientes_match:>12,}")
    print("=" * 92)

    print("\nResumen modulos (solo rango Query1, antes de match q2):")
    print(resumen_rango.to_string(index=False))

    print("\nResumen modulos (despues de match q2 por cliente+fecha):")
    print(resumen_match.to_string(index=False))


def exportar_clientes(df_match: pd.DataFrame, fecha_inicio: date, fecha_fin_exclusiva: date) -> None:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    fecha_fin_inclusiva = fecha_fin_exclusiva - timedelta(days=1)
    sufijo = f"{fecha_inicio.strftime('%Y%m%d')}_{fecha_fin_inclusiva.strftime('%Y%m%d')}"

    clientes = (
        df_match[df_match["modulo_regla"].isin(MODULOS_OBJETIVO)][["padded_codigo_cliente"]]
        .drop_duplicates()
        .sort_values("padded_codigo_cliente")
        .rename(columns={"padded_codigo_cliente": "codigo_cliente"})
    )

    txt_path = EXPORTS_DIR / f"Clientes_Quincena_{sufijo}_ConModulos.txt"
    xlsx_path = EXPORTS_DIR / f"Clientes_Quincena_{sufijo}_ConModulos.xlsx"

    clientes["codigo_cliente"].to_csv(txt_path, index=False, header=False, encoding="utf-8")
    try:
        clientes.to_excel(xlsx_path, index=False)
        excel_msg = str(xlsx_path)
    except Exception:
        excel_msg = "No se pudo generar (motor Excel no disponible)."

    print("\nExportables:")
    print(f"- Clientes unicos exportados: {len(clientes):,}")
    print(f"- TXT:   {txt_path}")
    print(f"- Excel: {excel_msg}")


def main() -> None:
    try:
        fecha_inicio, fecha_fin_exclusiva = validar_rango(
            CONFIG_ANIO, CONFIG_MES, CONFIG_DIA_INICIO, CONFIG_DIA_FIN
        )

        print(f"Cargando Query1: {QUERY1_PATH}")
        print(f"Cargando Query2: {QUERY2_PATH}")
        print(
            "Configuracion -> "
            f"anio={CONFIG_ANIO}, mes={CONFIG_MES}, dia_inicio={CONFIG_DIA_INICIO}, dia_fin={CONFIG_DIA_FIN}, "
            f"rtm_inicio={CONFIG_RTM_FECHA_INICIO}, exportar={int(CONFIG_EXPORTAR_CLIENTES)}"
        )

        raw_q1 = cargar_query(QUERY1_PATH)
        raw_q2 = cargar_query(QUERY2_PATH, params={"fecha_rtm_inicio": CONFIG_RTM_FECHA_INICIO})

        df1 = preparar_query1(raw_q1)
        df2 = preparar_query2(raw_q2)

        df_q1_rango = df1[(df1["fecha_q1"] >= pd.Timestamp(fecha_inicio)) & (df1["fecha_q1"] < pd.Timestamp(fecha_fin_exclusiva))].copy()

        cruce = df_q1_rango.merge(df2, on="padded_codigo_cliente", how="left")
        cruce["match_q2"] = cruce["fecha_q2"].notna() & (cruce["fecha_q1"] >= cruce["fecha_q2"])
        df_match = cruce[cruce["match_q2"]].copy()

        imprimir_resumen(df_q1_rango, df2, df_match, fecha_inicio, fecha_fin_exclusiva)

        if CONFIG_EXPORTAR_CLIENTES:
            exportar_clientes(df_match, fecha_inicio, fecha_fin_exclusiva)

    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
