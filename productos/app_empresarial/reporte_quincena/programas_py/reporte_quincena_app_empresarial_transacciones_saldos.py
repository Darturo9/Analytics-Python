"""
Reporte quincenal configurable de saldos - App Empresarial (solo Transacción).

Flujo de datos:
1) Actividad desde query1_quincena.sql
2) Universo de clientes contactados desde clientes_rtm_quincena.sql
3) Regla de cruce tipo Tableau: cliente y fecha_q1 >= fecha_q2
4) Filtro exclusivo al modulo Transacción
5) Perfil financiero del universo Transacción: saldo de cierre, saldo promedio, clientes con saldo y top deptos

Ejecucion:
    python3 productos/app_empresarial/reporte_quincena/programas_py/reporte_quincena_app_empresarial_transacciones_saldos.py
"""

from __future__ import annotations

import re
import sys
import unicodedata
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import get_engine, run_query_file


BASE_DIR = Path(__file__).resolve().parents[1]
QUERY1_PATH = BASE_DIR / "queries" / "query1_quincena.sql"
QUERY2_PATH = BASE_DIR / "queries" / "clientes_rtm_quincena.sql"

# Configuracion editable desde codigo.
CONFIG_ANIO = 2026
CONFIG_MES = 5
CONFIG_DIA_INICIO = 1
CONFIG_DIA_FIN = 15
CONFIG_RTM_FECHA_INICIO = "2025-05-01"
CONFIG_CHUNK_SIZE_CLIENTES = 400
CONFIG_TOP_DEPTOS = 5

MODULO_OBJETIVO = "Transacción"

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


def construir_resumen_transaccion(df: pd.DataFrame) -> tuple[int, int]:
    sub = df[df["modulo_regla"] == MODULO_OBJETIVO]
    return int(len(sub)), int(sub["padded_codigo_cliente"].nunique())


def construir_query_perfil_clientes_tmp() -> str:
    return """
WITH clientes_empresariales AS (
    SELECT
        x.padded_codigo_cliente,
        x.cldoc,
        x.cltdoc
    FROM (
        SELECT
            RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) AS padded_codigo_cliente,
            LTRIM(RTRIM(c.CLDOC)) AS cldoc,
            LTRIM(RTRIM(c.CLTDOC)) AS cltdoc,
            c.CLTIPE AS tipo_cliente,
            c.dw_usuarios_bel_cnt AS banca_e,
            ROW_NUMBER() OVER (
                PARTITION BY RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8)
                ORDER BY c.dw_fecha_informacion DESC
            ) AS rn
        FROM DW_CIF_CLIENTES c
        INNER JOIN #tmp_clientes cli
            ON cli.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8)
    ) x
    WHERE x.rn = 1
      AND x.tipo_cliente = 'J'
      AND x.banca_e = 1
),
saldos_dia AS (
    SELECT
        ce.padded_codigo_cliente,
        SUM(CAST(COALESCE(h.ctt001, 0) AS DECIMAL(18, 2))) AS saldo_total_actual
    FROM DW_DEP_DEPOSITOS h
    INNER JOIN clientes_empresariales ce
        ON ce.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(h.CLDOC)), 8)
    GROUP BY ce.padded_codigo_cliente
),
saldo_cliente AS (
    SELECT
        ce.padded_codigo_cliente,
        AVG(COALESCE(sd.saldo_total_actual, 0)) AS saldo_promedio_periodo,
        SUM(COALESCE(sd.saldo_total_actual, 0)) AS saldo_corte_periodo,
        MAX(CASE WHEN COALESCE(sd.saldo_total_actual, 0) > 0 THEN 1 ELSE 0 END) AS con_saldo_positivo
    FROM clientes_empresariales ce
    LEFT JOIN saldos_dia sd
        ON sd.padded_codigo_cliente = ce.padded_codigo_cliente
    GROUP BY ce.padded_codigo_cliente
),
direccion_cliente AS (
    SELECT
        y.padded_codigo_cliente,
        COALESCE(NULLIF(LTRIM(RTRIM(y.depto)), ''), 'SIN DATO') AS depto
    FROM (
        SELECT
            ce.padded_codigo_cliente,
            d.dw_nivel_geo2 AS depto,
            ROW_NUMBER() OVER (
                PARTITION BY ce.padded_codigo_cliente
                ORDER BY d.dw_fecha DESC
            ) AS rn
        FROM clientes_empresariales ce
        LEFT JOIN dw_cif_direcciones_principal d
            ON LTRIM(RTRIM(ce.cltdoc)) = LTRIM(RTRIM(d.cldoc))
    ) y
    WHERE y.rn = 1
)
SELECT
    ce.padded_codigo_cliente,
    CAST(COALESCE(sc.saldo_promedio_periodo, 0) AS DECIMAL(18, 2)) AS saldo_promedio_periodo,
    CAST(COALESCE(sc.saldo_corte_periodo, 0) AS DECIMAL(18, 2)) AS saldo_corte_periodo,
    CAST(COALESCE(sc.con_saldo_positivo, 0) AS INT) AS con_saldo_positivo,
    COALESCE(dc.depto, 'SIN DATO') AS depto
FROM clientes_empresariales ce
LEFT JOIN saldo_cliente sc
    ON sc.padded_codigo_cliente = ce.padded_codigo_cliente
LEFT JOIN direccion_cliente dc
    ON dc.padded_codigo_cliente = ce.padded_codigo_cliente;
"""


def cargar_perfil_financiero_clientes(
    clientes_objetivo: list[str],
    fecha_inicio: date,
    fecha_fin_exclusiva: date,
) -> pd.DataFrame:
    if not clientes_objetivo:
        return pd.DataFrame(
            columns=[
                "padded_codigo_cliente",
                "saldo_promedio_periodo",
                "saldo_corte_periodo",
                "con_saldo_positivo",
                "depto",
            ]
        )

    sql = construir_query_perfil_clientes_tmp()
    engine = get_engine()

    with engine.connect() as conn:
        conn.execute(text("IF OBJECT_ID('tempdb..#tmp_clientes') IS NOT NULL DROP TABLE #tmp_clientes;"))
        conn.execute(text("CREATE TABLE #tmp_clientes (padded_codigo_cliente VARCHAR(8) NOT NULL PRIMARY KEY);"))

        registros = [{"codigo": c} for c in clientes_objetivo]
        for i in range(0, len(registros), CONFIG_CHUNK_SIZE_CLIENTES):
            bloque = registros[i : i + CONFIG_CHUNK_SIZE_CLIENTES]
            conn.execute(
                text("INSERT INTO #tmp_clientes (padded_codigo_cliente) VALUES (:codigo)"),
                bloque,
            )

        df = pd.read_sql(
            text(sql),
            conn,
            params={},
        )
        conn.execute(text("DROP TABLE #tmp_clientes;"))

    df.columns = [str(c).strip() for c in df.columns]
    df = df.drop_duplicates(subset=["padded_codigo_cliente"]).copy()
    df["padded_codigo_cliente"] = df["padded_codigo_cliente"].apply(normalizar_codigo_cliente)
    df["saldo_promedio_periodo"] = pd.to_numeric(df["saldo_promedio_periodo"], errors="coerce").fillna(0.0)
    df["saldo_corte_periodo"] = pd.to_numeric(df["saldo_corte_periodo"], errors="coerce").fillna(0.0)
    df["con_saldo_positivo"] = pd.to_numeric(df["con_saldo_positivo"], errors="coerce").fillna(0).astype(int)
    df["depto"] = df["depto"].fillna("SIN DATO").astype(str).str.strip().replace("", "SIN DATO")
    return df


def imprimir_resumen_financiero(
    df_fin: pd.DataFrame,
    fecha_inicio: date,
    fecha_fin_exclusiva: date,
) -> None:
    fecha_fin_inclusiva = fecha_fin_exclusiva - timedelta(days=1)
    total_clientes = int(len(df_fin))

    if total_clientes == 0:
        print("\nPerfil financiero de clientes matched (solo Transacción):")
        print("Sin clientes para evaluar.")
        return

    clientes_con_saldo = int((df_fin["con_saldo_positivo"] > 0).sum())
    pct_con_saldo = (clientes_con_saldo / total_clientes * 100.0) if total_clientes else 0.0
    saldo_total_corte = float(df_fin["saldo_corte_periodo"].sum())
    saldo_promedio_periodo_cliente = float(df_fin["saldo_promedio_periodo"].mean())
    saldo_promedio_corte_cliente = float(df_fin["saldo_corte_periodo"].mean())

    print("\n" + "=" * 92)
    print("PERFIL FINANCIERO - CLIENTES MATCHED (SOLO TRANSACCIÓN)")
    print("=" * 92)
    print("Fuente saldos:                              DW_DEP_DEPOSITOS (maestro)")
    print(f"Rango para universo de transacciones:       {fecha_inicio.isoformat()} a {fecha_fin_inclusiva.isoformat()}")
    print(f"Clientes empresariales evaluados:            {total_clientes:>12,}")
    print(f"Clientes con saldo positivo en periodo:      {clientes_con_saldo:>12,}")
    print(f"% clientes con saldo positivo:               {pct_con_saldo:>11.2f}%")
    print("-" * 92)
    print(f"Saldo total al cierre del rango:             L {saldo_total_corte:>12,.2f}")
    print(f"Saldo promedio del periodo por cliente:      L {saldo_promedio_periodo_cliente:>12,.2f}")
    print(f"Saldo promedio al cierre por cliente:        L {saldo_promedio_corte_cliente:>12,.2f}")
    print("=" * 92)

    top_deptos = (
        df_fin.groupby("depto", as_index=False)
        .agg(clientes=("padded_codigo_cliente", "nunique"))
        .sort_values(["clientes", "depto"], ascending=[False, True])
        .head(CONFIG_TOP_DEPTOS)
        .reset_index(drop=True)
    )
    top_deptos["porcentaje"] = (top_deptos["clientes"] / total_clientes * 100.0).round(2)

    print(f"\nTop {CONFIG_TOP_DEPTOS} deptos por clientes (matched solo Transacción):")
    print(
        top_deptos.to_string(
            index=False,
            formatters={
                "clientes": "{:,.0f}".format,
                "porcentaje": "{:,.2f}%".format,
            },
        )
    )


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
    trx_q1, clientes_trx_q1 = construir_resumen_transaccion(df_q1_rango)
    trx_match, clientes_trx_match = construir_resumen_transaccion(df_match)

    print("=" * 92)
    print("APP EMPRESARIAL - REPORTE QUINCENAL (SOLO TRANSACCIÓN)")
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
    print("\nResumen Transacción:")
    print(f"- TRX en Query1 (rango):                      {trx_q1:>12,}")
    print(f"- Clientes unicos Transacción en Query1:      {clientes_trx_q1:>12,}")
    print(f"- TRX Transacción con match q2:               {trx_match:>12,}")
    print(f"- Clientes unicos Transacción con match q2:   {clientes_trx_match:>12,}")


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
            f"rtm_inicio={CONFIG_RTM_FECHA_INICIO}, exportar=0"
        )

        raw_q1 = cargar_query(QUERY1_PATH)
        raw_q2 = cargar_query(QUERY2_PATH, params={"fecha_rtm_inicio": CONFIG_RTM_FECHA_INICIO})

        df1 = preparar_query1(raw_q1)
        df2 = preparar_query2(raw_q2)

        df_q1_rango = df1[(df1["fecha_q1"] >= pd.Timestamp(fecha_inicio)) & (df1["fecha_q1"] < pd.Timestamp(fecha_fin_exclusiva))].copy()

        cruce = df_q1_rango.merge(df2, on="padded_codigo_cliente", how="left")
        cruce["match_q2"] = cruce["fecha_q2"].notna() & (cruce["fecha_q1"] >= cruce["fecha_q2"])
        df_match = cruce[cruce["match_q2"]].copy()
        df_match_obj = df_match[df_match["modulo_regla"] == MODULO_OBJETIVO].copy()

        imprimir_resumen(df_q1_rango, df2, df_match, fecha_inicio, fecha_fin_exclusiva)

        clientes_objetivo = sorted(df_match_obj["padded_codigo_cliente"].dropna().astype(str).unique().tolist())
        df_financiero = cargar_perfil_financiero_clientes(clientes_objetivo, fecha_inicio, fecha_fin_exclusiva)
        imprimir_resumen_financiero(df_financiero, fecha_inicio, fecha_fin_exclusiva)

    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
