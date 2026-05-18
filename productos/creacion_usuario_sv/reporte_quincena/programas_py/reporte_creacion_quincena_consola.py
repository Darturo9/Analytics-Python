"""
Reporte en consola: Creacion Usuario SV - Quincena configurable.

Uso:
    python3 productos/creacion_usuario_sv/reporte_quincena/programas_py/reporte_creacion_quincena_consola.py
"""

from __future__ import annotations

import sys
import urllib
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.config import DB_DRIVER, DB_PASS, DB_SERVER, DB_USER


BASE_DIR = Path(__file__).resolve().parents[1]

# Configuracion editable.
CONFIG_ANIO = 2026
CONFIG_MES = 5
CONFIG_DIA_INICIO = 1
CONFIG_DIA_FIN = 15
CONFIG_RTM_FECHA_INICIO = "2024-09-01"
CONFIG_DB_NAME = "DWHSV"
CONFIG_VENTANA_CAMPANIA_MESES = 3


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


def run_query_hsv(sql: str, params: dict | None = None) -> pd.DataFrame:
    conn_params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={CONFIG_DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_params}", fast_executemany=True)
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def run_query_file_hsv(path: Path, params: dict | None = None) -> pd.DataFrame:
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    return run_query_hsv(sql, params=params)


def normalizar_codigo_cliente(valor) -> str:
    if pd.isna(valor):
        return ""
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    return solo_digitos[-8:].zfill(8) if solo_digitos else ""


def clasificar_generacion(fecha_nac) -> str:
    if pd.isna(fecha_nac):
        return "OTRA GENERACION"
    anio = int(fecha_nac.year)
    if 1965 <= anio <= 1980:
        return "Generation X (1965-1980)"
    if 1981 <= anio <= 1996:
        return "Gen Y - Millennials (1981-1996)"
    if 1997 <= anio <= 2012:
        return "Generacion Z (1997-2012)"
    return "OTRA GENERACION"


def normalizar_genero(valor) -> str:
    if pd.isna(valor):
        return "Sin dato"
    texto = str(valor).strip().upper()
    if texto in {"F", "FEMENINO", "MUJER"}:
        return "Mujer"
    if texto in {"M", "MASCULINO", "HOMBRE"}:
        return "Hombre"
    return "Sin dato"


def es_usuario_activo(valor) -> bool:
    if pd.isna(valor):
        return False

    estado = str(valor).strip().upper()
    if estado.endswith(".0") and estado[:-2].isdigit():
        estado = estado[:-2]

    if "INACT" in estado:
        return False

    activos = {"A", "ACTIVO", "ACTIVE", "1", "TRUE", "T", "HABILITADO", "VIGENTE"}
    return estado in activos or ("ACT" in estado and "INACT" not in estado)


def cargar_bases(
    fecha_inicio: date, fecha_fin_exclusiva: date
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fecha_rtm_inicio = pd.to_datetime(CONFIG_RTM_FECHA_INICIO)
    fecha_rtm_fin_exclusiva = pd.to_datetime(fecha_fin_exclusiva)

    print(f"Cargando bases en {CONFIG_DB_NAME}...")
    conversion = run_query_file_hsv(
        BASE_DIR / "queries" / "conversion_quincena.sql",
        params={
            "fecha_inicio": fecha_inicio.isoformat(),
            "fecha_fin_exclusiva": fecha_fin_exclusiva.isoformat(),
        },
    )
    rtm = run_query_file_hsv(
        BASE_DIR / "queries" / "comunicacionesRTM_quincena.sql",
        params={
            "fecha_rtm_inicio": fecha_rtm_inicio.isoformat(),
            "fecha_rtm_fin_exclusiva": fecha_rtm_fin_exclusiva.isoformat(),
        },
    )
    logins = run_query_file_hsv(
        BASE_DIR / "queries" / "logins_quincena.sql",
        params={
            "fecha_inicio": fecha_inicio.isoformat(),
            "fecha_fin_exclusiva": fecha_fin_exclusiva.isoformat(),
        },
    )
    trx = run_query_file_hsv(
        BASE_DIR / "queries" / "trx_quincena.sql",
        params={
            "fecha_inicio": fecha_inicio.isoformat(),
            "fecha_fin_exclusiva": fecha_fin_exclusiva.isoformat(),
        },
    )

    print(f"  {len(conversion):,} registros conversion")
    print(f"  {len(rtm):,} registros RTM")
    print(f"  {len(logins):,} registros logins")
    print(f"  {len(trx):,} registros trx")
    return conversion, rtm, logins, trx


def preparar_cohorte(conversion: pd.DataFrame, rtm: pd.DataFrame) -> pd.DataFrame:
    conv = conversion.copy()
    camp = rtm.copy()

    conv["fecha_creacion_usuario"] = pd.to_datetime(conv["fecha_creacion_usuario"], errors="coerce")
    conv["fecha_nacimiento_usuario"] = pd.to_datetime(conv["fecha_nacimiento_usuario"], errors="coerce")
    conv = conv[conv["fecha_creacion_usuario"].notna()].copy()

    conv["generacion"] = conv["fecha_nacimiento_usuario"].apply(clasificar_generacion)
    conv["genero"] = conv["genero_cliente"].apply(normalizar_genero)
    conv["es_usuario_activo"] = conv["estado_usuario"].apply(es_usuario_activo)

    conv["id_usuario"] = conv["nombre_usuario"].astype("string").str.strip()
    conv.loc[conv["nombre_usuario"].isna(), "id_usuario"] = pd.NA
    conv.loc[conv["id_usuario"] == "", "id_usuario"] = pd.NA

    conv["anio"] = conv["fecha_creacion_usuario"].dt.year
    conv["mes"] = conv["fecha_creacion_usuario"].dt.month
    conv["dia"] = conv["fecha_creacion_usuario"].dt.day

    conv["codigo_cliente_usuario_creado"] = conv["codigo_cliente_usuario_creado"].apply(normalizar_codigo_cliente)
    camp["codigo_cliente_usuario_campania"] = camp["codigo_cliente_usuario_campania"].apply(normalizar_codigo_cliente)
    camp["fecha_campania"] = pd.to_datetime(camp["fecha_campania"], errors="coerce")

    camp_match = (
        camp[["codigo_cliente_usuario_campania", "fecha_campania"]]
        .dropna(subset=["codigo_cliente_usuario_campania", "fecha_campania"])
        .drop_duplicates(subset=["codigo_cliente_usuario_campania", "fecha_campania"])
        .copy()
    )

    df_merge = conv.merge(
        camp_match,
        how="left",
        left_on="codigo_cliente_usuario_creado",
        right_on="codigo_cliente_usuario_campania",
    )

    fecha_creacion = df_merge["fecha_creacion_usuario"].dt.normalize()
    fecha_campania = df_merge["fecha_campania"].dt.normalize()
    fecha_campania_mas_ventana = fecha_campania + pd.DateOffset(months=CONFIG_VENTANA_CAMPANIA_MESES)

    df_merge["match_campania"] = (
        fecha_campania.notna()
        & (fecha_creacion >= fecha_campania)
        & (fecha_creacion <= fecha_campania_mas_ventana)
    )

    df = (
        df_merge.groupby(["anio", "mes", "id_usuario"], as_index=False)
        .agg(
            dia=("dia", "min"),
            fecha_creacion_usuario=("fecha_creacion_usuario", "min"),
            codigo_cliente_usuario_creado=("codigo_cliente_usuario_creado", "first"),
            generacion=("generacion", "first"),
            genero=("genero", "first"),
            es_usuario_activo=("es_usuario_activo", lambda s: bool(s.fillna(False).any())),
            direccion_lvl_1=("direccion_lvl_1", "first"),
            direccion_lvl_2=("direccion_lvl_2", "first"),
            estado_usuario=("estado_usuario", "first"),
            estado_cliente=("estado_cliente", "first"),
            medio=("match_campania", lambda s: "Medios propios" if s.fillna(False).any() else "Producto"),
        )
        .copy()
    )

    df = df[df["id_usuario"].notna()].copy()
    df["fecha"] = df["fecha_creacion_usuario"].dt.date
    return df


def preparar_logins(logins_df: pd.DataFrame) -> pd.DataFrame:
    logins = logins_df.copy()
    if logins.empty:
        return logins

    logins["codigo_cliente_login"] = logins["codigo_cliente_login"].apply(normalizar_codigo_cliente)
    logins = logins[logins["codigo_cliente_login"] != ""].copy()
    logins["fecha_login"] = pd.to_datetime(logins["fecha_login"], errors="coerce")
    logins = logins[logins["fecha_login"].notna()].copy()
    return logins


def preparar_trx(trx_df: pd.DataFrame) -> pd.DataFrame:
    trx = trx_df.copy()
    if trx.empty:
        return trx

    trx["codigo_cliente_transaccion"] = trx["codigo_cliente_transaccion"].apply(normalizar_codigo_cliente)
    trx = trx[trx["codigo_cliente_transaccion"] != ""].copy()
    trx["fecha_transaccion"] = pd.to_datetime(trx["fecha_transaccion"], errors="coerce")
    trx = trx[trx["fecha_transaccion"].notna()].copy()
    trx["monto_transaccion"] = pd.to_numeric(trx["monto_transaccion"], errors="coerce").fillna(0.0)
    trx["transaccion"] = trx["transaccion"].fillna("SIN DATO").astype(str).str.strip().replace("", "SIN DATO")
    return trx


def calcular_metricas_eventos(
    cohorte: pd.DataFrame,
    logins: pd.DataFrame,
    trx: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    mapa_cod_usuario = (
        cohorte[["codigo_cliente_usuario_creado", "id_usuario"]]
        .dropna(subset=["codigo_cliente_usuario_creado", "id_usuario"])
        .drop_duplicates()
    )

    logins_match = logins.merge(
        mapa_cod_usuario,
        how="inner",
        left_on="codigo_cliente_login",
        right_on="codigo_cliente_usuario_creado",
    )

    trx_match = trx.merge(
        mapa_cod_usuario,
        how="inner",
        left_on="codigo_cliente_transaccion",
        right_on="codigo_cliente_usuario_creado",
    )

    return logins_match, trx_match


def imprimir_resumen(
    cohorte: pd.DataFrame,
    logins_match: pd.DataFrame,
    trx_match: pd.DataFrame,
    fecha_inicio: date,
    fecha_fin_exclusiva: date,
) -> None:
    total_usuarios = int(cohorte["id_usuario"].nunique()) if not cohorte.empty else 0
    fecha_fin_inclusiva = fecha_fin_exclusiva - timedelta(days=1)

    usuarios_activos = int(cohorte.loc[cohorte["es_usuario_activo"] == True, "id_usuario"].nunique())
    usuarios_con_login = int(logins_match["id_usuario"].nunique()) if not logins_match.empty else 0
    total_logins = int(len(logins_match))
    usuarios_con_trx = int(trx_match["id_usuario"].nunique()) if not trx_match.empty else 0
    total_trx = int(len(trx_match))

    print("=" * 88)
    print("CREACION DE USUARIO SV - REPORTE QUINCENAL (CONSOLA)")
    print("=" * 88)
    print(f"Rango: {fecha_inicio.isoformat()} a {fecha_fin_inclusiva.isoformat()}")
    print(f"Total usuarios unicos creados (RECDIST nombre_usuario): {total_usuarios:,}")
    print(f"Usuarios activos:                                      {usuarios_activos:,}")
    print(f"Usuarios con login:                                   {usuarios_con_login:,}")
    print(f"Total logins:                                         {total_logins:,}")
    print(f"Usuarios con trx:                                     {usuarios_con_trx:,}")
    print(f"Total trx:                                            {total_trx:,}")

    print("\n--- Distribucion por medio ---")
    if cohorte.empty:
        print("Sin datos.")
    else:
        medio_counts = cohorte.groupby("medio")["id_usuario"].nunique().sort_values(ascending=False)
        for medio, val in medio_counts.items():
            pct = (val / total_usuarios * 100) if total_usuarios else 0
            print(f"{medio:15} {val:>8,}  ({pct:5.2f}%)")

    print("\n--- Distribucion por genero ---")
    if cohorte.empty:
        print("Sin datos.")
    else:
        genero_counts = (
            cohorte["genero"]
            .fillna("Sin dato")
            .astype(str)
            .str.strip()
            .replace("", "Sin dato")
            .value_counts()
            .reindex(["Mujer", "Hombre", "Sin dato"], fill_value=0)
        )
        for genero, val in genero_counts.items():
            pct = (val / total_usuarios * 100) if total_usuarios else 0
            print(f"{genero:10} {val:>8,}  ({pct:5.2f}%)")

    print("\n--- Generaciones ---")
    if cohorte.empty:
        print("Sin datos.")
    else:
        orden_generaciones = [
            "Generation X (1965-1980)",
            "Gen Y - Millennials (1981-1996)",
            "Generacion Z (1997-2012)",
            "OTRA GENERACION",
        ]
        gen_counts = (
            cohorte["generacion"]
            .fillna("OTRA GENERACION")
            .astype(str)
            .str.strip()
            .replace("", "OTRA GENERACION")
            .value_counts()
            .reindex(orden_generaciones, fill_value=0)
        )
        for generacion, val in gen_counts.items():
            pct = (val / total_usuarios * 100) if total_usuarios else 0
            print(f"{generacion:32} {val:>8,}  ({pct:5.2f}%)")

    print("\n--- Creaciones por dia ---")
    if cohorte.empty:
        print("Sin datos.")
    else:
        diario = cohorte.groupby("fecha")["id_usuario"].nunique().sort_index()
        for fecha, val in diario.items():
            print(f"{fecha}  {val:>8,}")

    print("\n--- Top 10 direccion_lvl_1 ---")
    if cohorte.empty:
        print("Sin datos.")
    else:
        top_geo1 = (
            cohorte["direccion_lvl_1"].fillna("SIN_DATO").astype(str).str.strip().replace("", "SIN_DATO")
        ).value_counts().head(10)
        for geo, val in top_geo1.items():
            print(f"{geo[:30]:30} {val:>8,}")


def main() -> None:
    try:
        fecha_inicio, fecha_fin_exclusiva = validar_rango(
            CONFIG_ANIO, CONFIG_MES, CONFIG_DIA_INICIO, CONFIG_DIA_FIN
        )
        print(
            "Configuracion -> "
            f"anio={CONFIG_ANIO}, mes={CONFIG_MES}, dia_inicio={CONFIG_DIA_INICIO}, dia_fin={CONFIG_DIA_FIN}, "
            f"rtm_inicio={CONFIG_RTM_FECHA_INICIO}, db={CONFIG_DB_NAME}, ventana_meses={CONFIG_VENTANA_CAMPANIA_MESES}"
        )

        conversion_df, rtm_df, logins_df, trx_df = cargar_bases(fecha_inicio, fecha_fin_exclusiva)
        cohorte = preparar_cohorte(conversion_df, rtm_df)
        logins = preparar_logins(logins_df)
        trx = preparar_trx(trx_df)
        logins_match, trx_match = calcular_metricas_eventos(cohorte, logins, trx)

        imprimir_resumen(cohorte, logins_match, trx_match, fecha_inicio, fecha_fin_exclusiva)

    except SQLAlchemyError as exc:
        msg = " ".join(str(exc).split())
        print(f"[ERROR] No se pudo conectar/consultar en {CONFIG_DB_NAME}: {msg}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
