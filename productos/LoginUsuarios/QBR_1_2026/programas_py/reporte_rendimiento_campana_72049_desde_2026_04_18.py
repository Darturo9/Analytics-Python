"""
reporte_rendimiento_campana_72049_desde_2026_04_18.py
-----------------------------------------------------
Reporte en consola para medir rendimiento de clientes contactados en campaña
72049 (login) desde 2026-04-18.

Muestra:
- total de clientes contactados
- total de eventos login
- clientes unicos con login y sin login
- total de eventos de cambio de password
- clientes unicos con cambio de password
- desglose de logins por canal
- resumen diario (logins / cambios)
- top clientes por logins y por cambios de password

Ejecucion:
    python3 productos/LoginUsuarios/QBR_1_2026/programas_py/reporte_rendimiento_campana_72049_desde_2026_04_18.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query, run_query_file


RUTA_QUERY_CLIENTES = (
    PROJECT_ROOT
    / "productos"
    / "LoginUsuarios"
    / "QBR_1_2026"
    / "queries"
    / "sin_login"
    / "clientes_contactados_campana_72049_login_desde_2026_04_18.sql"
)

SQL_LOGINS = """
SELECT
    RTRIM(LTRIM(clccli)) AS codigo_usuario,
    RIGHT('00000000' + RTRIM(LTRIM(clccli)), 8) AS padded_codigo_usuario,
    uscode AS nombre_usuario,
    secode AS canal_login,
    dw_fecha_trx AS fecha_inicio
FROM dw_bel_IBSTTRA_VIEW
WHERE dw_fecha_trx >= :fecha_inicio
  AND dw_fecha_trx < :fecha_fin_exclusiva
  AND secode IN ('app-login', 'web-login', 'login')
  AND clccli IS NOT NULL;
"""

SQL_CAMBIOS_PASSWORD = """
SELECT
    DW_BEL_IBUSER.CLCCLI AS codigo_cliente,
    DW_BEL_IBUSER.dw_fecha_cambio_pass AS fecha_cambio_pass
FROM DW_BEL_IBUSER
WHERE DW_BEL_IBUSER.dw_fecha_cambio_pass IS NOT NULL
  AND DW_BEL_IBUSER.dw_fecha_cambio_pass >= :fecha_inicio
  AND DW_BEL_IBUSER.dw_fecha_cambio_pass < :fecha_fin_exclusiva;
"""


def normalizar_codigo(valor) -> str | None:
    if pd.isna(valor):
        return None
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    if not solo_digitos:
        return None
    return solo_digitos[-8:].zfill(8)


def fecha_fin_default() -> str:
    return (pd.Timestamp.today().normalize() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")


def validar_fechas(fecha_inicio: str, fecha_fin_exclusiva: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    inicio = pd.to_datetime(fecha_inicio, errors="coerce")
    fin = pd.to_datetime(fecha_fin_exclusiva, errors="coerce")
    if pd.isna(inicio) or pd.isna(fin):
        raise ValueError("Fechas invalidas. Usa formato YYYY-MM-DD.")
    if fin <= inicio:
        raise ValueError("fecha_fin_exclusiva debe ser mayor a fecha_inicio.")
    return inicio, fin


def cargar_clientes_contactados() -> pd.DataFrame:
    if not RUTA_QUERY_CLIENTES.exists():
        raise FileNotFoundError(f"No existe query de clientes contactados: {RUTA_QUERY_CLIENTES}")

    df = run_query_file(str(RUTA_QUERY_CLIENTES))
    if df.empty:
        return pd.DataFrame(columns=["padded_codigo_cliente"])

    columnas = {str(c).strip().lower(): c for c in df.columns}
    if "padded_codigo_cliente" not in columnas:
        raise ValueError(
            "La query de clientes contactados debe devolver la columna padded_codigo_cliente. "
            f"Columnas encontradas: {list(df.columns)}"
        )

    col = columnas["padded_codigo_cliente"]
    out = df.copy()
    out["padded_codigo_cliente"] = out[col].apply(normalizar_codigo)
    out = out[out["padded_codigo_cliente"].notna()].drop_duplicates(subset=["padded_codigo_cliente"])
    return out[["padded_codigo_cliente"]].copy()


def cargar_logins(fecha_inicio: str, fecha_fin_exclusiva: str) -> pd.DataFrame:
    df = run_query(
        SQL_LOGINS,
        params={"fecha_inicio": fecha_inicio, "fecha_fin_exclusiva": fecha_fin_exclusiva},
    )
    if df.empty:
        return df

    df = df.copy()
    df["padded_codigo_usuario"] = df["padded_codigo_usuario"].apply(normalizar_codigo)
    df["fecha_inicio"] = pd.to_datetime(df["fecha_inicio"], errors="coerce")
    df["canal_login"] = df["canal_login"].fillna("SIN DATO").astype(str).str.strip()
    df = df[df["padded_codigo_usuario"].notna() & df["fecha_inicio"].notna()].copy()
    return df


def cargar_cambios_password(fecha_inicio: str, fecha_fin_exclusiva: str) -> pd.DataFrame:
    df = run_query(
        SQL_CAMBIOS_PASSWORD,
        params={"fecha_inicio": fecha_inicio, "fecha_fin_exclusiva": fecha_fin_exclusiva},
    )
    if df.empty:
        return df

    df = df.copy()
    df["padded_codigo_cliente"] = df["codigo_cliente"].apply(normalizar_codigo)
    df["fecha_cambio_pass"] = pd.to_datetime(df["fecha_cambio_pass"], errors="coerce")
    df = df[df["padded_codigo_cliente"].notna() & df["fecha_cambio_pass"].notna()].copy()
    return df


def construir_resumen_por_cliente(
    df_clientes: pd.DataFrame,
    df_logins_filtrado: pd.DataFrame,
    df_cambios_filtrado: pd.DataFrame,
) -> pd.DataFrame:
    logins_por_cliente = (
        df_logins_filtrado.groupby("padded_codigo_usuario")
        .size()
        .rename("total_logins")
    )
    cambios_por_cliente = (
        df_cambios_filtrado.groupby("padded_codigo_cliente")
        .size()
        .rename("total_cambios_password")
    )

    df_resumen = (
        df_clientes.merge(
            logins_por_cliente,
            how="left",
            left_on="padded_codigo_cliente",
            right_index=True,
        )
        .merge(
            cambios_por_cliente,
            how="left",
            left_on="padded_codigo_cliente",
            right_index=True,
        )
        .fillna(0)
    )

    df_resumen["total_logins"] = df_resumen["total_logins"].astype(int)
    df_resumen["total_cambios_password"] = df_resumen["total_cambios_password"].astype(int)
    return df_resumen


def construir_resumen_diario(
    df_logins_filtrado: pd.DataFrame,
    df_cambios_filtrado: pd.DataFrame,
    fecha_inicio: pd.Timestamp,
    fecha_fin_exclusiva: pd.Timestamp,
) -> pd.DataFrame:
    fechas = pd.date_range(fecha_inicio, fecha_fin_exclusiva - pd.Timedelta(days=1), freq="D")

    if df_logins_filtrado.empty:
        logins_eventos = pd.Series(0, index=fechas)
        logins_clientes = pd.Series(0, index=fechas)
    else:
        logins_eventos = (
            df_logins_filtrado.assign(dia=df_logins_filtrado["fecha_inicio"].dt.normalize())
            .groupby("dia")
            .size()
            .reindex(fechas, fill_value=0)
        )
        logins_clientes = (
            df_logins_filtrado.assign(dia=df_logins_filtrado["fecha_inicio"].dt.normalize())
            .groupby("dia")["padded_codigo_usuario"]
            .nunique()
            .reindex(fechas, fill_value=0)
        )

    if df_cambios_filtrado.empty:
        cambios_eventos = pd.Series(0, index=fechas)
        cambios_clientes = pd.Series(0, index=fechas)
    else:
        cambios_eventos = (
            df_cambios_filtrado.assign(dia=df_cambios_filtrado["fecha_cambio_pass"].dt.normalize())
            .groupby("dia")
            .size()
            .reindex(fechas, fill_value=0)
        )
        cambios_clientes = (
            df_cambios_filtrado.assign(dia=df_cambios_filtrado["fecha_cambio_pass"].dt.normalize())
            .groupby("dia")["padded_codigo_cliente"]
            .nunique()
            .reindex(fechas, fill_value=0)
        )

    detalle = pd.DataFrame(
        {
            "dia": fechas,
            "eventos_logins": logins_eventos.values,
            "clientes_unicos_login": logins_clientes.values,
            "eventos_cambios_password": cambios_eventos.values,
            "clientes_unicos_cambio": cambios_clientes.values,
        }
    )
    return detalle


def imprimir_reporte(
    df_resumen: pd.DataFrame,
    df_logins_filtrado: pd.DataFrame,
    df_cambios_filtrado: pd.DataFrame,
    resumen_diario: pd.DataFrame,
    fecha_inicio: str,
    fecha_fin_exclusiva: str,
    top_n: int,
) -> None:
    total_clientes_contactados = int(df_resumen["padded_codigo_cliente"].nunique())

    total_eventos_login = int(df_logins_filtrado.shape[0])
    clientes_con_login = int((df_resumen["total_logins"] > 0).sum())
    clientes_sin_login = max(total_clientes_contactados - clientes_con_login, 0)

    total_eventos_cambios = int(df_cambios_filtrado.shape[0])
    clientes_con_cambio = int((df_resumen["total_cambios_password"] > 0).sum())

    clientes_con_ambos = int(
        ((df_resumen["total_logins"] > 0) & (df_resumen["total_cambios_password"] > 0)).sum()
    )

    print("=" * 108)
    print("RENDIMIENTO CLIENTES CONTACTADOS CAMPANA 72049 LOGIN")
    print("=" * 108)
    print(f"Periodo evaluado: {fecha_inicio} a {fecha_fin_exclusiva} (fin exclusiva)")
    print(f"Clientes contactados unicos: {total_clientes_contactados:,}")
    print("-" * 108)
    print(f"Total eventos login: {total_eventos_login:,}")
    print(f"Clientes unicos con login: {clientes_con_login:,}")
    print(f"Clientes unicos sin login: {clientes_sin_login:,}")
    print("-" * 108)
    print(f"Total eventos cambio de password: {total_eventos_cambios:,}")
    print(f"Clientes unicos con cambio de password: {clientes_con_cambio:,}")
    print(f"Clientes unicos con login y cambio de password: {clientes_con_ambos:,}")
    print("-" * 108)

    if df_logins_filtrado.empty:
        print("Desglose canal login: sin eventos")
    else:
        por_canal = (
            df_logins_filtrado.groupby("canal_login", as_index=False)
            .size()
            .rename(columns={"size": "eventos_login"})
            .sort_values(["eventos_login", "canal_login"], ascending=[False, True])
        )
        por_canal["eventos_login"] = por_canal["eventos_login"].astype(int).map(lambda x: f"{x:,}")
        print("Desglose canal login:")
        print(por_canal.to_string(index=False))

    print("-" * 108)
    diario = resumen_diario.copy()
    diario["dia"] = diario["dia"].dt.strftime("%Y-%m-%d")
    for col in ["eventos_logins", "clientes_unicos_login", "eventos_cambios_password", "clientes_unicos_cambio"]:
        diario[col] = diario[col].astype(int).map(lambda x: f"{x:,}")
    print("Resumen diario:")
    print(diario.to_string(index=False))

    print("-" * 108)
    top_logins = (
        df_resumen[df_resumen["total_logins"] > 0][["padded_codigo_cliente", "total_logins"]]
        .sort_values(["total_logins", "padded_codigo_cliente"], ascending=[False, True])
        .head(max(1, top_n))
    )
    top_cambios = (
        df_resumen[df_resumen["total_cambios_password"] > 0][["padded_codigo_cliente", "total_cambios_password"]]
        .sort_values(["total_cambios_password", "padded_codigo_cliente"], ascending=[False, True])
        .head(max(1, top_n))
    )

    if top_logins.empty:
        print(f"Top {max(1, top_n)} clientes por logins: sin datos")
    else:
        top_logins = top_logins.copy()
        top_logins["total_logins"] = top_logins["total_logins"].astype(int).map(lambda x: f"{x:,}")
        print(f"Top {max(1, top_n)} clientes por logins:")
        print(top_logins.to_string(index=False))

    print("-" * 108)
    if top_cambios.empty:
        print(f"Top {max(1, top_n)} clientes por cambios de password: sin datos")
    else:
        top_cambios = top_cambios.copy()
        top_cambios["total_cambios_password"] = (
            top_cambios["total_cambios_password"].astype(int).map(lambda x: f"{x:,}")
        )
        print(f"Top {max(1, top_n)} clientes por cambios de password:")
        print(top_cambios.to_string(index=False))

    print("=" * 108)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Reporte de rendimiento (logins y cambios de password) para clientes contactados "
            "en campana 72049 login."
        )
    )
    parser.add_argument("--fecha-inicio", default="2026-04-18", help="Fecha inicio inclusiva (YYYY-MM-DD).")
    parser.add_argument(
        "--fecha-fin-exclusiva",
        default=fecha_fin_default(),
        help="Fecha fin exclusiva (YYYY-MM-DD). Por defecto: manana respecto a la fecha de ejecucion.",
    )
    parser.add_argument("--top", type=int, default=15, help="Cantidad de clientes para top de logins/cambios.")
    args = parser.parse_args()

    try:
        inicio_ts, fin_ts = validar_fechas(args.fecha_inicio, args.fecha_fin_exclusiva)

        print(f"Cargando clientes contactados desde: {RUTA_QUERY_CLIENTES}")
        clientes = cargar_clientes_contactados()
        total_clientes = int(clientes["padded_codigo_cliente"].nunique())
        print(f"Clientes contactados unicos: {total_clientes:,}")

        if total_clientes == 0:
            print("No se encontraron clientes contactados para la campaña en la query configurada.")
            return

        print("Consultando logins...")
        logins = cargar_logins(args.fecha_inicio, args.fecha_fin_exclusiva)

        print("Consultando cambios de password...")
        cambios = cargar_cambios_password(args.fecha_inicio, args.fecha_fin_exclusiva)

        universo = set(clientes["padded_codigo_cliente"].tolist())
        logins_filtrado = logins[logins["padded_codigo_usuario"].isin(universo)].copy()
        cambios_filtrado = cambios[cambios["padded_codigo_cliente"].isin(universo)].copy()

        resumen_cliente = construir_resumen_por_cliente(
            df_clientes=clientes,
            df_logins_filtrado=logins_filtrado,
            df_cambios_filtrado=cambios_filtrado,
        )

        resumen_diario = construir_resumen_diario(
            df_logins_filtrado=logins_filtrado,
            df_cambios_filtrado=cambios_filtrado,
            fecha_inicio=inicio_ts,
            fecha_fin_exclusiva=fin_ts,
        )

        imprimir_reporte(
            df_resumen=resumen_cliente,
            df_logins_filtrado=logins_filtrado,
            df_cambios_filtrado=cambios_filtrado,
            resumen_diario=resumen_diario,
            fecha_inicio=args.fecha_inicio,
            fecha_fin_exclusiva=args.fecha_fin_exclusiva,
            top_n=max(1, int(args.top)),
        )

    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar consulta SQL: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
