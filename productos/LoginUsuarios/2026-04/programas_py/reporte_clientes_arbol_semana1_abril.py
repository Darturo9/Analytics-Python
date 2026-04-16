"""
reporte_clientes_arbol_semana1_abril.py
---------------------------------------
Reporte en consola para clientes del archivo ClientesArbol (columna "Clientes"),
usando solo la primera semana de abril 2026 (del 1 al 7, inclusive).

Muestra:
- cuantos clientes cambiaron password
- total de eventos login
- total de clientes unicos con login
- detalle por cliente (logins y cambios de password)
- export diario a Excel (logins y cambios por dia, y top dias)

Ejecucion:
    python3 productos/LoginUsuarios/2026-04/programas_py/reporte_clientes_arbol_semana1_abril.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query_file


RUTA_EXCEL_BASE = PROJECT_ROOT / "productos" / "LoginUsuarios" / "2026-04" / "archivosExcel"
RUTA_QUERY_LOGINS = PROJECT_ROOT / "productos" / "LoginUsuarios" / "2026-04" / "queries" / "Logins_01_07_Abril.sql"
RUTA_QUERY_CAMBIOS = PROJECT_ROOT / "productos" / "LoginUsuarios" / "2026-04" / "queries" / "CambiosPassword_01_07_Abril.sql"
RUTA_EXPORTS = PROJECT_ROOT / "productos" / "LoginUsuarios" / "2026-04" / "exports"
RUTA_SALIDA_DIARIO = RUTA_EXPORTS / "Resumen_Diario_Logins_Cambios_2026-04-01_a_2026-04-07.xlsx"

FECHA_INICIO = pd.Timestamp("2026-04-01")
FECHA_FIN_EXCLUSIVA = pd.Timestamp("2026-04-08")


def normalizar_codigo(valor) -> str | None:
    if pd.isna(valor):
        return None
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    if not solo_digitos:
        return None
    return solo_digitos[-8:].zfill(8)


def normalizar_nombre_columna(nombre: str) -> str:
    return (
        str(nombre)
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace(" ", "_")
    )


def buscar_archivo_clientes() -> Path:
    preferidos = [
        "ClientesArbol.xlsx",
        "ClientesArbol.xls",
        "ClientesArbol.csv",
        "clientesarbol.xlsx",
        "clientesarbol.xls",
        "clientesarbol.csv",
        "Clientes_Arbol.xlsx",
        "Clientes_Arbol.xls",
        "Clientes_Arbol.csv",
        "Clientes Arbol.xlsx",
        "Clientes Arbol.xls",
        "Clientes Arbol.csv",
    ]

    for nombre in preferidos:
        candidato = RUTA_EXCEL_BASE / nombre
        if candidato.exists():
            return candidato

    if RUTA_EXCEL_BASE.exists():
        encontrados = sorted(
            p
            for p in RUTA_EXCEL_BASE.iterdir()
            if p.is_file()
            and p.suffix.lower() in {".xlsx", ".xls", ".csv"}
            and "cliente" in p.stem.lower()
            and "arbol" in p.stem.lower()
        )
        if encontrados:
            return encontrados[0]

    raise FileNotFoundError(
        "No se encontro el archivo de base de clientes en "
        f"{RUTA_EXCEL_BASE}. Esperado: ClientesArbol.xlsx/.xls/.csv"
    )


def cargar_clientes_base() -> tuple[pd.DataFrame, Path]:
    archivo = buscar_archivo_clientes()

    if archivo.suffix.lower() in {".xlsx", ".xls"}:
        df_base = pd.read_excel(archivo)
    else:
        df_base = pd.read_csv(archivo)

    columnas_norm = {col: normalizar_nombre_columna(col) for col in df_base.columns}
    columna_clientes = None
    for original, normalizada in columnas_norm.items():
        if normalizada == "clientes":
            columna_clientes = original
            break

    if columna_clientes is None:
        raise ValueError(
            f"El archivo {archivo.name} no tiene la columna 'Clientes'. "
            f"Columnas encontradas: {list(df_base.columns)}"
        )

    df_base["padded_codigo_cliente"] = df_base[columna_clientes].apply(normalizar_codigo)
    df_base = df_base[df_base["padded_codigo_cliente"].notna()].copy()
    df_base = df_base.drop_duplicates(subset=["padded_codigo_cliente"])

    return df_base[["padded_codigo_cliente"]].copy(), archivo


def cargar_logins() -> pd.DataFrame:
    df_logins = run_query_file(str(RUTA_QUERY_LOGINS))
    df_logins.columns = [str(c) for c in df_logins.columns]

    if "padded_codigo_usuario" not in df_logins.columns:
        if "codigo_usuario" in df_logins.columns:
            df_logins["padded_codigo_usuario"] = df_logins["codigo_usuario"].apply(normalizar_codigo)
        else:
            raise ValueError("La query de logins debe devolver padded_codigo_usuario o codigo_usuario.")

    if "fecha_inicio" not in df_logins.columns:
        raise ValueError("La query de logins debe devolver la columna fecha_inicio.")

    df_logins["padded_codigo_usuario"] = df_logins["padded_codigo_usuario"].apply(normalizar_codigo)
    df_logins["fecha_inicio"] = pd.to_datetime(df_logins["fecha_inicio"], errors="coerce")
    df_logins = df_logins[
        df_logins["padded_codigo_usuario"].notna() & df_logins["fecha_inicio"].notna()
    ].copy()
    df_logins = df_logins[
        (df_logins["fecha_inicio"] >= FECHA_INICIO)
        & (df_logins["fecha_inicio"] < FECHA_FIN_EXCLUSIVA)
    ].copy()

    return df_logins


def cargar_cambios_password() -> pd.DataFrame:
    df_cambios = run_query_file(str(RUTA_QUERY_CAMBIOS))
    df_cambios.columns = [normalizar_nombre_columna(c) for c in df_cambios.columns]

    if "codigo_cliente" not in df_cambios.columns or "fecha_cambio_pass" not in df_cambios.columns:
        raise ValueError(
            "La query de cambios de password debe devolver codigo_cliente y fecha_cambio_pass."
        )

    df_cambios["padded_codigo_cliente"] = df_cambios["codigo_cliente"].apply(normalizar_codigo)
    df_cambios["fecha_cambio_pass"] = pd.to_datetime(df_cambios["fecha_cambio_pass"], errors="coerce")
    df_cambios = df_cambios[
        df_cambios["padded_codigo_cliente"].notna() & df_cambios["fecha_cambio_pass"].notna()
    ].copy()
    df_cambios = df_cambios[
        (df_cambios["fecha_cambio_pass"] >= FECHA_INICIO)
        & (df_cambios["fecha_cambio_pass"] < FECHA_FIN_EXCLUSIVA)
    ].copy()

    return df_cambios


def construir_resumen_por_cliente(
    df_base: pd.DataFrame,
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
        df_base.merge(
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


def _construir_resumen_diario(
    df_logins_filtrado: pd.DataFrame,
    df_cambios_filtrado: pd.DataFrame,
) -> tuple[pd.Series, pd.Series, pd.DataFrame]:
    fechas = pd.date_range(FECHA_INICIO, FECHA_FIN_EXCLUSIVA - pd.Timedelta(days=1), freq="D")

    logins_por_dia = (
        df_logins_filtrado.assign(dia=df_logins_filtrado["fecha_inicio"].dt.normalize())
        .groupby("dia")
        .size()
        .reindex(fechas, fill_value=0)
    )

    cambios_por_dia = (
        df_cambios_filtrado.assign(dia=df_cambios_filtrado["fecha_cambio_pass"].dt.normalize())
        .groupby("dia")
        .size()
        .reindex(fechas, fill_value=0)
    )

    detalle_diario = pd.DataFrame(
        {
            "dia": fechas,
            "total_logins": logins_por_dia.values,
            "total_cambios_password": cambios_por_dia.values,
        }
    )
    return logins_por_dia, cambios_por_dia, detalle_diario


def exportar_resumen_diario_excel(
    logins_por_dia: pd.Series,
    cambios_por_dia: pd.Series,
    detalle_diario: pd.DataFrame,
) -> Path:
    top_dias_logins = (
        logins_por_dia.reset_index()
        .rename(columns={"index": "dia", 0: "total_logins"})
        .sort_values(["total_logins", "dia"], ascending=[False, True])
        .head(5)
    )
    top_dias_cambios = (
        cambios_por_dia.reset_index()
        .rename(columns={"index": "dia", 0: "total_cambios_password"})
        .sort_values(["total_cambios_password", "dia"], ascending=[False, True])
        .head(5)
    )

    detalle_export = detalle_diario.copy()
    detalle_export["dia"] = detalle_export["dia"].dt.strftime("%Y-%m-%d")
    top_logins_export = top_dias_logins.copy()
    top_logins_export["dia"] = top_logins_export["dia"].dt.strftime("%Y-%m-%d")
    top_cambios_export = top_dias_cambios.copy()
    top_cambios_export["dia"] = top_cambios_export["dia"].dt.strftime("%Y-%m-%d")

    RUTA_EXPORTS.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(RUTA_SALIDA_DIARIO) as writer:
        detalle_export.to_excel(writer, sheet_name="detalle_diario", index=False)
        top_logins_export.to_excel(writer, sheet_name="top_dias_logins", index=False)
        top_cambios_export.to_excel(writer, sheet_name="top_dias_cambios", index=False)

    return RUTA_SALIDA_DIARIO


def imprimir_reporte(
    df_resumen: pd.DataFrame,
    ruta_archivo: Path,
    ruta_salida_excel: Path,
) -> None:
    total_clientes_excel = int(df_resumen["padded_codigo_cliente"].nunique())
    total_logins = int(df_resumen["total_logins"].sum())
    clientes_unicos_login = int((df_resumen["total_logins"] > 0).sum())
    clientes_cambio_password = int((df_resumen["total_cambios_password"] > 0).sum())

    print("=" * 96)
    print("REPORTE CLIENTES ARBOL - SEMANA 1 ABRIL 2026 (2026-04-01 al 2026-04-07)")
    print("=" * 96)
    print(f"Archivo base: {ruta_archivo}")
    print(f"Clientes en Excel: {total_clientes_excel:,}")
    print("-" * 96)
    print(f"Total de logins (eventos): {total_logins:,}")
    print(f"Logins de clientes unicos: {clientes_unicos_login:,}")
    print(f"Clientes que cambiaron password: {clientes_cambio_password:,}")
    print("-" * 96)

    detalle_clientes = df_resumen[
        (df_resumen["total_logins"] > 0) | (df_resumen["total_cambios_password"] > 0)
    ].copy()
    detalle_clientes = detalle_clientes.sort_values(
        ["total_logins", "total_cambios_password", "padded_codigo_cliente"],
        ascending=[False, False, True],
    )

    print("CLIENTE / LOGINS / CAMBIOS DE PASSWORD")
    if detalle_clientes.empty:
        print("No hay clientes con actividad en el periodo.")
    else:
        print(
            detalle_clientes[
                ["padded_codigo_cliente", "total_logins", "total_cambios_password"]
            ].to_string(index=False)
        )
    print("-" * 96)

    print(f"Export diario generado: {ruta_salida_excel}")
    print("=" * 96)


def main() -> None:
    print(f"Cargando base de clientes desde: {RUTA_EXCEL_BASE}")
    print(f"Cargando logins desde: {RUTA_QUERY_LOGINS}")
    print(f"Cargando cambios password desde: {RUTA_QUERY_CAMBIOS}")

    try:
        df_base, ruta_archivo = cargar_clientes_base()
        df_logins = cargar_logins()
        df_cambios = cargar_cambios_password()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar una query SQL: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    universo = set(df_base["padded_codigo_cliente"].tolist())
    df_logins_filtrado = df_logins[df_logins["padded_codigo_usuario"].isin(universo)].copy()
    df_cambios_filtrado = df_cambios[df_cambios["padded_codigo_cliente"].isin(universo)].copy()

    df_resumen = construir_resumen_por_cliente(
        df_base=df_base,
        df_logins_filtrado=df_logins_filtrado,
        df_cambios_filtrado=df_cambios_filtrado,
    )

    logins_por_dia, cambios_por_dia, detalle_diario = _construir_resumen_diario(
        df_logins_filtrado=df_logins_filtrado,
        df_cambios_filtrado=df_cambios_filtrado,
    )
    ruta_salida_excel = exportar_resumen_diario_excel(
        logins_por_dia=logins_por_dia,
        cambios_por_dia=cambios_por_dia,
        detalle_diario=detalle_diario,
    )

    imprimir_reporte(
        df_resumen=df_resumen,
        ruta_archivo=ruta_archivo,
        ruta_salida_excel=ruta_salida_excel,
    )


if __name__ == "__main__":
    main()
