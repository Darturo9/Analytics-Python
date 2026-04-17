"""
reporte_clientes_arbol_comparativo_marzo16_abril17_2026.py
-----------------------------------------------------------
Reporte comparativo para clientes del archivo ClientesArbol (columna "Clientes").

Ventanas evaluadas:
- Marzo: 2026-03-16 al 2026-03-31
- Abril: 2026-04-01 al 2026-04-17

Muestra en consola:
- total de logins
- clientes unicos con login
- total de cambios de password
- clientes unicos con cambio de password
- comparativa Marzo vs Abril (delta absoluto y porcentual)

Salidas:
- Excel con resumen y detalle diario por periodo
- TXT de clientes con cambio de password (abril 1-17) en formato 'numerocliente',

Ejecucion:
    python3 productos/LoginUsuarios/2026-04/programas_py/reporte_clientes_arbol_comparativo_marzo16_abril17_2026.py
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
RUTA_QUERY_LOGINS = (
    PROJECT_ROOT
    / "productos"
    / "LoginUsuarios"
    / "2026-04"
    / "queries"
    / "Logins_2026-03-16_a_2026-04-17.sql"
)
RUTA_QUERY_CAMBIOS = (
    PROJECT_ROOT
    / "productos"
    / "LoginUsuarios"
    / "2026-04"
    / "queries"
    / "CambiosPassword_2026-03-16_a_2026-04-17.sql"
)
RUTA_EXPORTS = PROJECT_ROOT / "productos" / "LoginUsuarios" / "2026-04" / "exports"
RUTA_SALIDA_EXCEL = (
    RUTA_EXPORTS / "Resumen_Comparativo_Logins_Cambios_ClientesArbol_Mar16_Abr17_2026.xlsx"
)
RUTA_SALIDA_CLIENTES_CAMBIO = RUTA_EXPORTS / "Clientes_Cambio_Password_Abril01_17_Management.txt"

PERIODO_MARZO_INICIO = pd.Timestamp("2026-03-16")
PERIODO_MARZO_FIN_EXCLUSIVA = pd.Timestamp("2026-04-01")
PERIODO_ABRIL_INICIO = pd.Timestamp("2026-04-01")
PERIODO_ABRIL_FIN_EXCLUSIVA = pd.Timestamp("2026-04-18")

PERIODOS = [
    {
        "id": "MARZO_16_31",
        "label": "2026-03-16 al 2026-03-31",
        "inicio": PERIODO_MARZO_INICIO,
        "fin_exclusiva": PERIODO_MARZO_FIN_EXCLUSIVA,
    },
    {
        "id": "ABRIL_01_17",
        "label": "2026-04-01 al 2026-04-17",
        "inicio": PERIODO_ABRIL_INICIO,
        "fin_exclusiva": PERIODO_ABRIL_FIN_EXCLUSIVA,
    },
]


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
    return df_cambios


def filtrar_por_periodo(df: pd.DataFrame, fecha_col: str, inicio: pd.Timestamp, fin_exclusiva: pd.Timestamp) -> pd.DataFrame:
    return df[(df[fecha_col] >= inicio) & (df[fecha_col] < fin_exclusiva)].copy()


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


def construir_resumen_diario(
    df_logins_filtrado: pd.DataFrame,
    df_cambios_filtrado: pd.DataFrame,
    inicio: pd.Timestamp,
    fin_exclusiva: pd.Timestamp,
) -> pd.DataFrame:
    fechas = pd.date_range(inicio, fin_exclusiva - pd.Timedelta(days=1), freq="D")
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
    return pd.DataFrame(
        {
            "dia": fechas,
            "total_logins": logins_por_dia.values,
            "total_cambios_password": cambios_por_dia.values,
        }
    )


def construir_resultado_periodo(
    periodo: dict,
    df_base: pd.DataFrame,
    df_logins_universo: pd.DataFrame,
    df_cambios_universo: pd.DataFrame,
) -> dict:
    inicio = periodo["inicio"]
    fin_exclusiva = periodo["fin_exclusiva"]

    df_logins = filtrar_por_periodo(df_logins_universo, "fecha_inicio", inicio, fin_exclusiva)
    df_cambios = filtrar_por_periodo(df_cambios_universo, "fecha_cambio_pass", inicio, fin_exclusiva)
    df_resumen = construir_resumen_por_cliente(
        df_base=df_base,
        df_logins_filtrado=df_logins,
        df_cambios_filtrado=df_cambios,
    )
    df_detalle_diario = construir_resumen_diario(
        df_logins_filtrado=df_logins,
        df_cambios_filtrado=df_cambios,
        inicio=inicio,
        fin_exclusiva=fin_exclusiva,
    )

    total_clientes_excel = int(df_resumen["padded_codigo_cliente"].nunique())
    total_logins = int(df_resumen["total_logins"].sum())
    clientes_unicos_login = int((df_resumen["total_logins"] > 0).sum())
    total_cambios_password = int(df_resumen["total_cambios_password"].sum())
    clientes_unicos_cambio_password = int((df_resumen["total_cambios_password"] > 0).sum())

    df_detalle_clientes = df_resumen[
        (df_resumen["total_logins"] > 0) | (df_resumen["total_cambios_password"] > 0)
    ].copy()
    df_detalle_clientes = df_detalle_clientes.sort_values(
        ["total_logins", "total_cambios_password", "padded_codigo_cliente"],
        ascending=[False, False, True],
    ).reset_index(drop=True)

    return {
        "id": periodo["id"],
        "label": periodo["label"],
        "inicio": inicio,
        "fin_exclusiva": fin_exclusiva,
        "df_logins": df_logins,
        "df_cambios": df_cambios,
        "df_resumen": df_resumen,
        "df_detalle_clientes": df_detalle_clientes,
        "df_detalle_diario": df_detalle_diario,
        "metrica": {
            "periodo_id": periodo["id"],
            "periodo_label": periodo["label"],
            "fecha_inicio": inicio.date().isoformat(),
            "fecha_fin": (fin_exclusiva - pd.Timedelta(days=1)).date().isoformat(),
            "clientes_excel": total_clientes_excel,
            "total_logins": total_logins,
            "clientes_unicos_login": clientes_unicos_login,
            "total_cambios_password": total_cambios_password,
            "clientes_unicos_cambio_password": clientes_unicos_cambio_password,
        },
    }


def construir_comparativa(df_resumen_periodos: pd.DataFrame) -> pd.DataFrame:
    marzo = df_resumen_periodos[df_resumen_periodos["periodo_id"] == "MARZO_16_31"]
    abril = df_resumen_periodos[df_resumen_periodos["periodo_id"] == "ABRIL_01_17"]
    if marzo.empty or abril.empty:
        return pd.DataFrame()

    marzo_row = marzo.iloc[0]
    abril_row = abril.iloc[0]

    metricas = [
        ("total_logins", "Total logins"),
        ("clientes_unicos_login", "Clientes unicos con login"),
        ("total_cambios_password", "Total cambios password"),
        ("clientes_unicos_cambio_password", "Clientes unicos con cambio password"),
    ]

    filas = []
    for key, label in metricas:
        valor_marzo = int(marzo_row[key])
        valor_abril = int(abril_row[key])
        delta_abs = valor_abril - valor_marzo
        delta_pct = None if valor_marzo == 0 else (delta_abs * 100.0 / valor_marzo)
        filas.append(
            {
                "metrica": label,
                "marzo_16_31": valor_marzo,
                "abril_01_17": valor_abril,
                "delta_abs_abril_vs_marzo": delta_abs,
                "delta_pct_abril_vs_marzo": delta_pct,
            }
        )
    return pd.DataFrame(filas)


def exportar_clientes_cambio_password_abril(df_cambios_abril: pd.DataFrame) -> Path:
    clientes = sorted(df_cambios_abril["padded_codigo_cliente"].dropna().astype(str).unique().tolist())
    RUTA_EXPORTS.mkdir(parents=True, exist_ok=True)
    with open(RUTA_SALIDA_CLIENTES_CAMBIO, "w", encoding="utf-8") as f:
        for codigo in clientes:
            f.write(f"'{codigo}',\n")
    return RUTA_SALIDA_CLIENTES_CAMBIO


def exportar_excel(
    df_resumen_periodos: pd.DataFrame,
    df_comparativa: pd.DataFrame,
    resultados_por_periodo: dict[str, dict],
) -> Path:
    RUTA_EXPORTS.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(RUTA_SALIDA_EXCEL) as writer:
        df_resumen_periodos.to_excel(writer, sheet_name="resumen_periodos", index=False)
        df_comparativa.to_excel(writer, sheet_name="comparativa", index=False)

        marzo = resultados_por_periodo["MARZO_16_31"]
        abril = resultados_por_periodo["ABRIL_01_17"]

        marzo_diario = marzo["df_detalle_diario"].copy()
        marzo_diario["dia"] = marzo_diario["dia"].dt.strftime("%Y-%m-%d")
        marzo_diario.to_excel(writer, sheet_name="diario_marzo_16_31", index=False)

        abril_diario = abril["df_detalle_diario"].copy()
        abril_diario["dia"] = abril_diario["dia"].dt.strftime("%Y-%m-%d")
        abril_diario.to_excel(writer, sheet_name="diario_abril_01_17", index=False)

        marzo["df_detalle_clientes"].to_excel(writer, sheet_name="clientes_activos_marzo", index=False)
        abril["df_detalle_clientes"].to_excel(writer, sheet_name="clientes_activos_abril", index=False)

    return RUTA_SALIDA_EXCEL


def imprimir_reporte(
    ruta_archivo: Path,
    df_resumen_periodos: pd.DataFrame,
    df_comparativa: pd.DataFrame,
    resultados_por_periodo: dict[str, dict],
    ruta_excel: Path,
    ruta_clientes_cambio: Path,
) -> None:
    print("=" * 108)
    print("REPORTE CLIENTES ARBOL - COMPARATIVO MARZO 16-31 VS ABRIL 01-17 (2026)")
    print("=" * 108)
    print(f"Archivo base: {ruta_archivo}")
    print(f"Clientes en Excel: {int(df_resumen_periodos['clientes_excel'].max()):,}")
    print("-" * 108)

    for _, row in df_resumen_periodos.iterrows():
        print(f"Periodo: {row['periodo_label']}")
        print(f"  - Total logins: {int(row['total_logins']):,}")
        print(f"  - Clientes unicos con login: {int(row['clientes_unicos_login']):,}")
        print(f"  - Total cambios password: {int(row['total_cambios_password']):,}")
        print(f"  - Clientes unicos con cambio password: {int(row['clientes_unicos_cambio_password']):,}")
        print("-" * 108)

    print("Comparativa Abril (01-17) vs Marzo (16-31):")
    if df_comparativa.empty:
        print("No fue posible construir la comparativa.")
    else:
        comparativa_fmt = df_comparativa.copy()
        comparativa_fmt["delta_pct_abril_vs_marzo"] = comparativa_fmt["delta_pct_abril_vs_marzo"].apply(
            lambda x: "" if pd.isna(x) else f"{x:,.2f}%"
        )
        print(comparativa_fmt.to_string(index=False))
    print("-" * 108)

    abril_detalle = resultados_por_periodo["ABRIL_01_17"]["df_detalle_clientes"].copy()
    print("Detalle clientes activos abril (primeros 50):")
    if abril_detalle.empty:
        print("No hay clientes con actividad en abril 01-17.")
    else:
        print(abril_detalle.head(50).to_string(index=False))
        if len(abril_detalle) > 50:
            print(f"... ({len(abril_detalle) - 50:,} filas adicionales en el Excel)")
    print("-" * 108)

    print(f"Excel comparativo generado: {ruta_excel}")
    print(f"Lista clientes cambio password abril (management): {ruta_clientes_cambio}")
    print("=" * 108)


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
    df_logins_universo = df_logins[df_logins["padded_codigo_usuario"].isin(universo)].copy()
    df_cambios_universo = df_cambios[df_cambios["padded_codigo_cliente"].isin(universo)].copy()

    resultados = {}
    metricas = []
    for periodo in PERIODOS:
        resultado = construir_resultado_periodo(
            periodo=periodo,
            df_base=df_base,
            df_logins_universo=df_logins_universo,
            df_cambios_universo=df_cambios_universo,
        )
        resultados[periodo["id"]] = resultado
        metricas.append(resultado["metrica"])

    df_resumen_periodos = pd.DataFrame(metricas)
    df_comparativa = construir_comparativa(df_resumen_periodos)

    ruta_clientes_cambio = exportar_clientes_cambio_password_abril(
        resultados["ABRIL_01_17"]["df_cambios"]
    )
    ruta_excel = exportar_excel(
        df_resumen_periodos=df_resumen_periodos,
        df_comparativa=df_comparativa,
        resultados_por_periodo=resultados,
    )

    imprimir_reporte(
        ruta_archivo=ruta_archivo,
        df_resumen_periodos=df_resumen_periodos,
        df_comparativa=df_comparativa,
        resultados_por_periodo=resultados,
        ruta_excel=ruta_excel,
        ruta_clientes_cambio=ruta_clientes_cambio,
    )


if __name__ == "__main__":
    main()
