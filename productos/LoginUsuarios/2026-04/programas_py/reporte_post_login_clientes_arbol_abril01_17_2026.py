"""
reporte_post_login_clientes_arbol_abril01_17_2026.py
----------------------------------------------------
Reporte en consola + export Excel de actividad post-login para clientes de
ClientesArbol (columna "Clientes") en la ventana 2026-04-01 al 2026-04-17.

Usa 4 fuentes de eventos:
- BEL (dw_bel_IBSTTRA_VIEW)
- JOURNAL (dw_BEL_IBJOUR)
- ACH (DW_DEP_DPMOVM_VIEW)
- MULTIPAGOS (DW_MUL_SPPADAT)

Muestra:
- clientes base
- clientes con login
- clientes con actividad post-login
- total de eventos post-login
- top 15 tipos de accion post-login
- top 15 operaciones post-login
- top 15 dias con mas eventos post-login
- top de eventos por fuente

Ejecucion:
    python3 productos/LoginUsuarios/2026-04/programas_py/reporte_post_login_clientes_arbol_abril01_17_2026.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query, run_query_file


RUTA_EXCEL_BASE = PROJECT_ROOT / "productos" / "LoginUsuarios" / "2026-04" / "archivosExcel"
RUTA_QUERY_LOGINS = PROJECT_ROOT / "productos" / "LoginUsuarios" / "2026-04" / "queries" / "Logins_01_17_Abril.sql"
RUTA_QUERY_POST_LOGIN = (
    PROJECT_ROOT
    / "productos"
    / "LoginUsuarios"
    / "2026-04"
    / "queries"
    / "PostLogin_Operaciones_01_17_Abril.sql"
)
RUTA_EXPORTS = PROJECT_ROOT / "productos" / "LoginUsuarios" / "2026-04" / "exports"
RUTA_SALIDA_EXCEL = RUTA_EXPORTS / "Resumen_PostLogin_ClientesArbol_Abril01_17_2026_Top15.xlsx"

FECHA_INICIO = pd.Timestamp("2026-04-01")
FECHA_FIN_EXCLUSIVA = pd.Timestamp("2026-04-18")
PLACEHOLDER_CLIENTES_VALUES = "{{CLIENTES_BASE_VALUES}}"
TAM_CHUNK_CLIENTES_SQL = 500
TOP_N = 15


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


def construir_values_clientes(codigos: list[str]) -> str:
    codigos_validos = sorted(
        {
            c
            for c in codigos
            if isinstance(c, str)
            and len(c.strip()) == 8
            and c.strip().isdigit()
        }
    )
    if not codigos_validos:
        raise ValueError("No hay codigos de cliente validos para consultar eventos post-login.")
    return ",\n            ".join(f"('{c.strip()}')" for c in codigos_validos)


def cargar_eventos_operaciones_clientes(codigos_base: list[str]) -> pd.DataFrame:
    columnas_base = [
        "fecha_evento",
        "codigo_usuario",
        "padded_codigo_usuario",
        "canal",
        "fuente",
        "secode",
        "modulo",
        "operacion",
        "valor",
        "valorlempirizado",
    ]
    if not codigos_base:
        return pd.DataFrame(columns=columnas_base)

    with open(RUTA_QUERY_POST_LOGIN, "r", encoding="utf-8") as f:
        sql_template = f.read()
    if PLACEHOLDER_CLIENTES_VALUES not in sql_template:
        raise ValueError(
            f"La query de post-login no contiene el placeholder {PLACEHOLDER_CLIENTES_VALUES}."
        )

    frames: list[pd.DataFrame] = []
    codigos_ordenados = sorted(set(codigos_base))
    for i in range(0, len(codigos_ordenados), TAM_CHUNK_CLIENTES_SQL):
        chunk = codigos_ordenados[i : i + TAM_CHUNK_CLIENTES_SQL]
        values_sql = construir_values_clientes(chunk)
        sql = sql_template.replace(PLACEHOLDER_CLIENTES_VALUES, values_sql)
        df_chunk = run_query(sql)
        df_chunk.columns = [normalizar_nombre_columna(c) for c in df_chunk.columns]
        frames.append(df_chunk)

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=columnas_base)
    if df.empty:
        return pd.DataFrame(columns=columnas_base)

    if "padded_codigo_usuario" not in df.columns:
        if "padded_codigo_cliente" in df.columns:
            df["padded_codigo_usuario"] = df["padded_codigo_cliente"].apply(normalizar_codigo)
        elif "codigo_usuario" in df.columns:
            df["padded_codigo_usuario"] = df["codigo_usuario"].apply(normalizar_codigo)
        else:
            raise ValueError(
                "La query de post-login debe devolver padded_codigo_usuario o padded/codigo cliente."
            )
    if "fecha_evento" not in df.columns:
        if "fecha" in df.columns:
            df["fecha_evento"] = df["fecha"]
        else:
            raise ValueError("La query de post-login debe devolver fecha_evento.")

    for col, default in {
        "codigo_usuario": "",
        "canal": "Sin canal",
        "fuente": "Sin fuente",
        "secode": "",
        "modulo": "",
        "operacion": "",
        "valor": 0,
        "valorlempirizado": 0,
    }.items():
        if col not in df.columns:
            df[col] = default

    df["padded_codigo_usuario"] = df["padded_codigo_usuario"].apply(normalizar_codigo)
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"], errors="coerce")
    for col in ["codigo_usuario", "canal", "fuente", "secode", "modulo", "operacion"]:
        df[col] = df[col].fillna("").astype(str).str.strip()
    for col in ["valor", "valorlempirizado"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df[
        df["padded_codigo_usuario"].notna()
        & df["fecha_evento"].notna()
        & (df["secode"] != "")
    ].copy()
    df = df[
        (df["fecha_evento"] >= FECHA_INICIO)
        & (df["fecha_evento"] < FECHA_FIN_EXCLUSIVA)
    ].copy()
    return df[columnas_base]


def clasificar_actividad_post_login(secode: str, modulo: str, operacion: str) -> str:
    texto = f"{secode} {modulo} {operacion}".lower()
    if any(x in texto for x in ["edocta", "estado de cuenta", "con-sal", "saldo", "cns", "consulta"]):
        return "Consultas / Estado de cuenta"
    if any(x in texto for x in ["pag", "pago", "mpg", "multipago", "tcpago", "cpago"]):
        return "Pagos"
    if any(x in texto for x in ["ach", "transf", "transfer", "traint", "transh", "trach"]):
        return "Transferencias"
    if any(x in texto for x in ["ptm", "prestamo", "ptr-", "pym-"]):
        return "Prestamos"
    return "Otras operaciones"


def es_login_evento(secode: str, modulo: str, operacion: str) -> bool:
    se = str(secode).strip().lower()
    md = str(modulo).strip().lower()
    op = str(operacion).strip().lower()
    return se in {"login", "web-login", "app-login"} or "login" in md or "login" in op


def construir_eventos_post_login(
    df_logins_filtrado: pd.DataFrame,
    df_eventos_operaciones: pd.DataFrame,
) -> pd.DataFrame:
    if df_logins_filtrado.empty or df_eventos_operaciones.empty:
        return pd.DataFrame()

    primer_login = (
        df_logins_filtrado.groupby("padded_codigo_usuario", as_index=False)["fecha_inicio"]
        .min()
        .rename(columns={"fecha_inicio": "primer_login"})
    )
    df_post = df_eventos_operaciones.merge(
        primer_login,
        how="inner",
        on="padded_codigo_usuario",
    )
    df_post = df_post[df_post["fecha_evento"] >= df_post["primer_login"]].copy()
    if df_post.empty:
        return df_post

    df_post = df_post[
        ~df_post.apply(
            lambda r: es_login_evento(r.get("secode", ""), r.get("modulo", ""), r.get("operacion", "")),
            axis=1,
        )
    ].copy()
    if df_post.empty:
        return df_post

    df_post["tipo_actividad"] = df_post.apply(
        lambda r: clasificar_actividad_post_login(
            r.get("secode", ""),
            r.get("modulo", ""),
            r.get("operacion", ""),
        ),
        axis=1,
    )
    df_post["dia"] = df_post["fecha_evento"].dt.normalize()
    return df_post


def construir_resumenes(
    total_clientes_base: int,
    clientes_unicos_login: int,
    df_post_login: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if df_post_login.empty:
        resumen = pd.DataFrame(
            [
                {
                    "fecha_inicio": FECHA_INICIO.date().isoformat(),
                    "fecha_fin": (FECHA_FIN_EXCLUSIVA - pd.Timedelta(days=1)).date().isoformat(),
                    "clientes_base": total_clientes_base,
                    "clientes_con_login": clientes_unicos_login,
                    "clientes_con_actividad_post_login": 0,
                    "pct_post_login_sobre_login": 0.0,
                    "eventos_post_login": 0,
                    "monto_total_post_login": 0.0,
                }
            ]
        )
        vacio = pd.DataFrame()
        return resumen, vacio, vacio, vacio, vacio

    clientes_post = int(df_post_login["padded_codigo_usuario"].nunique())
    total_eventos_post = int(len(df_post_login))
    pct_clientes_post = (clientes_post * 100.0 / clientes_unicos_login) if clientes_unicos_login > 0 else 0.0
    monto_total_post = float(df_post_login["valorlempirizado"].fillna(0).sum())

    resumen = pd.DataFrame(
        [
            {
                "fecha_inicio": FECHA_INICIO.date().isoformat(),
                "fecha_fin": (FECHA_FIN_EXCLUSIVA - pd.Timedelta(days=1)).date().isoformat(),
                "clientes_base": total_clientes_base,
                "clientes_con_login": clientes_unicos_login,
                "clientes_con_actividad_post_login": clientes_post,
                "pct_post_login_sobre_login": pct_clientes_post,
                "eventos_post_login": total_eventos_post,
                "monto_total_post_login": monto_total_post,
            }
        ]
    )

    top_tipos = (
        df_post_login.groupby("tipo_actividad", as_index=False)
        .agg(
            eventos=("tipo_actividad", "size"),
            clientes_unicos=("padded_codigo_usuario", "nunique"),
            monto_total=("valorlempirizado", "sum"),
        )
        .sort_values(["eventos", "tipo_actividad"], ascending=[False, True])
        .head(TOP_N)
        .reset_index(drop=True)
    )

    top_operaciones = (
        df_post_login.groupby(["operacion", "secode", "modulo"], as_index=False)
        .agg(
            eventos=("operacion", "size"),
            clientes_unicos=("padded_codigo_usuario", "nunique"),
            monto_total=("valorlempirizado", "sum"),
        )
        .sort_values(["eventos", "operacion"], ascending=[False, True])
        .head(TOP_N)
        .reset_index(drop=True)
    )

    top_dias = (
        df_post_login.groupby("dia", as_index=False)
        .agg(
            eventos=("dia", "size"),
            clientes_unicos=("padded_codigo_usuario", "nunique"),
            monto_total=("valorlempirizado", "sum"),
        )
        .sort_values(["eventos", "dia"], ascending=[False, True])
        .head(TOP_N)
        .reset_index(drop=True)
    )
    top_dias["dia"] = pd.to_datetime(top_dias["dia"], errors="coerce").dt.strftime("%Y-%m-%d")

    top_fuentes = (
        df_post_login.groupby("fuente", as_index=False)
        .agg(
            eventos=("fuente", "size"),
            clientes_unicos=("padded_codigo_usuario", "nunique"),
            monto_total=("valorlempirizado", "sum"),
        )
        .sort_values(["eventos", "fuente"], ascending=[False, True])
        .reset_index(drop=True)
    )

    return resumen, top_tipos, top_operaciones, top_dias, top_fuentes


def exportar_excel(
    resumen: pd.DataFrame,
    top_tipos: pd.DataFrame,
    top_operaciones: pd.DataFrame,
    top_dias: pd.DataFrame,
    top_fuentes: pd.DataFrame,
    df_post_login: pd.DataFrame,
) -> Path:
    RUTA_EXPORTS.mkdir(parents=True, exist_ok=True)

    detalle = df_post_login.copy()
    if not detalle.empty:
        detalle["fecha_evento"] = pd.to_datetime(detalle["fecha_evento"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        detalle["primer_login"] = pd.to_datetime(detalle["primer_login"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        if "dia" in detalle.columns:
            detalle["dia"] = pd.to_datetime(detalle["dia"], errors="coerce").dt.strftime("%Y-%m-%d")

    with pd.ExcelWriter(RUTA_SALIDA_EXCEL, engine="openpyxl") as writer:
        resumen.to_excel(writer, sheet_name="ResumenGeneral", index=False)
        top_tipos.to_excel(writer, sheet_name="Top15Tipos", index=False)
        top_operaciones.to_excel(writer, sheet_name="Top15Operaciones", index=False)
        top_dias.to_excel(writer, sheet_name="Top15Dias", index=False)
        top_fuentes.to_excel(writer, sheet_name="EventosPorFuente", index=False)
        detalle.to_excel(writer, sheet_name="DetallePostLogin", index=False)

    return RUTA_SALIDA_EXCEL


def imprimir_reporte(
    ruta_archivo: Path,
    resumen: pd.DataFrame,
    top_tipos: pd.DataFrame,
    top_operaciones: pd.DataFrame,
    top_dias: pd.DataFrame,
    top_fuentes: pd.DataFrame,
    ruta_excel: Path,
) -> None:
    row = resumen.iloc[0]
    print("=" * 112)
    print("REPORTE POST-LOGIN CLIENTES ARBOL - ABRIL 2026 (2026-04-01 al 2026-04-17)")
    print("=" * 112)
    print(f"Archivo base: {ruta_archivo}")
    print(f"Clientes en Excel: {int(row['clientes_base']):,}")
    print(f"Clientes unicos con login: {int(row['clientes_con_login']):,}")
    print(f"Clientes unicos con actividad post-login: {int(row['clientes_con_actividad_post_login']):,}")
    print(f"% sobre clientes con login: {float(row['pct_post_login_sobre_login']):,.2f}%")
    print(f"Total eventos post-login: {int(row['eventos_post_login']):,}")
    print(f"Monto total post-login (L): {float(row['monto_total_post_login']):,.2f}")
    print("-" * 112)

    if top_fuentes.empty:
        print("No se encontraron eventos post-login para el periodo.")
        print(f"Excel generado: {ruta_excel}")
        print("=" * 112)
        return

    print("EVENTOS POR FUENTE (4 FUENTES)")
    print(top_fuentes.to_string(index=False))
    print("-" * 112)

    print(f"TOP {TOP_N} TIPOS DE ACCION POST-LOGIN")
    print(top_tipos.to_string(index=False))
    print("-" * 112)

    print(f"TOP {TOP_N} OPERACIONES POST-LOGIN")
    print(top_operaciones.to_string(index=False))
    print("-" * 112)

    print(f"TOP {TOP_N} DIAS CON MAS EVENTOS POST-LOGIN")
    print(top_dias.to_string(index=False))
    print("-" * 112)
    print(f"Excel generado: {ruta_excel}")
    print("=" * 112)


def main() -> None:
    print(f"Cargando base de clientes desde: {RUTA_EXCEL_BASE}")
    print(f"Cargando logins desde: {RUTA_QUERY_LOGINS}")
    print(f"Cargando operaciones post-login desde: {RUTA_QUERY_POST_LOGIN}")

    try:
        df_base, ruta_archivo = cargar_clientes_base()
        df_logins = cargar_logins()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar una query SQL: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    universo = set(df_base["padded_codigo_cliente"].tolist())
    df_logins_filtrado = df_logins[df_logins["padded_codigo_usuario"].isin(universo)].copy()
    clientes_unicos_login = int(df_logins_filtrado["padded_codigo_usuario"].nunique())

    try:
        df_eventos_operaciones = cargar_eventos_operaciones_clientes(sorted(universo))
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar query de post-login: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo procesando query de post-login: {exc}")
        raise SystemExit(1) from exc

    df_post_login = construir_eventos_post_login(
        df_logins_filtrado=df_logins_filtrado,
        df_eventos_operaciones=df_eventos_operaciones,
    )

    resumen, top_tipos, top_operaciones, top_dias, top_fuentes = construir_resumenes(
        total_clientes_base=int(df_base["padded_codigo_cliente"].nunique()),
        clientes_unicos_login=clientes_unicos_login,
        df_post_login=df_post_login,
    )
    ruta_excel = exportar_excel(
        resumen=resumen,
        top_tipos=top_tipos,
        top_operaciones=top_operaciones,
        top_dias=top_dias,
        top_fuentes=top_fuentes,
        df_post_login=df_post_login,
    )

    imprimir_reporte(
        ruta_archivo=ruta_archivo,
        resumen=resumen,
        top_tipos=top_tipos,
        top_operaciones=top_operaciones,
        top_dias=top_dias,
        top_fuentes=top_fuentes,
        ruta_excel=ruta_excel,
    )


if __name__ == "__main__":
    main()
