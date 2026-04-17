"""
reporte_post_login_clientes_arbol_abril01_17_2026.py
----------------------------------------------------
Reporte en consola de actividad post-login para clientes de ClientesArbol
(columna "Clientes") en la ventana 2026-04-01 al 2026-04-17.

Muestra:
- clientes base
- clientes con login
- clientes con actividad post-login
- total de eventos post-login
- top 5 tipos de accion post-login
- top 5 operaciones post-login
- top 5 dias con mas eventos post-login

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

FECHA_INICIO = pd.Timestamp("2026-04-01")
FECHA_FIN_EXCLUSIVA = pd.Timestamp("2026-04-18")
PLACEHOLDER_CLIENTES_VALUES = "{{CLIENTES_BASE_VALUES}}"
TAM_CHUNK_CLIENTES_SQL = 500


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
        raise ValueError("No hay códigos de cliente válidos para consultar eventos post-login.")
    return ",\n            ".join(f"('{c.strip()}')" for c in codigos_validos)


def cargar_eventos_operaciones_clientes(codigos_base: list[str]) -> pd.DataFrame:
    if not codigos_base:
        return pd.DataFrame(columns=["fecha_evento", "padded_codigo_usuario", "secode", "operacion"])

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
        df_chunk.columns = [str(c).strip().lower() for c in df_chunk.columns]
        frames.append(df_chunk)

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        return pd.DataFrame(columns=["fecha_evento", "padded_codigo_usuario", "secode", "operacion"])

    if "padded_codigo_usuario" not in df.columns:
        if "codigo_usuario" in df.columns:
            df["padded_codigo_usuario"] = df["codigo_usuario"].apply(normalizar_codigo)
        else:
            raise ValueError("La query de post-login debe devolver padded_codigo_usuario o codigo_usuario.")
    if "fecha_evento" not in df.columns:
        raise ValueError("La query de post-login debe devolver la columna fecha_evento.")
    if "secode" not in df.columns:
        df["secode"] = ""
    if "operacion" not in df.columns:
        df["operacion"] = ""

    df["padded_codigo_usuario"] = df["padded_codigo_usuario"].apply(normalizar_codigo)
    df["fecha_evento"] = pd.to_datetime(df["fecha_evento"], errors="coerce")
    df["secode"] = df["secode"].fillna("").astype(str).str.strip()
    df["operacion"] = df["operacion"].fillna("").astype(str).str.strip()

    df = df[
        df["padded_codigo_usuario"].notna()
        & df["fecha_evento"].notna()
        & (df["secode"] != "")
    ].copy()
    df = df[
        (df["fecha_evento"] >= FECHA_INICIO)
        & (df["fecha_evento"] < FECHA_FIN_EXCLUSIVA)
    ].copy()
    return df


def clasificar_actividad_post_login(secode: str, operacion: str) -> str:
    texto = f"{secode} {operacion}".lower()
    if any(x in texto for x in ["edocta", "estado de cuenta", "con-sal", "saldo", "cns", "consulta"]):
        return "Consultas / Estado de cuenta"
    if any(x in texto for x in ["pag", "pago", "mpg", "multipago", "tcpago", "cpago"]):
        return "Pagos"
    if any(x in texto for x in ["ach", "transf", "transfer", "traint", "transh", "trach"]):
        return "Transferencias"
    if any(x in texto for x in ["ptm", "prestamo", "ptr-", "pym-"]):
        return "Prestamos"
    return "Otras operaciones"


def construir_eventos_post_login(
    df_logins_filtrado: pd.DataFrame,
    df_eventos_operaciones: pd.DataFrame,
) -> pd.DataFrame:
    if df_logins_filtrado.empty or df_eventos_operaciones.empty:
        return pd.DataFrame(columns=["padded_codigo_usuario", "fecha_evento", "secode", "operacion", "primer_login"])

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

    df_post["tipo_actividad"] = df_post.apply(
        lambda r: clasificar_actividad_post_login(r.get("secode", ""), r.get("operacion", "")),
        axis=1,
    )
    return df_post


def imprimir_reporte(
    ruta_archivo: Path,
    total_clientes_base: int,
    clientes_unicos_login: int,
    df_post_login: pd.DataFrame,
) -> None:
    print("=" * 108)
    print("REPORTE POST-LOGIN CLIENTES ARBOL - ABRIL 2026 (2026-04-01 al 2026-04-17)")
    print("=" * 108)
    print(f"Archivo base: {ruta_archivo}")
    print(f"Clientes en Excel: {total_clientes_base:,}")
    print(f"Clientes unicos con login: {clientes_unicos_login:,}")

    if df_post_login.empty:
        print("No se encontraron eventos post-login para el periodo.")
        print("=" * 108)
        return

    total_eventos_post = int(len(df_post_login))
    clientes_post = int(df_post_login["padded_codigo_usuario"].nunique())
    pct_clientes_post = (
        (clientes_post * 100.0 / clientes_unicos_login)
        if clientes_unicos_login > 0
        else 0.0
    )

    print(f"Clientes unicos con actividad post-login: {clientes_post:,}")
    print(f"% sobre clientes con login: {pct_clientes_post:,.2f}%")
    print(f"Total eventos post-login: {total_eventos_post:,}")
    print("-" * 108)

    top_tipos = (
        df_post_login.groupby("tipo_actividad", as_index=False)
        .agg(
            eventos=("tipo_actividad", "size"),
            clientes_unicos=("padded_codigo_usuario", "nunique"),
        )
        .sort_values(["eventos", "tipo_actividad"], ascending=[False, True])
        .head(5)
        .reset_index(drop=True)
    )
    print("TOP 5 TIPOS DE ACCION POST-LOGIN")
    print(top_tipos.to_string(index=False))
    print("-" * 108)

    top_operaciones = (
        df_post_login.groupby(["operacion", "secode"], as_index=False)
        .agg(
            eventos=("operacion", "size"),
            clientes_unicos=("padded_codigo_usuario", "nunique"),
        )
        .sort_values(["eventos", "operacion"], ascending=[False, True])
        .head(5)
        .reset_index(drop=True)
    )
    print("TOP 5 OPERACIONES POST-LOGIN")
    print(top_operaciones.to_string(index=False))
    print("-" * 108)

    top_dias = (
        df_post_login.assign(dia=df_post_login["fecha_evento"].dt.normalize())
        .groupby("dia", as_index=False)
        .agg(
            eventos=("dia", "size"),
            clientes_unicos=("padded_codigo_usuario", "nunique"),
        )
        .sort_values(["eventos", "dia"], ascending=[False, True])
        .head(5)
        .reset_index(drop=True)
    )
    top_dias["dia"] = top_dias["dia"].dt.strftime("%Y-%m-%d")
    print("TOP 5 DIAS CON MAS EVENTOS POST-LOGIN")
    print(top_dias.to_string(index=False))
    print("=" * 108)


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

    imprimir_reporte(
        ruta_archivo=ruta_archivo,
        total_clientes_base=int(df_base["padded_codigo_cliente"].nunique()),
        clientes_unicos_login=clientes_unicos_login,
        df_post_login=df_post_login,
    )


if __name__ == "__main__":
    main()
