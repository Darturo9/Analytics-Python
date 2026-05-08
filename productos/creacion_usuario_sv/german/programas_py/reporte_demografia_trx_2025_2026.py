"""
Resumen de genero y generacion de clientes que realizaron trx,
por cohorte de creacion de usuario (2025 y 2026).

Cohorte 2025: usuarios creados en 2025. Trx del ano 2025.
Cohorte 2026: usuarios creados en 2026. Trx 2026-01-01 a 2026-04-30 (excluye mayo).

Salida: consola + Excel con resumen agregado (sin detalle por cliente).

Uso:
    python3 productos/creacion_usuario_sv/german/programas_py/reporte_demografia_trx_2025_2026.py
"""

from pathlib import Path
import sys
import urllib

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.config import DB_SERVER, DB_USER, DB_PASS, DB_DRIVER

BASE_DIR = Path(__file__).resolve().parents[1]
DB_NAME = "DWHSV"
EXPORTS_DIR = BASE_DIR / "exports"

GEN_ORDER = [
    "Generacion silenciosa (<=1945)",
    "Baby Boomers (1946-1964)",
    "Generation X (1965-1980)",
    "Gen Y - Millennials (1981-1996)",
    "Generacion Z (1997-2012)",
    "Generacion Alpha (2013+)",
    "SIN DATO",
]


def get_engine():
    conn_params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={conn_params}", fast_executemany=True)


def run_query_file(path: Path) -> pd.DataFrame:
    sql = path.read_text(encoding="utf-8")
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn)


def normalizar_codigo_cliente(valor) -> str:
    if pd.isna(valor):
        return ""
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    return solo_digitos[-8:].zfill(8) if solo_digitos else ""


def normalizar_genero(valor) -> str:
    if pd.isna(valor):
        return "SIN DATO"
    g = str(valor).strip().upper()
    if g in {"M", "H", "MALE", "MASCULINO", "HOMBRE", "1"}:
        return "MASCULINO"
    if g in {"F", "FEMALE", "FEMENINO", "MUJER", "2"}:
        return "FEMENINO"
    return "SIN DATO"


def clasificar_generacion(fecha_nac) -> str:
    if pd.isna(fecha_nac):
        return "SIN DATO"
    anio = int(fecha_nac.year)
    if anio <= 1945:
        return "Generacion silenciosa (<=1945)"
    if 1946 <= anio <= 1964:
        return "Baby Boomers (1946-1964)"
    if 1965 <= anio <= 1980:
        return "Generation X (1965-1980)"
    if 1981 <= anio <= 1996:
        return "Gen Y - Millennials (1981-1996)"
    if 1997 <= anio <= 2012:
        return "Generacion Z (1997-2012)"
    return "Generacion Alpha (2013+)"


def cargar_bases() -> tuple[pd.DataFrame, pd.DataFrame]:
    print(f"Cargando datos desde {DB_NAME}...")
    usuarios = run_query_file(BASE_DIR / "queries" / "base_usuarios_2025_2026.sql")
    trx = run_query_file(BASE_DIR / "queries" / "trx_usuarios_2025_2026.sql")
    print(f"  Usuarios:    {len(usuarios):,}")
    print(f"  Journal trx: {len(trx):,}")
    return usuarios, trx


def preparar_cohorte(df: pd.DataFrame) -> pd.DataFrame:
    data = df.copy()
    data["fecha_creacion_usuario"] = pd.to_datetime(data["fecha_creacion_usuario"], errors="coerce")
    data["fecha_nacimiento_usuario"] = pd.to_datetime(data["fecha_nacimiento_usuario"], errors="coerce")
    data = data[data["fecha_creacion_usuario"].notna()].copy()
    data["anio_creacion"] = data["fecha_creacion_usuario"].dt.year
    data = data[data["anio_creacion"].isin([2025, 2026])].copy()

    data["id_usuario"] = data["nombre_usuario"].astype("string").str.strip()
    data.loc[data["id_usuario"] == "", "id_usuario"] = pd.NA
    data = data[data["id_usuario"].notna()].copy()

    data["codigo_cliente"] = data["codigo_cliente_usuario_creado"].apply(normalizar_codigo_cliente)
    data = data[data["codigo_cliente"] != ""].copy()

    data = (
        data.sort_values(["id_usuario", "fecha_creacion_usuario"])
        .drop_duplicates(subset=["id_usuario"], keep="first")
        .copy()
    )

    data["genero"] = data["genero_cliente"].apply(normalizar_genero)
    data["generacion"] = data["fecha_nacimiento_usuario"].apply(clasificar_generacion)

    return data[["id_usuario", "codigo_cliente", "anio_creacion", "genero", "generacion"]].copy()


def preparar_trx(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["fecha_transaccion"] = pd.to_datetime(out["fecha_transaccion"], errors="coerce")
    out = out[out["fecha_transaccion"].notna()].copy()
    out["codigo_cliente"] = out["codigo_cliente_transaccion"].apply(normalizar_codigo_cliente)
    out = out[out["codigo_cliente"] != ""].copy()
    return out[["fecha_transaccion", "codigo_cliente"]].copy()


def resumen_cohorte(cohort: pd.DataFrame, trx: pd.DataFrame, anio: int) -> dict[str, pd.DataFrame]:
    cohort_anio = cohort[cohort["anio_creacion"] == anio].copy()

    if anio == 2026:
        trx_anio = trx[
            (trx["fecha_transaccion"].dt.year == 2026)
            & (trx["fecha_transaccion"] < pd.Timestamp("2026-05-01"))
        ].copy()
    else:
        trx_anio = trx[trx["fecha_transaccion"].dt.year == anio].copy()

    # Clientes del cohorte que tuvieron al menos 1 trx
    clientes_con_trx = set(trx_anio["codigo_cliente"].unique())
    cohort_con_trx = cohort_anio[cohort_anio["codigo_cliente"].isin(clientes_con_trx)].copy()

    total_usuarios = cohort_anio["id_usuario"].nunique()
    total_con_trx = cohort_con_trx["id_usuario"].nunique()

    print(f"\nCohorte {anio}: {total_usuarios:,} usuarios | {total_con_trx:,} con trx ({total_con_trx/total_usuarios*100:.1f}%)" if total_usuarios else f"\nCohorte {anio}: sin datos")

    def agg_por(col: str, orden: list[str] | None = None) -> pd.DataFrame:
        g = (
            cohort_con_trx.groupby(col, as_index=False)
            .agg(clientes_unicos=("id_usuario", "nunique"))
            .assign(pct=lambda d: (d["clientes_unicos"] / total_con_trx * 100).round(2))
            .sort_values("clientes_unicos", ascending=False)
            .reset_index(drop=True)
        )
        if orden:
            g[col] = pd.Categorical(g[col], categories=orden, ordered=True)
            g = g.sort_values(col).reset_index(drop=True)
        g.columns = [col, "clientes_unicos", "pct"]
        return g

    resumen_global = pd.DataFrame([{
        "anio_cohorte": anio,
        "total_usuarios_cohorte": total_usuarios,
        "usuarios_con_trx": total_con_trx,
        "cobertura_pct": round(total_con_trx / total_usuarios * 100, 2) if total_usuarios else 0,
    }])

    return {
        "global": resumen_global,
        "genero": agg_por("genero"),
        "generacion": agg_por("generacion", orden=GEN_ORDER),
        "genero_x_generacion": (
            cohort_con_trx.groupby(["generacion", "genero"], as_index=False)
            .agg(clientes_unicos=("id_usuario", "nunique"))
            .assign(pct=lambda d: (d["clientes_unicos"] / total_con_trx * 100).round(2))
            .sort_values("clientes_unicos", ascending=False)
            .reset_index(drop=True)
        ),
    }


def imprimir_resumen(label: str, datos: dict[str, pd.DataFrame]) -> None:
    print(f"\n{'='*70}\n{label}\n{'='*70}")
    print("\n[Global]")
    print(datos["global"].to_string(index=False))
    print("\n[Por Genero]")
    print(datos["genero"].to_string(index=False))
    print("\n[Por Generacion]")
    print(datos["generacion"].to_string(index=False))
    print("\n[Generacion x Genero]")
    print(datos["genero_x_generacion"].to_string(index=False))


def main() -> None:
    usuarios_df, trx_df = cargar_bases()
    cohort = preparar_cohorte(usuarios_df)
    trx = preparar_trx(trx_df)

    datos_2025 = resumen_cohorte(cohort, trx, 2025)
    datos_2026 = resumen_cohorte(cohort, trx, 2026)

    imprimir_resumen("COHORTE 2025 - Genero y Generacion de clientes con trx", datos_2025)
    imprimir_resumen("COHORTE 2026 (ene-abr) - Genero y Generacion de clientes con trx", datos_2026)

    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out = EXPORTS_DIR / "demografia_trx_2025_2026.xlsx"
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        datos_2025["global"].to_excel(writer, sheet_name="2025_global", index=False)
        datos_2025["genero"].to_excel(writer, sheet_name="2025_genero", index=False)
        datos_2025["generacion"].to_excel(writer, sheet_name="2025_generacion", index=False)
        datos_2025["genero_x_generacion"].to_excel(writer, sheet_name="2025_genero_x_gen", index=False)
        datos_2026["global"].to_excel(writer, sheet_name="2026_global", index=False)
        datos_2026["genero"].to_excel(writer, sheet_name="2026_genero", index=False)
        datos_2026["generacion"].to_excel(writer, sheet_name="2026_generacion", index=False)
        datos_2026["genero_x_generacion"].to_excel(writer, sheet_name="2026_genero_x_gen", index=False)

    print(f"\nExcel generado: {out}")


if __name__ == "__main__":
    try:
        main()
    except SQLAlchemyError as exc:
        msg = " ".join(str(exc).split())
        print(f"[ERROR] DB {DB_NAME}: {msg}")
        sys.exit(1)
