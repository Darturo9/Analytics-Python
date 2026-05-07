"""
Reporte en consola de transacciones para usuarios creados en 2025-2026.

Modos:
    - anual: metricas generales y por anio
    - mensual: resumen por mes y tipo de transaccion

Uso:
    python3 productos/creacion_usuario_sv/german/programas_py/reporte_trx_usuarios_2025_2026.py --modo anual
    python3 productos/creacion_usuario_sv/german/programas_py/reporte_trx_usuarios_2025_2026.py --modo mensual
"""

from pathlib import Path
import argparse
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
DB_NAME_DASHBOARD = "DWHSV"
EXPORTS_DIR = BASE_DIR / "exports"


def run_query_hsv(sql: str, params: dict | None = None) -> pd.DataFrame:
    conn_params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME_DASHBOARD};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "TrustServerCertificate=yes;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_params}", fast_executemany=True)
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def run_query_file_hsv(path: Path, params: dict | None = None) -> pd.DataFrame:
    sql = path.read_text(encoding="utf-8")
    return run_query_hsv(sql, params)


def normalizar_codigo_cliente(valor) -> str:
    if pd.isna(valor):
        return ""
    solo_digitos = "".join(c for c in str(valor).strip() if c.isdigit())
    return solo_digitos[-8:].zfill(8) if solo_digitos else ""


def cargar_bases() -> tuple[pd.DataFrame, pd.DataFrame]:
    print(f"Cargando bases en {DB_NAME_DASHBOARD}...")
    usuarios = run_query_file_hsv(BASE_DIR / "queries" / "base_usuarios_2025_2026.sql")
    trx = run_query_file_hsv(BASE_DIR / "queries" / "trx_usuarios_2025_2026.sql")
    print(f"  Usuarios base: {len(usuarios):,} filas")
    print(f"  Transacciones: {len(trx):,} filas")
    return usuarios, trx


def preparar_cohorte_usuarios(df_usuarios: pd.DataFrame) -> pd.DataFrame:
    data = df_usuarios.copy()
    data["fecha_creacion_usuario"] = pd.to_datetime(data["fecha_creacion_usuario"], errors="coerce")
    data = data[data["fecha_creacion_usuario"].notna()].copy()
    data["anio_creacion"] = data["fecha_creacion_usuario"].dt.year
    data = data[data["anio_creacion"].isin([2025, 2026])].copy()

    data["id_usuario"] = data["nombre_usuario"].astype("string").str.strip()
    data.loc[data["nombre_usuario"].isna(), "id_usuario"] = pd.NA
    data.loc[data["id_usuario"] == "", "id_usuario"] = pd.NA
    data = data[data["id_usuario"].notna()].copy()

    data["codigo_cliente"] = data["codigo_cliente_usuario_creado"].apply(normalizar_codigo_cliente)
    data = data[data["codigo_cliente"] != ""].copy()

    # Cohorte RECDIST(nombre_usuario), conservando el primer registro cronologico por usuario.
    data = (
        data.sort_values(["id_usuario", "fecha_creacion_usuario"])
        .drop_duplicates(subset=["id_usuario"], keep="first")
        .copy()
    )

    # Para el cruce con trx, basta id_usuario + codigo_cliente.
    data = data[["id_usuario", "codigo_cliente", "fecha_creacion_usuario", "anio_creacion"]].copy()
    return data


def preparar_trx(df_trx: pd.DataFrame) -> pd.DataFrame:
    trx = df_trx.copy()
    trx["fecha_transaccion"] = pd.to_datetime(trx["fecha_transaccion"], errors="coerce")
    trx = trx[trx["fecha_transaccion"].notna()].copy()
    trx["codigo_cliente"] = trx["codigo_cliente_transaccion"].apply(normalizar_codigo_cliente)
    trx = trx[trx["codigo_cliente"] != ""].copy()
    trx["anio_trx"] = trx["fecha_transaccion"].dt.year
    trx["mes_trx"] = trx["fecha_transaccion"].dt.month
    trx["periodo"] = trx["fecha_transaccion"].dt.strftime("%Y-%m")
    trx["valor"] = pd.to_numeric(trx["valor"], errors="coerce")
    return trx


def construir_dataset(df_usuarios: pd.DataFrame, df_trx: pd.DataFrame) -> pd.DataFrame:
    cohort = preparar_cohorte_usuarios(df_usuarios)
    trx = preparar_trx(df_trx)

    df = cohort.merge(
        trx,
        how="left",
        on="codigo_cliente",
        suffixes=("_usr", "_trx"),
    )

    # Mantener solo filas con transaccion para analisis de trx.
    df = df[df["fecha_transaccion"].notna()].copy()
    return df


def imprimir_resumen_anual_y_exportar(df: pd.DataFrame, cohort_size: int) -> None:
    print("=" * 84)
    print("TRX DE CLIENTES CREADOS EN 2025-2026 (PRODUCTO GENERAL, SIN CAMPANIAS)")
    print("Periodo trx: 2025-01-01 a 2026-12-31")
    print("=" * 84)

    clientes_con_trx = int(df["id_usuario"].nunique())
    total_trx = int(len(df))
    monto_disponible = df["valor"].notna().any()
    monto_total = float(df["valor"].sum()) if monto_disponible else 0.0
    monto_prom = float(df["valor"].mean()) if (monto_disponible and total_trx) else 0.0
    cobertura = (clientes_con_trx / cohort_size * 100) if cohort_size else 0.0

    print(f"Cohorte usuarios creados (2025-2026): {cohort_size:,}")
    print(f"Clientes de cohorte con >=1 trx:      {clientes_con_trx:,} ({cobertura:5.2f}%)")
    print(f"Total transacciones:                  {total_trx:,}")
    if monto_disponible:
        print(f"Monto total:                          {monto_total:,.2f}")
        print(f"Monto promedio por trx:               {monto_prom:,.2f}")

    if monto_disponible:
        anio = (
            df.groupby("anio_trx", as_index=False)
            .agg(
                clientes_unicos=("id_usuario", "nunique"),
                total_trx=("id_usuario", "size"),
                monto_total=("valor", "sum"),
                monto_promedio=("valor", "mean"),
            )
            .sort_values("anio_trx")
        )
    else:
        anio = (
            df.groupby("anio_trx", as_index=False)
            .agg(
                clientes_unicos=("id_usuario", "nunique"),
                total_trx=("id_usuario", "size"),
            )
            .sort_values("anio_trx")
        )

    print("\n--- Resumen por anio de transaccion ---")
    if anio.empty:
        print("Sin datos")
        return
    for _, row in anio.iterrows():
        if monto_disponible:
            print(
                f"{int(row['anio_trx'])}: "
                f"clientes={int(row['clientes_unicos']):,} | "
                f"trx={int(row['total_trx']):,} | "
                f"monto_total={float(row['monto_total']):,.2f} | "
                f"monto_prom={float(row['monto_promedio']):,.2f}"
            )
        else:
            print(
                f"{int(row['anio_trx'])}: "
                f"clientes={int(row['clientes_unicos']):,} | "
                f"trx={int(row['total_trx']):,}"
            )

    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_anual = EXPORTS_DIR / "resumen_anual_transacciones_2025_2026.csv"
    anio.to_csv(out_anual, index=False, encoding="utf-8-sig")
    print(f"\nCSV generado: {out_anual}")

    out_global = EXPORTS_DIR / "resumen_global_transacciones_2025_2026.csv"
    if monto_disponible:
        global_df = pd.DataFrame(
            [
                {
                    "cohorte_usuarios_2025_2026": cohort_size,
                    "clientes_con_trx": clientes_con_trx,
                    "cobertura_clientes_pct": cobertura,
                    "total_trx": total_trx,
                    "monto_total": monto_total,
                    "monto_promedio_trx": monto_prom,
                }
            ]
        )
    else:
        global_df = pd.DataFrame(
            [
                {
                    "cohorte_usuarios_2025_2026": cohort_size,
                    "clientes_con_trx": clientes_con_trx,
                    "cobertura_clientes_pct": cobertura,
                    "total_trx": total_trx,
                }
            ]
        )
    global_df.to_csv(out_global, index=False, encoding="utf-8-sig")
    print(f"CSV generado: {out_global}")


def imprimir_y_exportar_resumen_mensual_por_transaccion(df: pd.DataFrame) -> None:
    print("=" * 96)
    print("RESUMEN MENSUAL POR TRANSACCION (CLIENTES CREADOS EN 2025-2026)")
    print("=" * 96)

    if df.empty:
        print("Sin transacciones para mostrar.")
        return

    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    monto_disponible = df["valor"].notna().any()

    trabajo = df.copy()
    trabajo["descripcion_transaccion"] = (
        trabajo["descripcion_transaccion"]
        .fillna("SIN_DESCRIPCION")
        .astype(str)
        .str.strip()
        .replace("", "SIN_DESCRIPCION")
    )
    trabajo["secode"] = (
        trabajo["secode"]
        .fillna("SIN_SECODE")
        .astype(str)
        .str.strip()
        .replace("", "SIN_SECODE")
    )
    trabajo["codigo_transaccion"] = (
        trabajo["codigo_transaccion"]
        .fillna("SIN_CODIGO")
        .astype(str)
        .str.strip()
        .replace("", "SIN_CODIGO")
    )
    trabajo["transaccion"] = (
        trabajo["descripcion_transaccion"] + " | SECODE=" + trabajo["secode"] + " | COD=" + trabajo["codigo_transaccion"]
    )

    if monto_disponible:
        resumen = (
            trabajo.groupby(
                ["anio_trx", "mes_trx", "periodo", "transaccion", "canal"],
                as_index=False,
            )
            .agg(
                clientes_unicos=("id_usuario", "nunique"),
                total_trx=("id_usuario", "size"),
                monto_total=("valor", "sum"),
                monto_promedio=("valor", "mean"),
            )
            .sort_values(
                ["anio_trx", "mes_trx", "clientes_unicos", "total_trx", "transaccion"],
                ascending=[True, True, False, False, True],
            )
        )

        totales_mes = (
            trabajo.groupby(["anio_trx", "mes_trx"], as_index=False)
            .agg(
                clientes_unicos_mes=("id_usuario", "nunique"),
                total_trx_mes=("id_usuario", "size"),
                monto_total_mes=("valor", "sum"),
            )
        )
    else:
        resumen = (
            trabajo.groupby(
                ["anio_trx", "mes_trx", "periodo", "transaccion", "canal"],
                as_index=False,
            )
            .agg(
                clientes_unicos=("id_usuario", "nunique"),
                total_trx=("id_usuario", "size"),
            )
            .sort_values(
                ["anio_trx", "mes_trx", "clientes_unicos", "total_trx", "transaccion"],
                ascending=[True, True, False, False, True],
            )
        )

        totales_mes = (
            trabajo.groupby(["anio_trx", "mes_trx"], as_index=False)
            .agg(
                clientes_unicos_mes=("id_usuario", "nunique"),
                total_trx_mes=("id_usuario", "size"),
            )
        )

    for anio in sorted(resumen["anio_trx"].unique().tolist()):
        print(f"\n{'=' * 96}\nANIO {int(anio)}\n{'=' * 96}")
        sub_anio = resumen[resumen["anio_trx"] == anio].copy()
        for mes in sorted(sub_anio["mes_trx"].unique().tolist()):
            sub_mes = sub_anio[sub_anio["mes_trx"] == mes].copy()
            periodo = sub_mes["periodo"].iloc[0]
            tot_mes = totales_mes[(totales_mes["anio_trx"] == anio) & (totales_mes["mes_trx"] == mes)].iloc[0]
            if monto_disponible:
                print(
                    f"\n[{periodo}] transacciones distintas: {len(sub_mes):,} | "
                    f"clientes_unicos_mes={int(tot_mes['clientes_unicos_mes']):,} | "
                    f"trx_mes={int(tot_mes['total_trx_mes']):,} | monto_total_mes={float(tot_mes['monto_total_mes']):,.2f}"
                )
            else:
                print(
                    f"\n[{periodo}] transacciones distintas: {len(sub_mes):,} | "
                    f"clientes_unicos_mes={int(tot_mes['clientes_unicos_mes']):,} | "
                    f"trx_mes={int(tot_mes['total_trx_mes']):,}"
                )

            for _, row in sub_mes.iterrows():
                if monto_disponible:
                    print(
                        f"- {row['transaccion']} | canal={row['canal']} | "
                        f"clientes={int(row['clientes_unicos']):,} | "
                        f"trx={int(row['total_trx']):,} | "
                        f"monto_total={float(row['monto_total']):,.2f} | "
                        f"monto_prom={float(row['monto_promedio']):,.2f}"
                    )
                else:
                    print(
                        f"- {row['transaccion']} | canal={row['canal']} | "
                        f"clientes={int(row['clientes_unicos']):,} | "
                        f"trx={int(row['total_trx']):,}"
                    )

    out_file = EXPORTS_DIR / "resumen_mensual_transacciones_2025_2026.csv"
    resumen.to_csv(out_file, index=False, encoding="utf-8-sig")
    print(f"\nCSV generado: {out_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Reporte de trx para usuarios creados en 2025-2026.")
    parser.add_argument(
        "--modo",
        type=str,
        default="anual",
        choices=["anual", "mensual", "resumen"],
        help="Modo de salida: anual o mensual por transaccion.",
    )
    args = parser.parse_args()

    usuarios_df, trx_df = cargar_bases()
    cohort = preparar_cohorte_usuarios(usuarios_df)
    cohort_size = int(cohort["id_usuario"].nunique())
    dataset = construir_dataset(usuarios_df, trx_df)

    if args.modo in {"anual", "resumen"}:
        imprimir_resumen_anual_y_exportar(dataset, cohort_size)
        return

    imprimir_y_exportar_resumen_mensual_por_transaccion(dataset)


if __name__ == "__main__":
    try:
        main()
    except SQLAlchemyError as exc:
        msg = " ".join(str(exc).split())
        print(f"[ERROR] No se pudo conectar/consultar en {DB_NAME_DASHBOARD}: {msg}")
        sys.exit(1)
