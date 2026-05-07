"""
Reporte en consola de transacciones y logins para usuarios creados en 2025-2026.

Modos:
    - anual: metricas generales y por anio (DEFAULT)
    - mensual: resumen por mes y tipo de evento

Uso:
    python3 productos/creacion_usuario_sv/german/programas_py/reporte_trx_usuarios_2025_2026.py
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


def cargar_bases() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    print(f"Cargando bases en {DB_NAME_DASHBOARD}...")
    usuarios = run_query_file_hsv(BASE_DIR / "queries" / "base_usuarios_2025_2026.sql")
    trx = run_query_file_hsv(BASE_DIR / "queries" / "trx_usuarios_2025_2026.sql")
    logins = run_query_file_hsv(BASE_DIR / "queries" / "logins_usuarios_2025_2026.sql")
    print(f"  Usuarios base: {len(usuarios):,} filas")
    print(f"  Transacciones: {len(trx):,} filas")
    print(f"  Logins:        {len(logins):,} filas")
    return usuarios, trx, logins


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

    # Cohorte RECDIST(nombre_usuario), conservando el primer registro por usuario.
    data = (
        data.sort_values(["id_usuario", "fecha_creacion_usuario"])
        .drop_duplicates(subset=["id_usuario"], keep="first")
        .copy()
    )

    return data[["id_usuario", "codigo_cliente", "fecha_creacion_usuario", "anio_creacion"]].copy()


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


def preparar_logins(df_logins: pd.DataFrame) -> pd.DataFrame:
    logins = df_logins.copy()
    logins["codigo_cliente"] = logins["codigo_cliente_login"].apply(normalizar_codigo_cliente)
    logins = logins[logins["codigo_cliente"] != ""].copy()

    logins["anio_login"] = pd.to_numeric(logins["anio_login"], errors="coerce")
    logins["mes_login"] = pd.to_numeric(logins["mes_login"], errors="coerce")
    logins = logins[logins["anio_login"].notna() & logins["mes_login"].notna()].copy()

    logins["anio_login"] = logins["anio_login"].astype(int)
    logins["mes_login"] = logins["mes_login"].astype(int)
    logins = logins[logins["anio_login"].isin([2025, 2026]) & logins["mes_login"].between(1, 12)].copy()

    logins["periodo"] = logins["anio_login"].astype(str) + "-" + logins["mes_login"].astype(str).str.zfill(2)
    logins["tipo_login"] = (
        logins["tipo_login"]
        .fillna("app-login")
        .astype(str)
        .str.strip()
        .replace("", "app-login")
    )
    return logins


def construir_datasets(
    df_usuarios: pd.DataFrame, df_trx: pd.DataFrame, df_logins: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    cohort = preparar_cohorte_usuarios(df_usuarios)
    trx = preparar_trx(df_trx)
    logins = preparar_logins(df_logins)

    df_trx_cohorte = cohort.merge(trx, how="left", on="codigo_cliente", suffixes=("_usr", "_trx"))
    df_trx_cohorte = df_trx_cohorte[df_trx_cohorte["fecha_transaccion"].notna()].copy()

    df_login_cohorte = cohort.merge(logins, how="left", on="codigo_cliente", suffixes=("_usr", "_login"))
    df_login_cohorte = df_login_cohorte[df_login_cohorte["periodo"].notna()].copy()

    return cohort, df_trx_cohorte, df_login_cohorte


def imprimir_resumen_anual_y_exportar(df_trx: pd.DataFrame, df_login: pd.DataFrame, cohort_size: int) -> None:
    print("=" * 90)
    print("REPORTE ANUAL - TRX Y LOGINS (COHORTE CREADA 2025-2026)")
    print("=" * 90)

    # --- TRX ---
    print("\n--- TRX (general) ---")
    if df_trx.empty:
        print("Sin transacciones para la cohorte")
        anio_trx = pd.DataFrame(columns=["anio_trx", "clientes_unicos", "total_trx", "monto_total", "monto_promedio"])
        global_trx_df = pd.DataFrame(
            [
                {
                    "cohorte_usuarios_2025_2026": cohort_size,
                    "clientes_con_trx": 0,
                    "cobertura_clientes_pct": 0.0,
                    "total_trx": 0,
                    "monto_total": 0.0,
                    "monto_promedio_trx": 0.0,
                }
            ]
        )
    else:
        clientes_con_trx = int(df_trx["id_usuario"].nunique())
        total_trx = int(len(df_trx))
        monto_disponible = df_trx["valor"].notna().any()
        monto_total = float(df_trx["valor"].sum()) if monto_disponible else 0.0
        monto_prom = float(df_trx["valor"].mean()) if monto_disponible else 0.0
        cobertura = (clientes_con_trx / cohort_size * 100) if cohort_size else 0.0

        print(f"Cohorte usuarios creados:            {cohort_size:,}")
        print(f"Clientes con >=1 trx:                {clientes_con_trx:,} ({cobertura:5.2f}%)")
        print(f"Total trx:                           {total_trx:,}")
        if monto_disponible:
            print(f"Monto total:                         {monto_total:,.2f}")
            print(f"Monto promedio por trx:              {monto_prom:,.2f}")

        if monto_disponible:
            anio_trx = (
                df_trx.groupby("anio_trx", as_index=False)
                .agg(
                    clientes_unicos=("id_usuario", "nunique"),
                    total_trx=("id_usuario", "size"),
                    monto_total=("valor", "sum"),
                    monto_promedio=("valor", "mean"),
                )
                .sort_values(["total_trx", "clientes_unicos"], ascending=[False, False])
            )
            global_trx_df = pd.DataFrame(
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
            anio_trx = (
                df_trx.groupby("anio_trx", as_index=False)
                .agg(
                    clientes_unicos=("id_usuario", "nunique"),
                    total_trx=("id_usuario", "size"),
                )
                .sort_values(["total_trx", "clientes_unicos"], ascending=[False, False])
            )
            global_trx_df = pd.DataFrame(
                [
                    {
                        "cohorte_usuarios_2025_2026": cohort_size,
                        "clientes_con_trx": clientes_con_trx,
                        "cobertura_clientes_pct": cobertura,
                        "total_trx": total_trx,
                    }
                ]
            )

        print("\nResumen anual trx:")
        for _, row in anio_trx.iterrows():
            if "monto_total" in anio_trx.columns:
                print(
                    f"{int(row['anio_trx'])}: clientes={int(row['clientes_unicos']):,} | "
                    f"trx={int(row['total_trx']):,} | "
                    f"monto_total={float(row['monto_total']):,.2f} | "
                    f"monto_prom={float(row['monto_promedio']):,.2f}"
                )
            else:
                print(
                    f"{int(row['anio_trx'])}: clientes={int(row['clientes_unicos']):,} | "
                    f"trx={int(row['total_trx']):,}"
                )

    # --- LOGINS ---
    print("\n--- LOGINS (general) ---")
    if df_login.empty:
        print("Sin logins para la cohorte")
        anio_login = pd.DataFrame(columns=["anio_login", "clientes_unicos_login", "total_logins"])
        global_login_df = pd.DataFrame(
            [
                {
                    "cohorte_usuarios_2025_2026": cohort_size,
                    "clientes_con_login": 0,
                    "cobertura_clientes_pct": 0.0,
                    "total_logins": 0,
                }
            ]
        )
    else:
        clientes_con_login = int(df_login["id_usuario"].nunique())
        total_logins = int(len(df_login))
        cobertura_login = (clientes_con_login / cohort_size * 100) if cohort_size else 0.0

        print(f"Cohorte usuarios creados:            {cohort_size:,}")
        print(f"Clientes con >=1 login:              {clientes_con_login:,} ({cobertura_login:5.2f}%)")
        print(f"Total logins:                        {total_logins:,}")

        anio_login = (
            df_login.groupby("anio_login", as_index=False)
            .agg(
                clientes_unicos_login=("id_usuario", "nunique"),
                total_logins=("id_usuario", "size"),
            )
            .sort_values(["total_logins", "clientes_unicos_login"], ascending=[False, False])
        )

        print("\nResumen anual logins:")
        for _, row in anio_login.iterrows():
            print(
                f"{int(row['anio_login'])}: clientes={int(row['clientes_unicos_login']):,} | "
                f"logins={int(row['total_logins']):,}"
            )

        global_login_df = pd.DataFrame(
            [
                {
                    "cohorte_usuarios_2025_2026": cohort_size,
                    "clientes_con_login": clientes_con_login,
                    "cobertura_clientes_pct": cobertura_login,
                    "total_logins": total_logins,
                }
            ]
        )

    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_file = EXPORTS_DIR / "resumen_anual_trx_logins_2025_2026.xlsx"
    with pd.ExcelWriter(out_file, engine="xlsxwriter") as writer:
        global_trx_df.to_excel(writer, sheet_name="global_trx", index=False)
        anio_trx.to_excel(writer, sheet_name="anual_trx", index=False)
        global_login_df.to_excel(writer, sheet_name="global_logins", index=False)
        anio_login.to_excel(writer, sheet_name="anual_logins", index=False)

    print(f"\nExcel generado: {out_file}")


def construir_resumen_mensual_trx(df_trx: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, bool]:
    if df_trx.empty:
        resumen = pd.DataFrame(
            columns=[
                "anio_trx",
                "mes_trx",
                "periodo",
                "transaccion",
                "canal",
                "clientes_unicos",
                "total_trx",
                "monto_total",
                "monto_promedio",
            ]
        )
        totales = pd.DataFrame(columns=["anio_trx", "mes_trx", "clientes_unicos_mes", "total_trx_mes", "monto_total_mes"])
        return resumen, totales, False

    trabajo = df_trx.copy()
    trabajo["descripcion_transaccion"] = (
        trabajo["descripcion_transaccion"].fillna("SIN_DESCRIPCION").astype(str).str.strip().replace("", "SIN_DESCRIPCION")
    )
    trabajo["secode"] = trabajo["secode"].fillna("SIN_SECODE").astype(str).str.strip().replace("", "SIN_SECODE")
    trabajo["codigo_transaccion"] = (
        trabajo["codigo_transaccion"].fillna("SIN_CODIGO").astype(str).str.strip().replace("", "SIN_CODIGO")
    )
    trabajo["transaccion"] = (
        trabajo["descripcion_transaccion"] + " | SECODE=" + trabajo["secode"] + " | COD=" + trabajo["codigo_transaccion"]
    )

    monto_disponible = trabajo["valor"].notna().any()

    if monto_disponible:
        resumen = (
            trabajo.groupby(["anio_trx", "mes_trx", "periodo", "transaccion", "canal"], as_index=False)
            .agg(
                clientes_unicos=("id_usuario", "nunique"),
                total_trx=("id_usuario", "size"),
                monto_total=("valor", "sum"),
                monto_promedio=("valor", "mean"),
            )
            .sort_values(["anio_trx", "mes_trx", "total_trx", "clientes_unicos", "transaccion"], ascending=[True, True, False, False, True])
        )

        totales = (
            trabajo.groupby(["anio_trx", "mes_trx"], as_index=False)
            .agg(
                clientes_unicos_mes=("id_usuario", "nunique"),
                total_trx_mes=("id_usuario", "size"),
                monto_total_mes=("valor", "sum"),
            )
            .sort_values(["anio_trx", "mes_trx"], ascending=[True, True])
        )
    else:
        resumen = (
            trabajo.groupby(["anio_trx", "mes_trx", "periodo", "transaccion", "canal"], as_index=False)
            .agg(
                clientes_unicos=("id_usuario", "nunique"),
                total_trx=("id_usuario", "size"),
            )
            .sort_values(["anio_trx", "mes_trx", "total_trx", "clientes_unicos", "transaccion"], ascending=[True, True, False, False, True])
        )

        totales = (
            trabajo.groupby(["anio_trx", "mes_trx"], as_index=False)
            .agg(
                clientes_unicos_mes=("id_usuario", "nunique"),
                total_trx_mes=("id_usuario", "size"),
            )
            .sort_values(["anio_trx", "mes_trx"], ascending=[True, True])
        )

    return resumen, totales, monto_disponible


def construir_resumen_mensual_logins(df_login: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df_login.empty:
        resumen = pd.DataFrame(columns=["anio_login", "mes_login", "periodo", "tipo_login", "clientes_unicos", "total_logins"])
        totales = pd.DataFrame(columns=["anio_login", "mes_login", "clientes_unicos_mes", "total_logins_mes"])
        return resumen, totales

    resumen = (
        df_login.groupby(["anio_login", "mes_login", "periodo", "tipo_login"], as_index=False)
        .agg(
            clientes_unicos=("id_usuario", "nunique"),
            total_logins=("id_usuario", "size"),
        )
        .sort_values(["anio_login", "mes_login", "total_logins", "clientes_unicos", "tipo_login"], ascending=[True, True, False, False, True])
    )

    totales = (
        df_login.groupby(["anio_login", "mes_login"], as_index=False)
        .agg(
            clientes_unicos_mes=("id_usuario", "nunique"),
            total_logins_mes=("id_usuario", "size"),
        )
        .sort_values(["anio_login", "mes_login"], ascending=[True, True])
    )

    return resumen, totales


def imprimir_y_exportar_resumen_mensual(df_trx: pd.DataFrame, df_login: pd.DataFrame) -> None:
    print("=" * 96)
    print("RESUMEN MENSUAL - TRX Y LOGINS (COHORTE CREADA 2025-2026)")
    print("=" * 96)

    resumen_trx, totales_trx, monto_trx_disponible = construir_resumen_mensual_trx(df_trx)
    resumen_logins, totales_logins = construir_resumen_mensual_logins(df_login)

    if resumen_trx.empty and resumen_logins.empty:
        print("Sin eventos para mostrar.")
    else:
        anios = sorted(set(resumen_trx.get("anio_trx", pd.Series(dtype=int)).dropna().astype(int).tolist()) | set(resumen_logins.get("anio_login", pd.Series(dtype=int)).dropna().astype(int).tolist()))

        for anio in anios:
            print(f"\n{'=' * 96}\nANIO {anio}\n{'=' * 96}")

            meses_trx = set(resumen_trx.loc[resumen_trx.get("anio_trx", pd.Series(dtype=int)) == anio, "mes_trx"].dropna().astype(int).tolist()) if not resumen_trx.empty else set()
            meses_login = set(resumen_logins.loc[resumen_logins.get("anio_login", pd.Series(dtype=int)) == anio, "mes_login"].dropna().astype(int).tolist()) if not resumen_logins.empty else set()
            meses = sorted(meses_trx | meses_login)

            for mes in meses:
                sub_trx = resumen_trx[(resumen_trx["anio_trx"] == anio) & (resumen_trx["mes_trx"] == mes)].copy() if not resumen_trx.empty else pd.DataFrame()
                sub_log = resumen_logins[(resumen_logins["anio_login"] == anio) & (resumen_logins["mes_login"] == mes)].copy() if not resumen_logins.empty else pd.DataFrame()

                if not sub_trx.empty:
                    periodo = sub_trx["periodo"].iloc[0]
                elif not sub_log.empty:
                    periodo = sub_log["periodo"].iloc[0]
                else:
                    periodo = f"{anio}-{mes:02d}"

                print(f"\n[{periodo}]")

                if not sub_trx.empty:
                    tot_trx = totales_trx[(totales_trx["anio_trx"] == anio) & (totales_trx["mes_trx"] == mes)].iloc[0]
                    if monto_trx_disponible:
                        print(
                            f"TRX mes: clientes_unicos={int(tot_trx['clientes_unicos_mes']):,} | "
                            f"trx={int(tot_trx['total_trx_mes']):,} | "
                            f"monto_total={float(tot_trx['monto_total_mes']):,.2f}"
                        )
                    else:
                        print(
                            f"TRX mes: clientes_unicos={int(tot_trx['clientes_unicos_mes']):,} | "
                            f"trx={int(tot_trx['total_trx_mes']):,}"
                        )

                    sub_trx = sub_trx.sort_values(["total_trx", "clientes_unicos", "transaccion"], ascending=[False, False, True])
                    for _, row in sub_trx.iterrows():
                        if monto_trx_disponible:
                            print(
                                f"- TRX {row['transaccion']} | canal={row['canal']} | "
                                f"clientes={int(row['clientes_unicos']):,} | trx={int(row['total_trx']):,} | "
                                f"monto_total={float(row['monto_total']):,.2f} | monto_prom={float(row['monto_promedio']):,.2f}"
                            )
                        else:
                            print(
                                f"- TRX {row['transaccion']} | canal={row['canal']} | "
                                f"clientes={int(row['clientes_unicos']):,} | trx={int(row['total_trx']):,}"
                            )

                if not sub_log.empty:
                    tot_log = totales_logins[(totales_logins["anio_login"] == anio) & (totales_logins["mes_login"] == mes)].iloc[0]
                    print(
                        f"LOGINS mes: clientes_unicos={int(tot_log['clientes_unicos_mes']):,} | "
                        f"logins={int(tot_log['total_logins_mes']):,}"
                    )

                    sub_log = sub_log.sort_values(["total_logins", "clientes_unicos", "tipo_login"], ascending=[False, False, True])
                    for _, row in sub_log.iterrows():
                        print(
                            f"- LOGIN tipo={row['tipo_login']} | "
                            f"clientes={int(row['clientes_unicos']):,} | logins={int(row['total_logins']):,}"
                        )

    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_file = EXPORTS_DIR / "resumen_mensual_trx_logins_2025_2026.xlsx"
    with pd.ExcelWriter(out_file, engine="xlsxwriter") as writer:
        resumen_trx.to_excel(writer, sheet_name="mensual_trx", index=False)
        totales_trx.to_excel(writer, sheet_name="totales_trx_mes", index=False)
        resumen_logins.to_excel(writer, sheet_name="mensual_logins", index=False)
        totales_logins.to_excel(writer, sheet_name="totales_login_mes", index=False)

    print(f"\nExcel generado: {out_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Reporte de trx y logins para usuarios creados en 2025-2026.")
    parser.add_argument(
        "--modo",
        type=str,
        default="anual",
        choices=["anual", "mensual", "resumen"],
        help="Modo de salida: anual (default) o mensual.",
    )
    args = parser.parse_args()

    usuarios_df, trx_df, logins_df = cargar_bases()
    cohort, dataset_trx, dataset_logins = construir_datasets(usuarios_df, trx_df, logins_df)
    cohort_size = int(cohort["id_usuario"].nunique())

    if args.modo in {"anual", "resumen"}:
        imprimir_resumen_anual_y_exportar(dataset_trx, dataset_logins, cohort_size)
        return

    imprimir_y_exportar_resumen_mensual(dataset_trx, dataset_logins)


if __name__ == "__main__":
    try:
        main()
    except SQLAlchemyError as exc:
        msg = " ".join(str(exc).split())
        print(f"[ERROR] No se pudo conectar/consultar en {DB_NAME_DASHBOARD}: {msg}")
        sys.exit(1)
