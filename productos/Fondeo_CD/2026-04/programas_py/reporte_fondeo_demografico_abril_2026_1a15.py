"""
reporte_fondeo_demografico_abril_2026_1a15.py
----------------------------------------------
Reporte en consola para cuentas de Cuenta Digital creadas del 1 al 15 de abril 2026.

Imprime:
- porcentaje de cuentas MUJER / HOMBRE
- top departamentos con mas cuentas fondeadas (1 al 15)
- rango de edad y generacion que mas fondean
- cantidad y porcentaje de cuentas con movimiento

Nota: "con movimiento" se calcula con cant_transacciones > 0
segun dw_dep_depositos (alineado a scripts existentes del repo).

Ejecucion:
    python3 productos/Fondeo_CD/2026-04/programas_py/reporte_fondeo_demografico_abril_2026_1a15.py
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


RUTA_QUERY = (
    PROJECT_ROOT
    / "productos"
    / "Fondeo_CD"
    / "2026-04"
    / "queries"
    / "AnalisisFondeoDemograficoAbril2026_1a15.sql"
)

TOP_DEPTOS = 10


def pct(valor: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return (valor / total) * 100.0


def cargar_datos() -> pd.DataFrame:
    df = run_query_file(str(RUTA_QUERY))
    df.columns = [str(c) for c in df.columns]

    columnas_requeridas = [
        "numero_cuenta",
        "genero",
        "depto",
        "rango_edad",
        "generacion",
        "fondeada_1_15",
        "con_movimiento",
        "cant_transacciones",
    ]
    faltantes = [c for c in columnas_requeridas if c not in df.columns]
    if faltantes:
        raise ValueError(f"La query no devolvio columnas requeridas: {faltantes}")

    df["numero_cuenta"] = df["numero_cuenta"].astype(str).str.strip()
    df["genero"] = df["genero"].astype(str).str.strip().str.upper().replace("", "SIN_DATO")
    df["depto"] = df["depto"].astype(str).str.strip().replace("", "SIN DEPTO")
    df["rango_edad"] = df["rango_edad"].astype(str).str.strip().replace("", "SIN DATO")
    df["generacion"] = df["generacion"].astype(str).str.strip().replace("", "SIN DATO")
    df["fondeada_1_15"] = pd.to_numeric(df["fondeada_1_15"], errors="coerce").fillna(0).astype(int)
    df["con_movimiento"] = pd.to_numeric(df["con_movimiento"], errors="coerce").fillna(0).astype(int)
    df["cant_transacciones"] = pd.to_numeric(df["cant_transacciones"], errors="coerce").fillna(0).astype(float)

    # Seguridad: una fila por cuenta
    df = df.drop_duplicates(subset=["numero_cuenta"]).reset_index(drop=True)
    return df


def imprimir_resumen_genero(df: pd.DataFrame) -> None:
    total = len(df)
    mujeres = int((df["genero"] == "MUJER").sum())
    hombres = int((df["genero"] == "HOMBRE").sum())
    sin_dato = total - mujeres - hombres

    print("Genero (sobre cuentas creadas del 1 al 15):")
    print(f"- Mujer:     {mujeres:>10,} ({pct(mujeres, total):6.2f}%)")
    print(f"- Hombre:    {hombres:>10,} ({pct(hombres, total):6.2f}%)")
    print(f"- Sin dato:  {sin_dato:>10,} ({pct(sin_dato, total):6.2f}%)")


def imprimir_top_deptos_fondeadas(df: pd.DataFrame) -> None:
    base = df[df["fondeada_1_15"] == 1].copy()
    if base.empty:
        print("Top deptos con cuentas fondeadas: sin datos (no hay cuentas fondeadas en el periodo).")
        return

    top = (
        base.groupby("depto", as_index=False)["numero_cuenta"]
        .nunique()
        .rename(columns={"numero_cuenta": "cuentas_fondeadas"})
        .sort_values(["cuentas_fondeadas", "depto"], ascending=[False, True])
        .head(TOP_DEPTOS)
        .reset_index(drop=True)
    )

    print(f"Top {TOP_DEPTOS} deptos con mas cuentas fondeadas (1 al 15):")
    for i, row in top.iterrows():
        print(f"{i + 1:>2}. {str(row['depto']):<25} {int(row['cuentas_fondeadas']):>10,}")


def imprimir_top_edad_generacion_fondeo(df: pd.DataFrame) -> None:
    base = df[df["fondeada_1_15"] == 1].copy()
    total_fondeadas = len(base)
    if total_fondeadas == 0:
        print("Rango de edad y generacion con mas fondeo: sin datos (0 fondeadas).")
        return

    rango = (
        base.groupby("rango_edad", as_index=False)["numero_cuenta"]
        .nunique()
        .rename(columns={"numero_cuenta": "cuentas_fondeadas"})
        .sort_values(["cuentas_fondeadas", "rango_edad"], ascending=[False, True])
        .reset_index(drop=True)
    )
    rango["porcentaje"] = (rango["cuentas_fondeadas"] / total_fondeadas * 100.0).round(2)

    gen = (
        base.groupby("generacion", as_index=False)["numero_cuenta"]
        .nunique()
        .rename(columns={"numero_cuenta": "cuentas_fondeadas"})
        .sort_values(["cuentas_fondeadas", "generacion"], ascending=[False, True])
        .reset_index(drop=True)
    )
    gen["porcentaje"] = (gen["cuentas_fondeadas"] / total_fondeadas * 100.0).round(2)

    top_rango = rango.iloc[0]
    top_gen = gen.iloc[0]

    print("Rango de edad que mas fondea (sobre cuentas fondeadas del 1 al 15):")
    print(
        f"- {top_rango['rango_edad']}: "
        f"{int(top_rango['cuentas_fondeadas']):,} cuentas ({float(top_rango['porcentaje']):.2f}%)"
    )
    print("Generacion que mas fondea (sobre cuentas fondeadas del 1 al 15):")
    print(
        f"- {top_gen['generacion']}: "
        f"{int(top_gen['cuentas_fondeadas']):,} cuentas ({float(top_gen['porcentaje']):.2f}%)"
    )


def imprimir_movimiento(df: pd.DataFrame) -> None:
    total = len(df)
    con_mov = int((df["con_movimiento"] == 1).sum())
    sin_mov = total - con_mov

    print("Movimiento (cant_transacciones > 0):")
    print(f"- Cuentas con movimiento:    {con_mov:>10,} ({pct(con_mov, total):6.2f}%)")
    print(f"- Cuentas sin movimiento:    {sin_mov:>10,} ({pct(sin_mov, total):6.2f}%)")


def imprimir_reporte(df: pd.DataFrame) -> None:
    if df.empty:
        print("No se devolvieron registros para abril 2026 (1-15).")
        return

    total = len(df)
    fondeadas = int((df["fondeada_1_15"] == 1).sum())

    print("=" * 88)
    print("REPORTE DEMOGRAFICO Y FONDEO - ABRIL 2026 (CUENTAS CREADAS DEL 1 AL 15)")
    print("=" * 88)
    print(f"Total cuentas creadas (1-15 abril 2026):            {total:>10,}")
    print(f"Cuentas fondeadas al menos 1 vez (1-15 abril 2026):  {fondeadas:>10,}")
    print(f"Tasa de fondeo (1-15 abril 2026):                    {pct(fondeadas, total):>9.2f}%")
    print("-" * 88)

    imprimir_resumen_genero(df)
    print("-" * 88)
    imprimir_top_deptos_fondeadas(df)
    print("-" * 88)
    imprimir_top_edad_generacion_fondeo(df)
    print("-" * 88)
    imprimir_movimiento(df)
    print("=" * 88)


def main() -> None:
    print(f"Cargando query: {RUTA_QUERY}")
    try:
        df = cargar_datos()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    imprimir_reporte(df)


if __name__ == "__main__":
    main()
