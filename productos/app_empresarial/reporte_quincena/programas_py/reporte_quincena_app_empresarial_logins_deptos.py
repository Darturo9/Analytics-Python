"""
Reporte quincenal configurable de logins - App Empresarial.

Objetivo:
- Total de logins
- Clientes unicos con login
- Top de departamentos (direccion nivel 2) por clientes y por total logins

Ejecucion:
    python3 productos/app_empresarial/reporte_quincena/programas_py/reporte_quincena_app_empresarial_logins_deptos.py
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sys

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query_file


BASE_DIR = Path(__file__).resolve().parents[1]
QUERY_PATH = BASE_DIR / "queries" / "login_deptos_quincena.sql"

# Configuracion editable desde codigo.
CONFIG_ANIO = 2026
CONFIG_MES = 5
CONFIG_DIA_INICIO = 1
CONFIG_DIA_FIN = 15
CONFIG_TOP_DEPTOS = 10


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


def cargar_datos(fecha_inicio: date, fecha_fin_exclusiva: date) -> pd.DataFrame:
    df = run_query_file(
        str(QUERY_PATH),
        params={
            "fecha_inicio": fecha_inicio.isoformat(),
            "fecha_fin_exclusiva": fecha_fin_exclusiva.isoformat(),
        },
    )
    df.columns = [str(c).strip() for c in df.columns]
    if df.empty:
        return df

    for col in [
        "ranking_depto",
        "clientes_unicos_depto",
        "total_logins_depto",
        "total_logins_global",
        "clientes_unicos_global",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df["depto"] = df["depto"].fillna("SIN DATO").astype(str).str.strip().replace("", "SIN DATO")
    return df


def imprimir_reporte(df: pd.DataFrame, fecha_inicio: date, fecha_fin_exclusiva: date) -> None:
    fecha_fin_inclusiva = fecha_fin_exclusiva - timedelta(days=1)

    print("=" * 92)
    print("APP EMPRESARIAL - LOGIN CLIENTES Y DEPARTAMENTOS")
    print("=" * 92)
    print(f"Rango de logins:                  {fecha_inicio.isoformat()} a {fecha_fin_inclusiva.isoformat()}")
    print("Fuente login:                     dw_bel_IBSTTRA_VIEW (secode login/web-login/app-login)")
    print("Fuente depto:                     DW_CIF_DIRECCIONES.DW_NIVEL_GEO2 (CLDICO=1)")
    print("-" * 92)

    if df.empty:
        print("Total logins:                     0")
        print("Clientes unicos con login:        0")
        print("Sin resultados para el rango.")
        print("=" * 92)
        return

    total_logins = int(df["total_logins_global"].iloc[0])
    clientes_unicos = int(df["clientes_unicos_global"].iloc[0])

    print(f"Total logins:                     {total_logins:>12,}")
    print(f"Clientes unicos con login:        {clientes_unicos:>12,}")
    print("=" * 92)

    top_deptos = (
        df.sort_values(
            ["clientes_unicos_depto", "total_logins_depto", "depto"],
            ascending=[False, False, True],
        )
        .head(CONFIG_TOP_DEPTOS)
        .copy()
    )
    top_deptos["pct_clientes"] = (
        (top_deptos["clientes_unicos_depto"] / clientes_unicos * 100.0) if clientes_unicos else 0.0
    )
    top_deptos["pct_logins"] = (
        (top_deptos["total_logins_depto"] / total_logins * 100.0) if total_logins else 0.0
    )

    print(f"\nTop {CONFIG_TOP_DEPTOS} deptos (clientes unicos con login):")
    print(
        top_deptos[
            [
                "ranking_depto",
                "depto",
                "clientes_unicos_depto",
                "pct_clientes",
                "total_logins_depto",
                "pct_logins",
            ]
        ].to_string(
            index=False,
            formatters={
                "clientes_unicos_depto": "{:,.0f}".format,
                "total_logins_depto": "{:,.0f}".format,
                "pct_clientes": "{:,.2f}%".format,
                "pct_logins": "{:,.2f}%".format,
            },
        )
    )


def main() -> None:
    try:
        fecha_inicio, fecha_fin_exclusiva = validar_rango(
            CONFIG_ANIO, CONFIG_MES, CONFIG_DIA_INICIO, CONFIG_DIA_FIN
        )
        print(f"Cargando query: {QUERY_PATH}")
        print(
            "Configuracion -> "
            f"anio={CONFIG_ANIO}, mes={CONFIG_MES}, dia_inicio={CONFIG_DIA_INICIO}, "
            f"dia_fin={CONFIG_DIA_FIN}, top_deptos={CONFIG_TOP_DEPTOS}"
        )

        df = cargar_datos(fecha_inicio, fecha_fin_exclusiva)
        imprimir_reporte(df, fecha_inicio, fecha_fin_exclusiva)

    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
