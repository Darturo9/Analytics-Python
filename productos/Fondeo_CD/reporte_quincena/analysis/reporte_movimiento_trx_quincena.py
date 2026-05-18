"""
reporte_movimiento_trx_quincena.py
----------------------------------
Reporte de consola para movimiento y transacciones en Fondeo_CD.

Salida:
- Cantidad de cuentas con movimiento
- Total de transacciones
- Monto total transaccionado
- Top de transacciones mas realizadas (por canal + tipo de uso)

Configuracion:
- Se modifica en constantes CONFIG_* dentro de este archivo.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
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
    / "reporte_quincena"
    / "queries"
    / "movimiento_trx_quincena.sql"
)

# Configuracion editable (sin argumentos en terminal).
CONFIG_ANIO = 2026
CONFIG_MES = 5
CONFIG_DIA_INICIO = 1
CONFIG_DIA_FIN = 15

# 1 = solo clientes fondeados en el periodo, 0 = todo el universo creado.
CONFIG_SOLO_FONDEADAS = 1

CONFIG_TOP_N = 10


def validar_rango(anio: int, mes: int, dia_inicio: int, dia_fin: int) -> tuple[date, date]:
    if mes < 1 or mes > 12:
        raise ValueError("CONFIG_MES debe estar entre 1 y 12.")
    if dia_inicio < 1:
        raise ValueError("CONFIG_DIA_INICIO debe ser >= 1.")
    if dia_fin < dia_inicio:
        raise ValueError("CONFIG_DIA_FIN debe ser >= CONFIG_DIA_INICIO.")

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


def cargar_datos(params: dict) -> pd.DataFrame:
    df = run_query_file(str(RUTA_QUERY), params=params)
    df.columns = [str(c).strip().lower() for c in df.columns]
    if df.empty:
        return df

    columnas_numericas = [
        "cuentas_universo",
        "cuentas_con_movimiento",
        "total_trx",
        "monto_trx_total",
        "ranking_global",
        "ranking_origen",
        "total_transacciones",
        "clientes_unicos",
        "monto_total",
        "monto_promedio",
    ]
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def imprimir_reporte(df: pd.DataFrame, periodo: str) -> None:
    if df.empty:
        print("No se devolvieron datos para el periodo configurado.")
        return

    base = df.iloc[0]
    cuentas_universo = int(base.get("cuentas_universo", 0) or 0)
    cuentas_mov = int(base.get("cuentas_con_movimiento", 0) or 0)
    total_trx = int(base.get("total_trx", 0) or 0)
    monto_trx_total = float(base.get("monto_trx_total", 0.0) or 0.0)
    pct_mov = (cuentas_mov / cuentas_universo * 100.0) if cuentas_universo > 0 else 0.0

    top = df[df["ranking_global"].notna()].copy()
    top = top.sort_values("ranking_global")
    if not top.empty:
        top["ranking_global"] = top["ranking_global"].astype(int)
        top["ranking_origen"] = top["ranking_origen"].astype(int)
        if total_trx > 0:
            top["pct_trx"] = (top["total_transacciones"] / total_trx * 100.0).round(2)
        else:
            top["pct_trx"] = 0.0

    print("=" * 118)
    print("MOVIMIENTO Y TRANSACCIONES - FONDEO_CD (RANGO CONFIGURABLE)")
    print("=" * 118)
    print(f"Periodo analizado:                               {periodo}")
    print(
        "Universo usado:                                  "
        + ("Solo clientes fondeados" if CONFIG_SOLO_FONDEADAS == 1 else "Todo cliente creado en el periodo")
    )
    print(f"Cuentas en universo:                              {cuentas_universo:>12,}")
    print(f"Cuentas con movimiento (trx > 0):                 {cuentas_mov:>12,}")
    print(f"Porcentaje con movimiento:                         {pct_mov:>11.2f}%")
    print(f"Total de transacciones:                            {total_trx:>12,}")
    print(f"Monto total transaccionado:                        {monto_trx_total:>12,.2f}")
    def imprimir_top_bloque(df_bloque: pd.DataFrame, titulo: str, ranking_col: str) -> None:
        print("-" * 118)
        print(titulo)
        print("-" * 118)
        if df_bloque.empty:
            print("Sin transacciones para este bloque.")
            return
        top_b = df_bloque.head(CONFIG_TOP_N).copy()
        print(
            top_b[
                [
                    ranking_col,
                    "tipo_uso",
                    "total_transacciones",
                    "pct_trx",
                    "clientes_unicos",
                    "monto_total",
                    "monto_promedio",
                ]
            ].to_string(
                index=False,
                formatters={
                    "total_transacciones": "{:,.0f}".format,
                    "pct_trx": "{:,.2f}%".format,
                    "clientes_unicos": "{:,.0f}".format,
                    "monto_total": "{:,.2f}".format,
                    "monto_promedio": "{:,.2f}".format,
                },
            )
        )

    if top.empty:
        print("-" * 118)
        print("Sin transacciones para mostrar top en este periodo.")
    else:
        bxi = top[top["origen"] == "BXI"].sort_values("ranking_origen")
        multi = top[top["origen"] == "MULTIPAGO"].sort_values("ranking_origen")

        consolidado = (
            top.groupby("tipo_uso", as_index=False)
            .agg(
                total_transacciones=("total_transacciones", "sum"),
                clientes_unicos=("clientes_unicos", "sum"),
                monto_total=("monto_total", "sum"),
            )
            .sort_values(["total_transacciones", "monto_total", "tipo_uso"], ascending=[False, False, True])
            .reset_index(drop=True)
        )
        consolidado["monto_promedio"] = consolidado.apply(
            lambda r: (r["monto_total"] / r["total_transacciones"]) if r["total_transacciones"] > 0 else 0.0,
            axis=1,
        )
        consolidado["pct_trx"] = (
            (consolidado["total_transacciones"] / total_trx * 100.0).round(2) if total_trx > 0 else 0.0
        )
        consolidado["ranking_consolidado"] = consolidado.index + 1

        imprimir_top_bloque(
            bxi.rename(columns={"ranking_origen": "ranking_bxi"}),
            f"Top {CONFIG_TOP_N} transacciones mas realizadas - BXI",
            "ranking_bxi",
        )
        imprimir_top_bloque(
            multi.rename(columns={"ranking_origen": "ranking_multi"}),
            f"Top {CONFIG_TOP_N} transacciones mas realizadas - MULTIPAGO",
            "ranking_multi",
        )
        imprimir_top_bloque(
            consolidado.rename(columns={"ranking_consolidado": "ranking_cons"}),
            f"Top {CONFIG_TOP_N} transacciones mas realizadas - CONSOLIDADO",
            "ranking_cons",
        )

    print("=" * 118)


def main() -> None:
    try:
        fecha_inicio, fecha_fin_exclusiva = validar_rango(
            CONFIG_ANIO, CONFIG_MES, CONFIG_DIA_INICIO, CONFIG_DIA_FIN
        )
        periodo = f"{fecha_inicio.isoformat()} a {(fecha_fin_exclusiva - timedelta(days=1)).isoformat()}"

        params = {
            "fecha_inicio_universo": fecha_inicio.isoformat(),
            "fecha_fin_universo_exclusiva": fecha_fin_exclusiva.isoformat(),
            "fecha_inicio_transacciones": fecha_inicio.isoformat(),
            "fecha_fin_transacciones_exclusiva": fecha_fin_exclusiva.isoformat(),
            "solo_fondeadas": int(CONFIG_SOLO_FONDEADAS),
        }

        print(f"Cargando query: {RUTA_QUERY}")
        print(
            "Configuracion -> "
            f"anio={CONFIG_ANIO}, mes={CONFIG_MES}, dia_inicio={CONFIG_DIA_INICIO}, "
            f"dia_fin={CONFIG_DIA_FIN}, solo_fondeadas={CONFIG_SOLO_FONDEADAS}, top_n={CONFIG_TOP_N}"
        )

        df = cargar_datos(params)
        imprimir_reporte(df, periodo)

    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
