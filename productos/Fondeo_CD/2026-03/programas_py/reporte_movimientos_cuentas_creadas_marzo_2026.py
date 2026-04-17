"""
reporte_movimientos_cuentas_creadas_marzo_2026.py
-------------------------------------------------
Imprime en consola:
- cuantas cuentas de Cuenta Digital creadas en marzo 2026 tuvieron movimientos
- cuantas transacciones acumulan (solo marzo y al corte del 31-03-2026)
- resumen diario de marzo (cuentas con mov y transacciones del dia)

Exporta a Excel:
- productos/Fondeo_CD/2026-03/exports/MovimientosCuentasCreadasMarzo2026.xlsx
  - ResumenEscenarios
  - ResumenDiasMarzo
  - DetalleCuentasMarzo
  - DetalleCuentaDia

Ejecucion:
    python3 productos/Fondeo_CD/2026-03/programas_py/reporte_movimientos_cuentas_creadas_marzo_2026.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query_file


RUTA_QUERY_DETALLE = (
    PROJECT_ROOT
    / "productos"
    / "Fondeo_CD"
    / "2026-03"
    / "queries"
    / "CuentasCreadasMarzoMovimientosHistoricoDetalle.sql"
)
RUTA_EXPORTS = PROJECT_ROOT / "productos" / "Fondeo_CD" / "2026-03" / "exports"
RUTA_SALIDA_XLSX = RUTA_EXPORTS / "MovimientosCuentasCreadasMarzo2026.xlsx"


def _fmt_int(value: float | int) -> str:
    return f"{int(round(float(value or 0))):,}"


def _fmt_dec(value: float | int) -> str:
    return f"{float(value or 0):,.2f}"


def _first_non_null(series: pd.Series):
    clean = series.dropna()
    if clean.empty:
        return pd.NA
    return clean.iloc[0]


def cargar_detalle() -> pd.DataFrame:
    df = run_query_file(str(RUTA_QUERY_DETALLE))
    df.columns = [str(c) for c in df.columns]
    columnas_numericas = [
        "ctctrx_dia",
        "ctctrx_prev",
        "delta_transacciones_dia",
        "movimiento_dia",
        "transacciones_corte_31",
        "saldo_ayer_corte",
        "saldo_promedio_corte",
    ]
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    columnas_fecha = [
        "fecha_apertura",
        "fecha_informacion",
        "fecha_ultimo_movimiento",
        "fecha_informacion_corte",
    ]
    for col in columnas_fecha:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "padded_codigo_cliente" in df.columns:
        df["padded_codigo_cliente"] = df["padded_codigo_cliente"].astype(str).str.zfill(8)
    if "cuenta" in df.columns:
        df["cuenta"] = df["cuenta"].astype(str).str.strip()

    return df


def construir_resumenes(df_detalle: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if df_detalle.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    base = (
        df_detalle.groupby("cuenta", as_index=False)
        .agg(
            padded_codigo_cliente=("padded_codigo_cliente", _first_non_null),
            fecha_apertura=("fecha_apertura", "min"),
            transacciones_corte_31=("transacciones_corte_31", "max"),
            fecha_ultimo_movimiento=("fecha_ultimo_movimiento", "max"),
            moneda=("moneda", _first_non_null),
            saldo_ayer_corte=("saldo_ayer_corte", "max"),
            saldo_promedio_corte=("saldo_promedio_corte", "max"),
            estatus_cuenta=("estatus_cuenta", _first_non_null),
            fecha_informacion_corte=("fecha_informacion_corte", "max"),
        )
        .reset_index(drop=True)
    )

    movimientos_por_cuenta = (
        df_detalle.groupby("cuenta", as_index=False)
        .agg(
            transacciones_marzo=("delta_transacciones_dia", "sum"),
            dias_con_movimiento_marzo=("movimiento_dia", "sum"),
            dias_reportados_marzo=("fecha_informacion", "nunique"),
        )
    )

    df_cuentas = base.merge(movimientos_por_cuenta, on="cuenta", how="left")
    for col in ["transacciones_marzo", "dias_con_movimiento_marzo", "dias_reportados_marzo"]:
        if col in df_cuentas.columns:
            df_cuentas[col] = pd.to_numeric(df_cuentas[col], errors="coerce").fillna(0)
    df_cuentas["con_movimiento_marzo"] = (df_cuentas["transacciones_marzo"] > 0).astype(int)
    df_cuentas["con_movimiento_corte"] = (df_cuentas["transacciones_corte_31"] > 0).astype(int)

    df_cuentas = df_cuentas.sort_values(
        ["padded_codigo_cliente", "cuenta"], na_position="last"
    ).reset_index(drop=True)

    diario = df_detalle[df_detalle["fecha_informacion"].notna()].copy()
    if diario.empty:
        df_dias = pd.DataFrame(
            columns=[
                "fecha",
                "cuentas_con_movimiento",
                "cuentas_sin_movimiento",
                "cuentas_reportadas",
                "pct_cuentas_con_movimiento",
                "total_transacciones_dia",
                "promedio_transacciones_por_cuenta_con_mov",
            ]
        )
    else:
        df_dias = (
            diario.groupby("fecha_informacion", as_index=False)
            .agg(
                cuentas_con_movimiento=("movimiento_dia", "sum"),
                cuentas_reportadas=("cuenta", "nunique"),
                total_transacciones_dia=("delta_transacciones_dia", "sum"),
            )
            .rename(columns={"fecha_informacion": "fecha"})
        )
        df_dias["cuentas_sin_movimiento"] = (
            df_dias["cuentas_reportadas"] - df_dias["cuentas_con_movimiento"]
        )
        df_dias["pct_cuentas_con_movimiento"] = (
            (df_dias["cuentas_con_movimiento"] * 100.0) / df_dias["cuentas_reportadas"].replace(0, pd.NA)
        ).fillna(0)
        df_dias["promedio_transacciones_por_cuenta_con_mov"] = (
            df_dias["total_transacciones_dia"] / df_dias["cuentas_con_movimiento"].replace(0, pd.NA)
        ).fillna(0)
        df_dias = df_dias.sort_values("fecha").reset_index(drop=True)

    total_cuentas = int(df_cuentas["cuenta"].nunique())
    con_mov_marzo = int(df_cuentas["con_movimiento_marzo"].sum())
    sin_mov_marzo = total_cuentas - con_mov_marzo
    total_tx_marzo = int(df_cuentas["transacciones_marzo"].sum())
    promedio_marzo = (total_tx_marzo / total_cuentas) if total_cuentas else 0
    max_tx_marzo = int(df_cuentas["transacciones_marzo"].max()) if total_cuentas else 0

    con_mov_corte = int(df_cuentas["con_movimiento_corte"].sum())
    sin_mov_corte = total_cuentas - con_mov_corte
    total_tx_corte = int(pd.to_numeric(df_cuentas["transacciones_corte_31"], errors="coerce").fillna(0).sum())
    promedio_corte = (total_tx_corte / total_cuentas) if total_cuentas else 0
    max_tx_corte = int(pd.to_numeric(df_cuentas["transacciones_corte_31"], errors="coerce").fillna(0).max()) if total_cuentas else 0

    df_resumen = pd.DataFrame(
        [
            {
                "escenario": "SOLO_MARZO_CTCTRX_DELTA",
                "cuentas_creadas_marzo": total_cuentas,
                "cuentas_con_movimiento": con_mov_marzo,
                "cuentas_sin_movimiento": sin_mov_marzo,
                "pct_cuentas_con_movimiento": (con_mov_marzo * 100.0 / total_cuentas) if total_cuentas else 0,
                "total_transacciones": total_tx_marzo,
                "promedio_transacciones_por_cuenta": promedio_marzo,
                "max_transacciones_en_una_cuenta": max_tx_marzo,
            },
            {
                "escenario": "CORTE_2026_03_31_CTCTRX",
                "cuentas_creadas_marzo": total_cuentas,
                "cuentas_con_movimiento": con_mov_corte,
                "cuentas_sin_movimiento": sin_mov_corte,
                "pct_cuentas_con_movimiento": (con_mov_corte * 100.0 / total_cuentas) if total_cuentas else 0,
                "total_transacciones": total_tx_corte,
                "promedio_transacciones_por_cuenta": promedio_corte,
                "max_transacciones_en_una_cuenta": max_tx_corte,
            },
        ]
    )

    return df_resumen, df_dias, df_cuentas


def imprimir_resumen_escenarios(df_resumen: pd.DataFrame) -> None:
    print("=" * 78)
    print("MOVIMIENTOS EN CUENTAS CREADAS EN MARZO 2026 - CUENTA DIGITAL")
    print("=" * 78)
    for _, row in df_resumen.iterrows():
        escenario = str(row.get("escenario", "SIN_ESCENARIO"))
        if escenario == "SOLO_MARZO_CTCTRX_DELTA":
            etiqueta = "ESCENARIO 1 - SOLO_MARZO (delta diario de CTCTRX)"
        elif escenario == "CORTE_2026_03_31_CTCTRX":
            etiqueta = "ESCENARIO 2 - CORTE_31_MARZO (foto de CTCTRX al cierre)"
        else:
            etiqueta = f"ESCENARIO - {escenario}"

        print(f"\n{etiqueta}")
        print("-" * 78)
        print(f"Cuentas creadas en marzo 2026:                 {_fmt_int(row['cuentas_creadas_marzo'])}")
        print(f"Cuentas con movimiento (> 0 trx):              {_fmt_int(row['cuentas_con_movimiento'])}")
        print(f"Cuentas sin movimiento:                         {_fmt_int(row['cuentas_sin_movimiento'])}")
        print(f"% cuentas con movimiento:                       {_fmt_dec(row['pct_cuentas_con_movimiento'])}%")
        print("-" * 78)
        print(f"Total de transacciones:                         {_fmt_int(row['total_transacciones'])}")
        print(f"Promedio de transacciones por cuenta:          {_fmt_dec(row['promedio_transacciones_por_cuenta'])}")
        print(f"Maximo de transacciones en una cuenta:         {_fmt_int(row['max_transacciones_en_una_cuenta'])}")
    print("=" * 78)


def imprimir_resumen_dias(df_dias: pd.DataFrame) -> None:
    if df_dias.empty:
        print("\nNo hubo registros diarios para marzo 2026.")
        return

    print("\nRESUMEN DIARIO DE MARZO 2026 (solo cuentas creadas en marzo)")
    print("-" * 78)
    print(f"{'Fecha':<12} {'Ctas con mov':>12} {'Tx del dia':>12} {'% ctas con mov':>16}")
    print("-" * 78)
    for _, row in df_dias.iterrows():
        fecha = pd.to_datetime(row["fecha"], errors="coerce")
        fecha_txt = fecha.strftime("%Y-%m-%d") if pd.notna(fecha) else ""
        print(
            f"{fecha_txt:<12} "
            f"{_fmt_int(row['cuentas_con_movimiento']):>12} "
            f"{_fmt_int(row['total_transacciones_dia']):>12} "
            f"{_fmt_dec(row['pct_cuentas_con_movimiento']) + '%':>16}"
        )


def exportar_excel(
    df_resumen: pd.DataFrame,
    df_dias: pd.DataFrame,
    df_cuentas: pd.DataFrame,
    df_detalle: pd.DataFrame,
) -> None:
    RUTA_EXPORTS.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(RUTA_SALIDA_XLSX, engine="openpyxl") as writer:
        df_resumen.to_excel(writer, sheet_name="ResumenEscenarios", index=False)
        df_dias.to_excel(writer, sheet_name="ResumenDiasMarzo", index=False)
        df_cuentas.to_excel(writer, sheet_name="DetalleCuentasMarzo", index=False)
        df_detalle.to_excel(writer, sheet_name="DetalleCuentaDia", index=False)


def main() -> None:
    print(f"Cargando query detalle: {RUTA_QUERY_DETALLE}")
    t0 = time.perf_counter()
    try:
        df_detalle = cargar_detalle()
    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] Fallo cargando datos: {exc}")
        raise SystemExit(1) from exc

    if df_detalle.empty:
        print("No se devolvieron registros para marzo 2026.")
        raise SystemExit(0)

    df_resumen, df_dias, df_cuentas = construir_resumenes(df_detalle)
    imprimir_resumen_escenarios(df_resumen)
    imprimir_resumen_dias(df_dias)
    exportar_excel(df_resumen, df_dias, df_cuentas, df_detalle)
    print(f"\nExport generado: {RUTA_SALIDA_XLSX}")

    t1 = time.perf_counter()
    print(f"\nTiempo total de ejecucion: {_fmt_dec(t1 - t0)}s")


if __name__ == "__main__":
    main()
