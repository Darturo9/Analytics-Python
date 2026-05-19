"""
Árbol de Comunicación Sin Login — Conversión por FechaAplica (47516/47619)

KPI primario: cambio de contraseña.
Ventana: día 0, 1 y 2 desde FechaAplica.
Si hay nuevo envío del cliente antes de convertir, se corta la ventana del envío previo.

Uso:
    python3 reporte_conversion_arbol_47516_47619.py

Fechas configuradas en FECHA_INICIO / FECHA_FIN al inicio del archivo.
"""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

# Raíz del proyecto
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

from core.db import run_query_file
from core.utils import exportar_excel_multi

QUERY_PATH = Path(__file__).resolve().parents[1] / "queries" / "conversion_arbol_comunicacion_47516_47619.sql"
EXPORTS_DIR = Path(__file__).resolve().parents[1] / "exports"
EXPORTS_DIR.mkdir(exist_ok=True)

# ── Fechas a analizar ────────────────────────────────────────────────────────
FECHA_INICIO = "2026-05-11"
FECHA_FIN    = "2026-05-17"
# ─────────────────────────────────────────────────────────────────────────────


def build_output_path(output_arg: str) -> Path:
    if output_arg.strip():
        out = Path(output_arg.strip())
        if out.suffix.lower() == ".xlsx":
            return out
        return out.with_suffix(".xlsx")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return EXPORTS_DIR / f"conversion_arbol_47516_47619_{timestamp}.xlsx"


def preparar_detalle(df: pd.DataFrame) -> pd.DataFrame:
    detalle = df.copy()
    detalle["CampaignID"] = detalle["CampaignID"].astype(str)

    for col in [
        "fecha_comunicacion",
        "siguiente_envio_cliente",
        "fecha_fin_ventana",
        "fecha_conversion_password",
    ]:
        detalle[col] = pd.to_datetime(detalle[col], errors="coerce").dt.date

    detalle["dias_a_conversion"] = pd.to_numeric(detalle["dias_a_conversion"], errors="coerce")
    detalle["convirtio_password"] = pd.to_numeric(detalle["convirtio_password"], errors="coerce").fillna(0).astype(int)

    detalle["tipo_conversion"] = detalle["dias_a_conversion"].apply(
        lambda x: f"Dia {int(x)}" if pd.notna(x) else "No convirtio"
    )

    detalle = detalle.rename(
        columns={
            "padded_codigo_cliente": "codigo_cliente",
            "CampaignID": "campaign_id",
            "tipo_campana": "campana",
            "fecha_comunicacion": "fecha_aplica",
            "siguiente_envio_cliente": "siguiente_envio_cliente",
            "fecha_fin_ventana": "fecha_fin_ventana",
            "fecha_conversion_password": "fecha_conversion_password",
            "dias_a_conversion": "dias_a_conversion",
        }
    )

    columnas = [
        "codigo_cliente",
        "campaign_id",
        "campana",
        "fecha_aplica",
        "siguiente_envio_cliente",
        "fecha_fin_ventana",
        "fecha_conversion_password",
        "dias_a_conversion",
        "convirtio_password",
        "tipo_conversion",
    ]

    detalle = detalle[columnas].sort_values(
        by=["fecha_aplica", "campaign_id", "codigo_cliente"],
        kind="stable",
    )

    return detalle.reset_index(drop=True)


def _agregar_metricas(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    agg = (
        df.groupby(group_cols, dropna=False)
        .agg(
            enviados=("codigo_cliente", "count"),
            conv_dia_0=("conv_dia_0", "sum"),
            conv_dia_1=("conv_dia_1", "sum"),
            conv_dia_2=("conv_dia_2", "sum"),
            conv_3d_total=("convirtio_password", "sum"),
        )
        .reset_index()
    )
    agg["tasa_conv_3d"] = (agg["conv_3d_total"] / agg["enviados"] * 100).round(2)
    return agg


def construir_resumen_diario(detalle: pd.DataFrame) -> pd.DataFrame:
    base = detalle.copy()
    base["campaign_id"] = base["campaign_id"].astype(str)
    base["campana"] = base["campana"].astype(str)
    base["conv_dia_0"] = (base["dias_a_conversion"] == 0).astype(int)
    base["conv_dia_1"] = (base["dias_a_conversion"] == 1).astype(int)
    base["conv_dia_2"] = (base["dias_a_conversion"] == 2).astype(int)

    total = _agregar_metricas(base, ["fecha_aplica"])
    total["segmento"] = "TOTAL"
    total["campaign_id"] = "TOTAL"

    por_campana = _agregar_metricas(base, ["fecha_aplica", "campaign_id", "campana"])
    por_campana["segmento"] = por_campana["campaign_id"] + " - " + por_campana["campana"]

    total = total[[
        "fecha_aplica",
        "segmento",
        "campaign_id",
        "enviados",
        "conv_dia_0",
        "conv_dia_1",
        "conv_dia_2",
        "conv_3d_total",
        "tasa_conv_3d",
    ]]

    por_campana = por_campana[[
        "fecha_aplica",
        "segmento",
        "campaign_id",
        "enviados",
        "conv_dia_0",
        "conv_dia_1",
        "conv_dia_2",
        "conv_3d_total",
        "tasa_conv_3d",
    ]]

    resumen = pd.concat([total, por_campana], ignore_index=True)
    resumen["orden_segmento"] = resumen["segmento"].eq("TOTAL").map({True: 0, False: 1})
    resumen = resumen.sort_values(
        by=["fecha_aplica", "orden_segmento", "campaign_id"],
        kind="stable",
    ).drop(columns=["orden_segmento"])

    return resumen.reset_index(drop=True)


def imprimir_resumen_consola(resumen: pd.DataFrame, detalle: pd.DataFrame) -> None:
    total_envios = len(detalle)
    total_conv = int(detalle["convirtio_password"].sum())
    tasa_global = (total_conv / total_envios * 100) if total_envios else 0.0

    print("\n" + "=" * 72)
    print("  ARBOL SIN LOGIN — CONVERSION POR FECHAAPLICA (PASSWORD, VENTANA 3 DIAS)")
    print("=" * 72)
    print(f"  Envios analizados           : {total_envios:,}")
    print(f"  Conversiones (password)     : {total_conv:,}")
    print(f"  Tasa global                 : {tasa_global:.2f}%")
    print("-" * 72)

    preview = resumen.head(18)
    with pd.option_context("display.max_columns", None, "display.width", 160):
        print("\n  Resumen diario (primeras filas):\n")
        print(preview.to_string(index=False))
    print("=" * 72 + "\n")


def validar_consistencia(resumen: pd.DataFrame) -> None:
    inconsistencias = resumen[
        (resumen["conv_dia_0"] + resumen["conv_dia_1"] + resumen["conv_dia_2"]) != resumen["conv_3d_total"]
    ]
    if not inconsistencias.empty:
        print("[AVISO] Se detectaron filas con inconsistencia conv_dia_0+1+2 != conv_3d_total")


def main() -> None:
    print(f"\nEjecutando query: {QUERY_PATH.name}")
    print(f"- fecha_inicio: {FECHA_INICIO}")
    print(f"- fecha_fin   : {FECHA_FIN}")

    try:
        df = run_query_file(
            str(QUERY_PATH),
            params={
                "fecha_inicio": FECHA_INICIO,
                "fecha_fin": FECHA_FIN,
            },
        )
    except SQLAlchemyError as exc:
        msg = str(exc).lower()
        if "permission was denied" in msg:
            print("[ERROR] Permiso denegado. Solicita acceso al DBA.")
        elif "login timeout expired" in msg or "server not found" in msg:
            print("[ERROR] No se pudo conectar. Verifica red/VPN.")
        else:
            print(f"[ERROR] {exc}")
        sys.exit(1)

    if df.empty:
        print("[AVISO] La query no retorno filas para el rango indicado.")
        sys.exit(0)

    detalle = preparar_detalle(df)
    resumen = construir_resumen_diario(detalle)
    validar_consistencia(resumen)
    imprimir_resumen_consola(resumen, detalle)

    output_path = build_output_path("")
    exportar_excel_multi(
        {
            "Resumen_Diario": resumen,
            "Detalle_Envios": detalle,
        },
        str(output_path),
    )

    print(f"- Archivo generado: {output_path}")
    print(f"- Total filas detalle: {len(detalle):,}")
    print(f"- Total filas resumen: {len(resumen):,}\n")


if __name__ == "__main__":
    main()
