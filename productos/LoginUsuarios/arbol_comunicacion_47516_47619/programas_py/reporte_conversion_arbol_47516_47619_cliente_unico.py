"""
Árbol de Comunicación Sin Login — Conversión Cliente Único (47516/47619)

KPI primario: cambio de contraseña.
Ventana: día 0, 1 y 2 desde FechaAplica.
Si hay nuevo envío del cliente antes de convertir, se corta la ventana del envío previo.

Uso:
    python3 reporte_conversion_arbol_47516_47619_cliente_unico.py
    python3 reporte_conversion_arbol_47516_47619_cliente_unico.py --fecha-inicio 2026-03-16
    python3 reporte_conversion_arbol_47516_47619_cliente_unico.py --fecha-inicio 2026-03-16 --fecha-fin 2026-03-31
    python3 reporte_conversion_arbol_47516_47619_cliente_unico.py --output exports/mi_reporte.xlsx
"""

import argparse
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

DEFAULT_FECHA_INICIO = "2026-03-16"


def build_output_path(output_arg: str) -> Path:
    if output_arg.strip():
        out = Path(output_arg.strip())
        if out.suffix.lower() == ".xlsx":
            return out
        return out.with_suffix(".xlsx")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return EXPORTS_DIR / f"conversion_arbol_47516_47619_cliente_unico_{timestamp}.xlsx"


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


def construir_cliente_unico(detalle: pd.DataFrame) -> pd.DataFrame:
    base = detalle.copy()
    base["campaign_id"] = base["campaign_id"].astype(str)
    base["convirtio_password"] = pd.to_numeric(base["convirtio_password"], errors="coerce").fillna(0).astype(int)
    base["fecha_aplica"] = pd.to_datetime(base["fecha_aplica"], errors="coerce").dt.date
    base["fecha_conversion_password"] = pd.to_datetime(base["fecha_conversion_password"], errors="coerce").dt.date

    clientes = pd.DataFrame({"codigo_cliente": base["codigo_cliente"].drop_duplicates().sort_values()})

    fechas = (
        base.pivot_table(
            index="codigo_cliente",
            columns="campaign_id",
            values="fecha_aplica",
            aggfunc="max",
        )
        .rename(columns={"47516": "fecha_47516", "47619": "fecha_47619"})
        .reset_index()
    )

    conversiones = base[base["convirtio_password"] == 1].copy()
    if not conversiones.empty:
        conversiones["prioridad_campana"] = conversiones["campaign_id"].eq("47619").astype(int)
        conversiones["fecha_aplica_dt"] = pd.to_datetime(conversiones["fecha_aplica"], errors="coerce")
        conversiones["fecha_conversion_dt"] = pd.to_datetime(conversiones["fecha_conversion_password"], errors="coerce")

        # Elegir una sola conversión por cliente:
        # 1) priorizar R1 (47619), 2) luego la última comunicación que convirtió.
        conversiones = conversiones.sort_values(
            by=["codigo_cliente", "prioridad_campana", "fecha_aplica_dt", "fecha_conversion_dt"],
            ascending=[True, False, False, True],
            kind="stable",
        )
        conversiones = conversiones.drop_duplicates(subset=["codigo_cliente"], keep="first")
        conversiones = conversiones[[
            "codigo_cliente",
            "campaign_id",
            "fecha_aplica",
            "fecha_conversion_password",
        ]].rename(columns={
            "campaign_id": "campana_conversion",
            "fecha_aplica": "fecha_ultima_comunicacion",
            "fecha_conversion_password": "fecha_conversion",
        })
    else:
        conversiones = pd.DataFrame(columns=[
            "codigo_cliente",
            "campana_conversion",
            "fecha_ultima_comunicacion",
            "fecha_conversion",
        ])

    cliente_unico = clientes.merge(fechas, on="codigo_cliente", how="left").merge(conversiones, on="codigo_cliente", how="left")

    cliente_unico["convirtio_por_47516"] = cliente_unico["campana_conversion"].eq("47516").astype(int)
    cliente_unico["convirtio_por_47619"] = cliente_unico["campana_conversion"].eq("47619").astype(int)
    cliente_unico["convirtio_total"] = cliente_unico["campana_conversion"].notna().astype(int)

    fecha_conv_dt = pd.to_datetime(cliente_unico["fecha_conversion"], errors="coerce")
    fecha_ult_dt = pd.to_datetime(cliente_unico["fecha_ultima_comunicacion"], errors="coerce")
    cliente_unico["dias_desde_ultima_comunicacion"] = (fecha_conv_dt - fecha_ult_dt).dt.days

    columnas = [
        "codigo_cliente",
        "fecha_47516",
        "fecha_47619",
        "convirtio_por_47516",
        "convirtio_por_47619",
        "convirtio_total",
        "fecha_ultima_comunicacion",
        "fecha_conversion",
        "dias_desde_ultima_comunicacion",
    ]
    for col in ["fecha_47516", "fecha_47619", "fecha_ultima_comunicacion", "fecha_conversion"]:
        if col not in cliente_unico.columns:
            cliente_unico[col] = pd.NaT

    cliente_unico = cliente_unico[columnas].sort_values(by=["codigo_cliente"], kind="stable")
    return cliente_unico.reset_index(drop=True)


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

    totales_dia = resumen[resumen["segmento"] == "TOTAL"].copy()
    totales_dia = totales_dia.sort_values("fecha_aplica")

    print(f"\n  {'Fecha':<12} {'Enviados':>10} {'Conv D0':>8} {'Conv D1':>8} {'Conv D2':>8} {'Conv Total':>11} {'Tasa%':>7}")
    print(f"  {'-'*12} {'-'*10} {'-'*8} {'-'*8} {'-'*8} {'-'*11} {'-'*7}")
    for _, row in totales_dia.iterrows():
        print(
            f"  {str(row['fecha_aplica']):<12}"
            f" {int(row['enviados']):>10,}"
            f" {int(row['conv_dia_0']):>8,}"
            f" {int(row['conv_dia_1']):>8,}"
            f" {int(row['conv_dia_2']):>8,}"
            f" {int(row['conv_3d_total']):>11,}"
            f" {float(row['tasa_conv_3d']):>7.2f}"
        )
    print("=" * 72 + "\n")


def validar_consistencia(resumen: pd.DataFrame) -> None:
    inconsistencias = resumen[
        (resumen["conv_dia_0"] + resumen["conv_dia_1"] + resumen["conv_dia_2"]) != resumen["conv_3d_total"]
    ]
    if not inconsistencias.empty:
        print("[AVISO] Se detectaron filas con inconsistencia conv_dia_0+1+2 != conv_3d_total")


def validar_cliente_unico(cliente_unico: pd.DataFrame, detalle: pd.DataFrame) -> None:
    esperados = detalle["codigo_cliente"].nunique()
    reales = cliente_unico["codigo_cliente"].nunique()
    if esperados != reales:
        print(f"[AVISO] Cliente_Unico no coincide en clientes unicos. esperado={esperados:,}, real={reales:,}")

    duplicados = int(cliente_unico.duplicated(subset=["codigo_cliente"]).sum())
    if duplicados > 0:
        print(f"[AVISO] Cliente_Unico tiene duplicados por codigo_cliente: {duplicados:,}")

    inconsistente_total = cliente_unico[
        cliente_unico["convirtio_total"]
        != ((cliente_unico["convirtio_por_47516"] == 1) | (cliente_unico["convirtio_por_47619"] == 1)).astype(int)
    ]
    if not inconsistente_total.empty:
        print("[AVISO] Cliente_Unico tiene filas con convirtio_total inconsistente.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reporte de conversion por FechaAplica + hoja Cliente_Unico (campanas 47516/47619)."
    )
    parser.add_argument(
        "--fecha-inicio",
        default=DEFAULT_FECHA_INICIO,
        help=f"Fecha inicio (YYYY-MM-DD). Default: {DEFAULT_FECHA_INICIO}",
    )
    parser.add_argument(
        "--fecha-fin",
        default="",
        help="Fecha fin (YYYY-MM-DD). Opcional.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Ruta de salida del Excel (opcional).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fecha_fin_param = args.fecha_fin.strip() or None

    print(f"\nEjecutando query: {QUERY_PATH.name}")
    print(f"- fecha_inicio: {args.fecha_inicio}")
    print(f"- fecha_fin   : {fecha_fin_param if fecha_fin_param else 'NULL (sin tope)'}")

    try:
        df = run_query_file(
            str(QUERY_PATH),
            params={
                "fecha_inicio": args.fecha_inicio,
                "fecha_fin": fecha_fin_param,
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
    cliente_unico = construir_cliente_unico(detalle)
    validar_consistencia(resumen)
    validar_cliente_unico(cliente_unico, detalle)
    imprimir_resumen_consola(resumen, detalle)

    output_path = build_output_path(args.output)
    exportar_excel_multi(
        {
            "Resumen_Diario": resumen,
            "Detalle_Envios": detalle,
            "Cliente_Unico": cliente_unico,
        },
        str(output_path),
    )

    print(f"- Archivo generado: {output_path}")
    print(f"- Total filas detalle: {len(detalle):,}")
    print(f"- Total filas resumen: {len(resumen):,}\n")


if __name__ == "__main__":
    main()
