"""
Árbol de Comunicación Sin Login — Conversión por FechaAplica (47723–48739)

KPI primario: login.
Ventana: día 0 al día 7 desde FechaAplica (8 días).
Si hay nuevo envío del cliente antes de que venza la ventana, se corta en el día previo.

Uso:
    python3 reporte_conversion_arbol_47723_48739.py

Fechas configuradas en FECHA_INICIO / FECHA_FIN al inicio del archivo.
"""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

from core.db import run_query_file
from core.utils import exportar_excel_multi

QUERY_PATH = Path(__file__).resolve().parents[1] / "queries" / "conversion_arbol_comunicacion_47723_48739.sql"
EXPORTS_DIR = Path(__file__).resolve().parents[1] / "exports"
EXPORTS_DIR.mkdir(exist_ok=True)

# ── Fechas a analizar ────────────────────────────────────────────────────────
FECHA_INICIO = "2026-05-11"
FECHA_FIN    = "2026-05-17"
# ─────────────────────────────────────────────────────────────────────────────


def build_output_path() -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return EXPORTS_DIR / f"conversion_arbol_47723_48739_{timestamp}.xlsx"


def preparar_detalle(df: pd.DataFrame) -> pd.DataFrame:
    detalle = df.copy()
    detalle["CampaignID"] = detalle["CampaignID"].astype(str)

    for col in [
        "fecha_comunicacion",
        "siguiente_envio_cliente",
        "fecha_fin_ventana",
        "fecha_conversion_login",
    ]:
        detalle[col] = pd.to_datetime(detalle[col], errors="coerce").dt.date

    detalle["dias_a_conversion"] = pd.to_numeric(detalle["dias_a_conversion"], errors="coerce")
    detalle["convirtio_login"] = pd.to_numeric(detalle["convirtio_login"], errors="coerce").fillna(0).astype(int)

    detalle["tipo_conversion"] = detalle["dias_a_conversion"].apply(
        lambda x: f"Dia {int(x)}" if pd.notna(x) else "No convirtio"
    )

    detalle = detalle.rename(
        columns={
            "padded_codigo_cliente": "codigo_cliente",
            "CampaignID": "campaign_id",
            "tipo_campana": "campana",
            "fecha_comunicacion": "fecha_aplica",
        }
    )

    columnas = [
        "codigo_cliente",
        "campaign_id",
        "campana",
        "fecha_aplica",
        "siguiente_envio_cliente",
        "fecha_fin_ventana",
        "fecha_conversion_login",
        "dias_a_conversion",
        "convirtio_login",
        "tipo_conversion",
    ]

    return detalle[columnas].sort_values(
        by=["fecha_aplica", "campaign_id", "codigo_cliente"],
        kind="stable",
    ).reset_index(drop=True)


def _agregar_metricas(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    agg = (
        df.groupby(group_cols, dropna=False)
        .agg(
            enviados=("codigo_cliente", "count"),
            conv_dia_0=("conv_dia_0", "sum"),
            conv_dia_1=("conv_dia_1", "sum"),
            conv_dia_2=("conv_dia_2", "sum"),
            conv_dia_3=("conv_dia_3", "sum"),
            conv_dia_4=("conv_dia_4", "sum"),
            conv_dia_5=("conv_dia_5", "sum"),
            conv_dia_6=("conv_dia_6", "sum"),
            conv_dia_7=("conv_dia_7", "sum"),
            conv_8d_total=("convirtio_login", "sum"),
        )
        .reset_index()
    )
    agg["tasa_conv_8d"] = (agg["conv_8d_total"] / agg["enviados"] * 100).round(2)
    return agg


def construir_resumen_diario(detalle: pd.DataFrame) -> pd.DataFrame:
    base = detalle.copy()
    base["campaign_id"] = base["campaign_id"].astype(str)
    base["campana"] = base["campana"].astype(str)
    for d in range(8):
        base[f"conv_dia_{d}"] = (base["dias_a_conversion"] == d).astype(int)

    total = _agregar_metricas(base, ["fecha_aplica"])
    total["segmento"] = "TOTAL"
    total["campaign_id"] = "TOTAL"

    por_campana = _agregar_metricas(base, ["fecha_aplica", "campaign_id", "campana"])
    por_campana["segmento"] = por_campana["campaign_id"] + " - " + por_campana["campana"]

    cols_orden = [
        "fecha_aplica", "segmento", "campaign_id",
        "enviados",
        "conv_dia_0", "conv_dia_1", "conv_dia_2", "conv_dia_3",
        "conv_dia_4", "conv_dia_5", "conv_dia_6", "conv_dia_7",
        "conv_8d_total", "tasa_conv_8d",
    ]

    resumen = pd.concat(
        [total[cols_orden], por_campana[cols_orden]],
        ignore_index=True,
    )
    resumen["_orden"] = resumen["segmento"].eq("TOTAL").map({True: 0, False: 1})
    resumen = resumen.sort_values(
        by=["fecha_aplica", "_orden", "campaign_id"],
        kind="stable",
    ).drop(columns=["_orden"])

    return resumen.reset_index(drop=True)


CAMPANAS_LABEL = {
    "47723": "Recordatorio 2",
    "47955": "Recordatorio 3",
    "48101": "Recordatorio 4",
    "48311": "Recordatorio 5",
    "48514": "Recordatorio 6",
    "48739": "Recordatorio 7",
}


def construir_cliente_unico(detalle: pd.DataFrame) -> pd.DataFrame:
    base = detalle.copy()
    base["campaign_id"] = base["campaign_id"].astype(str)
    base["convirtio_login"] = pd.to_numeric(base["convirtio_login"], errors="coerce").fillna(0).astype(int)
    base["fecha_aplica"] = pd.to_datetime(base["fecha_aplica"], errors="coerce").dt.date
    base["fecha_conversion_login"] = pd.to_datetime(base["fecha_conversion_login"], errors="coerce").dt.date

    clientes = pd.DataFrame({"codigo_cliente": base["codigo_cliente"].drop_duplicates().sort_values()})

    total_comms = (
        base.groupby("codigo_cliente", as_index=False)
        .size()
        .rename(columns={"size": "total_comunicaciones"})
    )

    fechas = (
        base.pivot_table(
            index="codigo_cliente",
            columns="campaign_id",
            values="fecha_aplica",
            aggfunc="max",
        )
        .rename(columns={cid: f"fecha_{label.lower().replace(' ', '_')}" for cid, label in CAMPANAS_LABEL.items()})
        .reset_index()
    )

    conversiones = base[base["convirtio_login"] == 1].copy()
    if not conversiones.empty:
        conversiones["fecha_aplica_dt"] = pd.to_datetime(conversiones["fecha_aplica"], errors="coerce")
        conversiones["fecha_conversion_dt"] = pd.to_datetime(conversiones["fecha_conversion_login"], errors="coerce")
        # Por cliente: priorizar campaña con ID mayor (más reciente), luego conversión más temprana
        conversiones = conversiones.sort_values(
            by=["codigo_cliente", "campaign_id", "fecha_aplica_dt", "fecha_conversion_dt"],
            ascending=[True, False, False, True],
            kind="stable",
        )
        conversiones = conversiones.drop_duplicates(subset=["codigo_cliente"], keep="first")
        conversiones["campana_conversion"] = conversiones["campaign_id"].map(CAMPANAS_LABEL)
        conversiones = conversiones[[
            "codigo_cliente",
            "campana_conversion",
            "fecha_aplica",
            "fecha_conversion_login",
        ]].rename(columns={
            "fecha_aplica": "fecha_ultima_comunicacion",
            "fecha_conversion_login": "fecha_conversion",
        })
    else:
        conversiones = pd.DataFrame(columns=[
            "codigo_cliente",
            "campana_conversion",
            "fecha_ultima_comunicacion",
            "fecha_conversion",
        ])

    cliente_unico = (
        clientes
        .merge(total_comms, on="codigo_cliente", how="left")
        .merge(fechas, on="codigo_cliente", how="left")
        .merge(conversiones, on="codigo_cliente", how="left")
    )

    cliente_unico["convirtio_total"] = cliente_unico["campana_conversion"].notna().astype(int)

    fecha_conv_dt = pd.to_datetime(cliente_unico["fecha_conversion"], errors="coerce")
    fecha_ult_dt = pd.to_datetime(cliente_unico["fecha_ultima_comunicacion"], errors="coerce")
    cliente_unico["dias_desde_ultima_comunicacion"] = (fecha_conv_dt - fecha_ult_dt).dt.days

    fecha_cols = [f"fecha_{label.lower().replace(' ', '_')}" for label in CAMPANAS_LABEL.values()]
    cols_existentes = [c for c in fecha_cols if c in cliente_unico.columns]
    for col in fecha_cols:
        if col not in cliente_unico.columns:
            cliente_unico[col] = pd.NaT

    columnas = (
        ["codigo_cliente", "total_comunicaciones"]
        + fecha_cols
        + ["convirtio_total", "campana_conversion",
           "fecha_ultima_comunicacion", "fecha_conversion",
           "dias_desde_ultima_comunicacion"]
    )

    return cliente_unico[columnas].sort_values(by=["codigo_cliente"], kind="stable").reset_index(drop=True)


def validar_cliente_unico(cliente_unico: pd.DataFrame, detalle: pd.DataFrame) -> None:
    esperados = detalle["codigo_cliente"].nunique()
    reales = cliente_unico["codigo_cliente"].nunique()
    if esperados != reales:
        print(f"[AVISO] Cliente_Unico no coincide en clientes unicos. esperado={esperados:,}, real={reales:,}")

    duplicados = int(cliente_unico.duplicated(subset=["codigo_cliente"]).sum())
    if duplicados > 0:
        print(f"[AVISO] Cliente_Unico tiene duplicados por codigo_cliente: {duplicados:,}")


def imprimir_resumen_consola(resumen: pd.DataFrame, detalle: pd.DataFrame) -> None:
    total_envios = len(detalle)
    total_conv = int(detalle["convirtio_login"].sum())
    tasa_global = (total_conv / total_envios * 100) if total_envios else 0.0

    print("\n" + "=" * 72)
    print("  ARBOL SIN LOGIN — CONVERSION POR FECHAAPLICA (LOGIN, VENTANA 8 DIAS)")
    print("=" * 72)
    print(f"  Envios analizados       : {total_envios:,}")
    print(f"  Conversiones (login)    : {total_conv:,}")
    print(f"  Tasa global             : {tasa_global:.2f}%")
    print("-" * 72)

    preview = resumen.head(18)
    with pd.option_context("display.max_columns", None, "display.width", 180):
        print("\n  Resumen diario (primeras filas):\n")
        print(preview.to_string(index=False))
    print("=" * 72 + "\n")


def validar_consistencia(resumen: pd.DataFrame) -> None:
    dia_cols = [f"conv_dia_{d}" for d in range(8)]
    inconsistencias = resumen[
        resumen[dia_cols].sum(axis=1) != resumen["conv_8d_total"]
    ]
    if not inconsistencias.empty:
        print("[AVISO] Filas con inconsistencia sum(conv_dia_0..7) != conv_8d_total")


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
    cliente_unico = construir_cliente_unico(detalle)
    validar_consistencia(resumen)
    validar_cliente_unico(cliente_unico, detalle)
    imprimir_resumen_consola(resumen, detalle)

    output_path = build_output_path()
    exportar_excel_multi(
        {
            "Resumen_Diario": resumen,
            "Cliente_Unico": cliente_unico,
        },
        str(output_path),
    )

    print(f"- Archivo generado: {output_path}")
    print(f"- Total filas detalle: {len(detalle):,}")
    print(f"- Total filas resumen: {len(resumen):,}\n")


if __name__ == "__main__":
    main()
