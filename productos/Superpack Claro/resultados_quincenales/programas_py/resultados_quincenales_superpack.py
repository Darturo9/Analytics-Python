"""
resultados_quincenales_superpack.py
====================================
Mide como va la compra de Superpack Claro en una quincena.

Genera exports/Resultados_Quincenal_Superpack_<ts>.xlsx con 2 hojas:
  - Resumen_Diario  : clientes unicos, transacciones netas, monto total por dia y canal
  - Totales         : fila resumen del periodo completo

Fechas configuradas en FECHA_INICIO / FECHA_FIN_EXCLUSIVA al inicio del archivo.
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

BASE_DIR     = Path(__file__).resolve().parents[1]
RUTA_QUERY   = BASE_DIR / "queries" / "compras_superpack_quincenal.sql"
RUTA_EXPORTS = BASE_DIR / "exports"

# ── Quincena a analizar ───────────────────────────────────────────────────────
FECHA_INICIO        = "2026-05-01"   # inclusivo
FECHA_FIN_EXCLUSIVA = "2026-05-16"   # exclusivo (dia siguiente al ultimo dia de quincena)
# ─────────────────────────────────────────────────────────────────────────────

CANAL_MAP = {
    1: "APP",
    7: "WEB",
}


def normalizar_canal(codigo) -> str:
    try:
        return CANAL_MAP.get(int(codigo), "OTRO")
    except (ValueError, TypeError):
        return "SIN_DATO"


def preparar_trx(df: pd.DataFrame) -> pd.DataFrame:
    trx = df.copy()
    trx["fecha_operacion"] = pd.to_datetime(trx["fecha_operacion"], errors="coerce").dt.date
    trx["monto_operacion"] = pd.to_numeric(trx["monto_operacion"], errors="coerce").fillna(0.0)
    trx["es_reversa"] = trx["es_reversa"].astype(str).str.strip().str.upper()
    trx["canal"] = trx["canal_operacion_codigo"].apply(normalizar_canal)
    trx["es_compra"] = trx["es_reversa"] != "S"
    return trx


def construir_resumen_diario(trx: pd.DataFrame) -> pd.DataFrame:
    compras = trx[trx["es_compra"]].copy()

    if compras.empty:
        return pd.DataFrame(columns=[
            "fecha", "clientes_unicos", "trx_netas", "monto_total", "APP", "WEB", "OTRO", "SIN_DATO"
        ])

    por_dia = (
        compras.groupby("fecha_operacion")
        .agg(
            clientes_unicos=("padded_codigo_cliente", "nunique"),
            trx_netas=("padded_codigo_cliente", "count"),
            monto_total=("monto_operacion", "sum"),
        )
        .reset_index()
        .rename(columns={"fecha_operacion": "fecha"})
    )

    canal_pivot = (
        compras.groupby(["fecha_operacion", "canal"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
        .rename(columns={"fecha_operacion": "fecha"})
    )
    for col in ["APP", "WEB", "OTRO", "SIN_DATO"]:
        if col not in canal_pivot.columns:
            canal_pivot[col] = 0

    resumen = por_dia.merge(canal_pivot, on="fecha", how="left")
    resumen["monto_total"] = resumen["monto_total"].round(2)
    return resumen.sort_values("fecha").reset_index(drop=True)


def construir_totales(resumen: pd.DataFrame) -> pd.DataFrame:
    if resumen.empty:
        return pd.DataFrame()

    num_cols = ["clientes_unicos", "trx_netas", "monto_total", "APP", "WEB", "OTRO", "SIN_DATO"]
    fila = {col: resumen[col].sum() for col in num_cols if col in resumen.columns}
    fila["clientes_unicos"] = resumen["clientes_unicos"].sum()
    fila["fecha"] = f"{FECHA_INICIO} a {FECHA_FIN_EXCLUSIVA} (excl.)"
    fila["monto_total"] = round(fila.get("monto_total", 0), 2)

    cols = ["fecha"] + [c for c in num_cols if c in fila]
    return pd.DataFrame([fila])[cols]


def main() -> None:
    RUTA_EXPORTS.mkdir(parents=True, exist_ok=True)

    try:
        df = run_query_file(
            str(RUTA_QUERY),
            params={
                "fecha_inicio": FECHA_INICIO,
                "fecha_fin_exclusiva": FECHA_FIN_EXCLUSIVA,
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
        print("[AVISO] No hay compras en el rango indicado.")
        sys.exit(0)

    trx = preparar_trx(df)
    resumen_diario = construir_resumen_diario(trx)
    totales = construir_totales(resumen_diario)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = RUTA_EXPORTS / f"Resultados_Quincenal_Superpack_{ts}.xlsx"

    exportar_excel_multi(
        {
            "Resumen_Diario": resumen_diario,
            "Totales": totales,
        },
        str(output_path),
    )

    print(f"Listo, archivo exportado: {output_path}")


if __name__ == "__main__":
    main()
