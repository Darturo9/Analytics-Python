"""
Árbol de Comunicación Sin Login — Conversión Campañas 47516 / 47619

KPI primario : cambio de contraseña (meta_cumplida)
KPI secundario: login (tuvo_login)

Uso:
    python reporte_conversion_arbol_47516_47619.py
"""

import sys
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

# Raíz del proyecto (3 niveles arriba de este archivo)
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

from core.db import run_query_file

QUERY_PATH  = Path(__file__).resolve().parents[1] / "queries" / "conversion_arbol_comunicacion_47516_47619.sql"
EXPORTS_DIR = Path(__file__).resolve().parents[1] / "exports"
EXPORTS_DIR.mkdir(exist_ok=True)


def imprimir_resumen(df: pd.DataFrame) -> None:
    total       = len(df)
    convertidos = df["meta_cumplida"].sum()
    tasa        = convertidos / total * 100 if total else 0

    print("\n" + "=" * 60)
    print("  ÁRBOL DE COMUNICACIÓN SIN LOGIN — CONVERSIÓN")
    print("=" * 60)
    print(f"  Clientes en campaña        : {total:,}")
    print(f"  Cambiaron contraseña        : {convertidos:,}  ({tasa:.1f}%)")
    print(f"  Solo hicieron login         : {df['tuvo_login'].sum() - (df['meta_cumplida'] & df['tuvo_login']).sum():,}")
    print(f"  No convirtieron             : {(df['meta_cumplida'] == 0).sum():,}")
    print("-" * 60)

    print("\n  Por tipo de campaña:\n")
    resumen = (
        df.groupby("tipo_campana")
        .agg(
            clientes=("padded_codigo_cliente", "count"),
            meta_cumplida=("meta_cumplida", "sum"),
            tuvo_login=("tuvo_login", "sum"),
            dias_cambio_pass_promedio=("dias_cambio_pass", "mean"),
            dias_login_promedio=("dias_login", "mean"),
        )
        .reset_index()
    )
    resumen["tasa_conversion_%"] = (resumen["meta_cumplida"] / resumen["clientes"] * 100).round(1)
    resumen["dias_cambio_pass_promedio"] = resumen["dias_cambio_pass_promedio"].round(1)
    resumen["dias_login_promedio"] = resumen["dias_login_promedio"].round(1)

    with pd.option_context("display.max_columns", None, "display.width", 120):
        print(resumen.to_string(index=False))

    print("\n  Distribución por tipo de conversión:\n")
    dist = df["tipo_conversion"].value_counts().reset_index()
    dist.columns = ["tipo_conversion", "clientes"]
    print(dist.to_string(index=False))
    print("=" * 60 + "\n")


def exportar_json(df: pd.DataFrame) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path  = EXPORTS_DIR / f"conversion_arbol_47516_47619_{timestamp}.json"

    registros = json.loads(
        df.to_json(orient="records", date_format="iso", force_ascii=False)
    )

    payload = {
        "generado_en": datetime.now().isoformat(),
        "campanas": ["47516 - Oferta Inicial", "47619 - Recordatorio 1"],
        "total_registros": len(df),
        "datos": registros,
    }

    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def main() -> None:
    print(f"\nEjecutando query: {QUERY_PATH.name} ...")

    try:
        df = run_query_file(str(QUERY_PATH))
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
        print("[AVISO] La query no retornó filas. Verifica los IDs de campaña.")
        sys.exit(0)

    imprimir_resumen(df)

    out_path = exportar_json(df)
    print(f"  Archivo JSON exportado: {out_path}\n")


if __name__ == "__main__":
    main()
