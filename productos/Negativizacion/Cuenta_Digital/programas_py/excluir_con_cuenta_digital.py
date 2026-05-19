"""
excluir_con_cuenta_digital.py
=============================
Lee un Excel de clientes desde imports/, consulta quienes tienen cuenta digital
(desde 2026-01-01), y genera dos archivos en exports/:
  - Clientes_CON_Cuenta_Digital.xlsx  → excluidos
  - Clientes_SIN_Cuenta_Digital.xlsx  → lista limpia para negativizacion

Configuracion:
  CONFIG_NOMBRE_ARCHIVO  : nombre del Excel en imports/ (sin extension o con ella)
  CONFIG_COL_CODIGO      : nombre de la columna con el codigo de cliente
"""

import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

from core.db import run_query_file

BASE_DIR   = Path(__file__).resolve().parents[1]
RUTA_IMPORTS = BASE_DIR / "imports"
RUTA_EXPORTS = BASE_DIR / "exports"
RUTA_QUERY   = BASE_DIR / "queries" / "clientes_con_cuenta_digital.sql"

# ── Configuracion ─────────────────────────────────────────────────────────────
CONFIG_NOMBRE_ARCHIVO = ""          # dejar vacío para tomar el único archivo en imports/
CONFIG_COL_CODIGO     = "codigo_cliente"
# ─────────────────────────────────────────────────────────────────────────────


def resolver_archivo_entrada() -> Path:
    if not RUTA_IMPORTS.exists():
        raise FileNotFoundError(f"No existe la carpeta imports: {RUTA_IMPORTS}")

    candidatos = [
        p for p in RUTA_IMPORTS.iterdir()
        if p.is_file() and p.suffix.lower() in {".xlsx", ".xls"} and not p.name.startswith("~")
    ]
    if not candidatos:
        raise FileNotFoundError("No hay archivos Excel en imports/. Sube el archivo de clientes.")

    if CONFIG_NOMBRE_ARCHIVO.strip():
        objetivo = CONFIG_NOMBRE_ARCHIVO.strip().lower()
        exactos = [p for p in candidatos if p.name.lower() == objetivo or p.stem.lower() == objetivo]
        if exactos:
            return exactos[0]
        raise FileNotFoundError(
            f"No se encontro '{CONFIG_NOMBRE_ARCHIVO}' en imports/. "
            f"Archivos disponibles: {[p.name for p in candidatos]}"
        )

    if len(candidatos) == 1:
        return candidatos[0]

    raise FileNotFoundError(
        f"Hay {len(candidatos)} archivos en imports/. "
        "Especifica CONFIG_NOMBRE_ARCHIVO o deja solo uno."
    )


def normalizar_codigo(serie: pd.Series) -> pd.Series:
    return (
        serie.astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .apply(lambda x: x[-8:].zfill(8) if x.isdigit() else x)
    )


def main() -> None:
    RUTA_EXPORTS.mkdir(parents=True, exist_ok=True)

    # Cargar archivo de entrada
    ruta_entrada = resolver_archivo_entrada()
    df_clientes = pd.read_excel(ruta_entrada, dtype=str)

    if CONFIG_COL_CODIGO not in df_clientes.columns:
        raise ValueError(
            f"Columna '{CONFIG_COL_CODIGO}' no encontrada. "
            f"Columnas disponibles: {list(df_clientes.columns)}"
        )

    df_clientes["_codigo_norm"] = normalizar_codigo(df_clientes[CONFIG_COL_CODIGO])

    # Cargar clientes con cuenta digital desde BD
    try:
        df_cuentas = run_query_file(str(RUTA_QUERY))
    except SQLAlchemyError as exc:
        msg = str(exc).lower()
        if "permission was denied" in msg:
            print("[ERROR] Permiso denegado. Solicita acceso al DBA.")
        elif "login timeout expired" in msg or "server not found" in msg:
            print("[ERROR] No se pudo conectar. Verifica red/VPN.")
        else:
            print(f"[ERROR] {exc}")
        sys.exit(1)

    codigos_con_cuenta = set(
        df_cuentas["padded_codigo_cliente"].astype(str).str.strip().dropna()
    )

    # Clasificar
    df_clientes["tiene_cuenta_digital"] = df_clientes["_codigo_norm"].isin(codigos_con_cuenta)

    cols_salida = [c for c in df_clientes.columns if not c.startswith("_")]
    df_con_cuenta = df_clientes[df_clientes["tiene_cuenta_digital"]].copy()[cols_salida]
    df_sin_cuenta = df_clientes[~df_clientes["tiene_cuenta_digital"]].copy()[cols_salida]

    # Exportar
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ruta_con = RUTA_EXPORTS / f"Clientes_CON_Cuenta_Digital_{ts}.xlsx"
    ruta_sin = RUTA_EXPORTS / f"Clientes_SIN_Cuenta_Digital_{ts}.xlsx"

    df_con_cuenta.to_excel(ruta_con, index=False)
    df_sin_cuenta.to_excel(ruta_sin, index=False)

    total     = len(df_clientes)
    con_cta   = len(df_con_cuenta)
    sin_cta   = len(df_sin_cuenta)
    pct       = round(con_cta / total * 100, 1) if total else 0.0

    print(f"Total clientes    : {total:,}")
    print(f"Con cuenta digital: {con_cta:,} ({pct}%) → {ruta_con}")
    print(f"Sin cuenta digital: {sin_cta:,} → {ruta_sin}")


if __name__ == "__main__":
    main()
