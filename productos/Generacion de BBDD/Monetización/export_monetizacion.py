import argparse
import re
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query_file
from core.utils import exportar_excel


BASE_DIR = Path(__file__).resolve().parent
QUERY_PATH = BASE_DIR / "Monetizacion.sql"
DEFAULT_OUTPUT = BASE_DIR / "exports" / "BBDD_MONETIZACION.xlsx"
EXPECTED_COLUMNS = [
    "codigo_cliente",
    "nombre_cliente",
    "numero_telefono",
    "nombre_operador",
    "correo",
    "anio_nac",
]


def construir_error_amigable(exc: Exception) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()

    if "permission was denied" in lower:
        match = re.search(
            r"permission was denied on the object '([^']+)', database '([^']+)', schema '([^']+)'",
            raw,
            flags=re.IGNORECASE,
        )
        if match:
            return (
                f"[ERROR] Permiso denegado en {match.group(2)}.{match.group(3)}.{match.group(1)}. "
                "Solicita permiso SELECT al DBA sobre ese objeto."
            )
        return "[ERROR] Permiso denegado al ejecutar la query. Solicita permisos SELECT al DBA."

    if "login timeout expired" in lower or "could not open a connection" in lower:
        return "[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales."

    return f"[ERROR] Fallo ejecutando la query: {raw}"


def normalizar_codigo_cliente(series: pd.Series) -> pd.Series:
    codigos = (
        series.astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    return codigos.apply(lambda codigo: codigo.zfill(8) if codigo else codigo)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ejecuta Monetizacion.sql y exporta el resultado a Excel."
    )
    parser.add_argument(
        "--output",
        default="",
        help="Ruta de salida del Excel (opcional). Default: exports/BBDD_MONETIZACION.xlsx",
    )
    args = parser.parse_args()

    print(f"Ejecutando query: {QUERY_PATH}")
    try:
        df = run_query_file(str(QUERY_PATH))
    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)

    if df.empty:
        print("No se obtuvieron registros para exportar.")
        return

    df.columns = [str(c) for c in df.columns]
    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"La consulta no devolvio las columnas esperadas: {missing_cols}. "
            f"Columnas recibidas: {list(df.columns)}"
        )

    df = df[EXPECTED_COLUMNS].copy()
    df = df.where(pd.notna(df), "")
    df["codigo_cliente"] = normalizar_codigo_cliente(df["codigo_cliente"])
    df["nombre_cliente"] = df["nombre_cliente"].astype(str).str.strip()
    df["correo"] = df["correo"].astype(str).str.strip().str.lower()
    df["numero_telefono"] = df["numero_telefono"].astype(str).str.strip()
    df["nombre_operador"] = df["nombre_operador"].astype(str).str.strip()

    duplicados = int(df.duplicated(subset=["codigo_cliente"]).sum())
    if duplicados > 0:
        df = df.drop_duplicates(subset=["codigo_cliente"], keep="first")
        print(f"- Duplicados removidos por codigo_cliente: {duplicados:,}")

    output_path = Path(args.output.strip()) if args.output.strip() else DEFAULT_OUTPUT
    exportar_excel(df, str(output_path), hoja="BBDD_MONETIZACION")

    print(f"- Total filas exportadas: {len(df.index):,}")
    print(f"- Archivo generado: {output_path}")


if __name__ == "__main__":
    main()
