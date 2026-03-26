import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, ".")

from core.db import run_query, run_query_file
from core.utils import exportar_excel


QUERY_PATH = "productos/Generacion de BBDD/Pago de servicios (claro)/base_clientes_contacto.sql"
DEFAULT_EXPORT_DIR = "productos/Generacion de BBDD/Pago de servicios (claro)/exports"

BASE_COUNT_SQL = """
SELECT COUNT(c.CLDOC) AS total_base
FROM DW_CIF_CLIENTES c
WHERE
    c.dw_usuarios_bel_cnt > 0
    AND c.ESTATU IN ('A')
    AND c.DW_FECHA_NACIMIENTO <= DATEADD(YEAR, -18, GETDATE())
    AND c.CLTIPE IN ('N')
"""


def build_output_path(output_arg: str) -> Path:
    if output_arg.strip():
        return Path(output_arg.strip())
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Path(DEFAULT_EXPORT_DIR) / f"base_clientes_contacto_{timestamp}.xlsx"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Exporta base de clientes con correo obligatorio y celular opcional."
    )
    parser.add_argument(
        "--output",
        default="",
        help="Ruta del archivo Excel de salida (opcional).",
    )
    args = parser.parse_args()

    print("Generando base de clientes con contacto...")
    df = run_query_file(QUERY_PATH)

    if df.empty:
        print("No se obtuvieron registros para exportar.")
        return

    expected_cols = ["codigo_cliente", "nombre_cliente", "correo", "celular"]
    missing_cols = [col for col in expected_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"La consulta no devolvio columnas esperadas: {missing_cols}")

    df = df[expected_cols].copy()
    df.columns = [str(c) for c in df.columns]
    df = df.where(pd.notna(df), "")

    duplicated_rows = int(df.duplicated(subset=["codigo_cliente"]).sum())
    if duplicated_rows > 0:
        df = df.drop_duplicates(subset=["codigo_cliente"], keep="first")
        print(f"- Duplicados removidos por codigo_cliente: {duplicated_rows:,}")

    universo_base = run_query(BASE_COUNT_SQL)
    total_base = int(universo_base["total_base"].iloc[0]) if not universo_base.empty else 0
    total_exportado = len(df.index)

    output_path = build_output_path(args.output)
    exportar_excel(df, str(output_path), hoja="BaseClientes")

    print(f"- Total universo base: {total_base:,}")
    print(f"- Total clientes exportados: {total_exportado:,}")
    print(f"- Validacion exportado <= base: {'OK' if total_exportado <= total_base else 'ALERTA'}")
    print(f"- Ruta de salida: {output_path}")


if __name__ == "__main__":
    main()
