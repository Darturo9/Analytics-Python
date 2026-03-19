import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, ".")

from core.db import run_query_file
from core.utils import exportar_excel


QUERY_PATH = "productos/Pago de servicios/Queries/DetallePrimerPagoClientes_parametros.sql"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Exporta un Excel con 1 fila por cliente y su primer pago (parametros PagosdeServicios)."
    )
    parser.add_argument(
        "--fecha-inicio",
        default="2025-01-01",
        help="Fecha inicio para considerar pagos. Usa 'none' para sin fecha.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Ruta Excel de salida (opcional).",
    )
    args = parser.parse_args()

    fecha_inicio = None if str(args.fecha_inicio).strip().lower() == "none" else args.fecha_inicio
    params = {"fecha_inicio": fecha_inicio}

    print("Generando detalle de primer pago por cliente...")
    print(f"- Filtro fecha inicio: {fecha_inicio if fecha_inicio else 'SIN FECHA'}")
    df = run_query_file(QUERY_PATH, params=params)

    if df.empty:
        print("No se obtuvieron registros.")
        return

    df.columns = [str(c) for c in df.columns]
    df = df.where(pd.notna(df), "")

    if args.output.strip():
        output_path = Path(args.output.strip())
    else:
        base_dir = Path("productos/Pago de servicios/exports")
        suffix = fecha_inicio if fecha_inicio else "sin_fecha"
        output_path = base_dir / f"validacion_primer_pago_clientes_{suffix}.xlsx"

    exportar_excel(df, str(output_path), hoja="PrimerPagoCliente")
    print(f"- Clientes unicos con pago: {len(df.index):,}")
    print(f"- Excel exportado: {output_path}")


if __name__ == "__main__":
    main()
