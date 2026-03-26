import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, ".")

from core.db import run_query_file
from core.utils import exportar_excel


BASE_DIR = Path("productos/Generacion de BBDD/SV CU")
EXPORT_DIR = BASE_DIR / "exports"

QUERY_BY_TYPE = {
    "email": BASE_DIR / "Base Email.sql",
    "sms": BASE_DIR / "Base SMS.sql",
}

EXPECTED_COLUMNS = ["codigo_cliente", "nombre_cliente", "correo"]


def normalizar_codigo_cliente(series: pd.Series) -> pd.Series:
    codigos = (
        series.astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    return codigos.apply(lambda codigo: codigo.zfill(8) if codigo else codigo)


def build_output_path(tipo: str, output_arg: str, multiple: bool = False) -> Path:
    if output_arg.strip():
        base_output = Path(output_arg.strip())
        if not multiple:
            return base_output

        if base_output.suffix.lower() == ".xlsx":
            return base_output.with_name(f"{base_output.stem}_{tipo}{base_output.suffix}")
        return base_output.with_name(f"{base_output.name}_{tipo}.xlsx")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return EXPORT_DIR / f"base_{tipo}_{timestamp}.xlsx"


def export_tipo(tipo: str, output_arg: str = "", multiple: bool = False) -> None:
    query_path = QUERY_BY_TYPE[tipo]
    print(f"Ejecutando query ({tipo}): {query_path}")

    df = run_query_file(str(query_path))

    if df.empty:
        print(f"No se obtuvieron registros para exportar en tipo '{tipo}'.")
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

    duplicados = int(df.duplicated(subset=["codigo_cliente"]).sum())
    if duplicados > 0:
        df = df.drop_duplicates(subset=["codigo_cliente"], keep="first")
        print(f"- Duplicados removidos por codigo_cliente ({tipo}): {duplicados:,}")

    output_path = build_output_path(tipo, output_arg, multiple=multiple)
    exportar_excel(df, str(output_path), hoja=f"Base_{tipo.upper()}")

    print(f"- Total filas exportadas ({tipo}): {len(df.index):,}")
    print(f"- Archivo generado ({tipo}): {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ejecuta base SV CU (email/sms/ambos) y exporta resultados a Excel."
    )
    parser.add_argument(
        "--tipo",
        default="ambos",
        choices=["email", "sms", "ambos"],
        help="Tipo de base a ejecutar: email, sms o ambos. Default: ambos.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Ruta de salida del Excel (opcional).",
    )
    args = parser.parse_args()

    if args.tipo == "ambos":
        export_tipo("email", args.output, multiple=True)
        export_tipo("sms", args.output, multiple=True)
        return

    export_tipo(args.tipo, args.output, multiple=False)


if __name__ == "__main__":
    main()
