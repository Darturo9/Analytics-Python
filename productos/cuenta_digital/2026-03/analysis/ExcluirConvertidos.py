import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, ".")

from core.db import run_query_file
from core.utils import exportar_excel


BASE_DIR = Path("productos/cuenta_digital/2026-03")
INPUT_DIR = BASE_DIR / "archivoExcel"
INPUT_FILE = INPUT_DIR / "clientes_para_excluir.xlsx"
SQL_FILE = BASE_DIR / "queries" / "clientes_con_cuenta_digital.sql"
OUTPUT_DIR = BASE_DIR / "exports"
OUTPUT_FILE = OUTPUT_DIR / "clientes_sin_cuenta_digital.xlsx"
MATCH_FILE = OUTPUT_DIR / "clientes_con_cuenta_digital.xlsx"
EXPECTED_COLUMN = "Cliente"


def normalizar_codigo_cliente(serie: pd.Series) -> pd.Series:
    s = serie.astype(str).str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.replace(r"[^0-9]", "", regex=True)
    s = s.where(s.notna() & (s.str.len() > 0), pd.NA)
    return s.str.zfill(8)


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"No se encontró el Excel de entrada: {INPUT_FILE}\n"
            f"Crea el archivo con encabezado '{EXPECTED_COLUMN}'."
        )

    print("Leyendo archivo Excel de clientes...")
    df_excel = pd.read_excel(INPUT_FILE, dtype=str)
    if EXPECTED_COLUMN not in df_excel.columns:
        raise ValueError(
            f"El Excel debe incluir el encabezado '{EXPECTED_COLUMN}'. "
            f"Encabezados actuales: {list(df_excel.columns)}"
        )

    df_excel["_codigo_cliente"] = normalizar_codigo_cliente(df_excel[EXPECTED_COLUMN])
    df_excel = df_excel[df_excel["_codigo_cliente"].notna()].copy()

    print("Consultando clientes con cuenta digital...")
    df_cuentas = run_query_file(str(SQL_FILE))
    if "padded_codigo_cliente" not in df_cuentas.columns:
        raise ValueError(
            "La query no devolvió la columna esperada 'padded_codigo_cliente'."
        )

    df_cuentas["_codigo_cliente"] = normalizar_codigo_cliente(
        df_cuentas["padded_codigo_cliente"]
    )
    codigos_con_cuenta = set(df_cuentas["_codigo_cliente"].dropna().unique())

    df_match = df_excel[df_excel["_codigo_cliente"].isin(codigos_con_cuenta)].copy()
    df_sin_cuenta = df_excel[~df_excel["_codigo_cliente"].isin(codigos_con_cuenta)].copy()

    codigos_match = sorted(df_match["_codigo_cliente"].dropna().unique())
    print("\nCódigos del Excel que YA tienen cuenta digital:")
    if codigos_match:
        for codigo in codigos_match:
            print(codigo)
    else:
        print("(ninguno)")

    df_match.drop(columns=["_codigo_cliente"], inplace=True)
    df_sin_cuenta.drop(columns=["_codigo_cliente"], inplace=True)

    exportar_excel(df_match, str(MATCH_FILE), hoja="ConCuentaDigital")
    exportar_excel(df_sin_cuenta, str(OUTPUT_FILE), hoja="SinCuentaDigital")

    print("\nResumen:")
    print(f"- Registros leídos desde Excel: {len(df_excel):,}")
    print(f"- Registros con cuenta digital: {len(df_match):,}")
    print(f"- Registros sin cuenta digital: {len(df_sin_cuenta):,}")
    print(f"- Archivo con convertidos: {MATCH_FILE}")
    print(f"- Archivo final sin convertidos: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
