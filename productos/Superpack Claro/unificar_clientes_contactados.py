import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, ".")

from core.utils import exportar_excel


BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "inputs"
EXPORT_DIR = BASE_DIR / "exports"

ARCHIVO_RTM = INPUT_DIR / "Clientes Contactados RTM.xlsx"
ARCHIVO_PAUTA = INPUT_DIR / "clientes Contactados promo Claro.xlsx"
ARCHIVO_SALIDA = EXPORT_DIR / "clientes_contactados_unificados_prioridad_rtm.xlsx"

PREFERRED_COLUMNS = (
    "codigo_cliente",
    "cod_cliente",
    "cif",
    "cliente",
    "codigo",
)


def cargar_excel(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")
    df = pd.read_excel(path, dtype=str)
    if df.empty:
        raise ValueError(f"El archivo no contiene filas: {path}")
    return df


def seleccionar_columna_cliente(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    cols_map = {str(c).strip().lower(): c for c in cols}

    for pref in PREFERRED_COLUMNS:
        if pref in cols_map:
            return cols_map[pref]

    for col in cols:
        low = str(col).strip().lower()
        if "cif" in low or "cliente" in low or ("codigo" in low and "producto" not in low):
            return col

    raise ValueError(
        "No se pudo detectar la columna de cliente automaticamente. "
        f"Columnas encontradas: {cols}"
    )


def normalizar_codigo_cliente(value: object) -> object:
    if pd.isna(value):
        return pd.NA

    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return pd.NA

    text = re.sub(r"\.0$", "", text)
    letter_match = re.search(r"[A-Za-z]", text)
    if letter_match:
        if letter_match.start() == 0:
            return pd.NA
        text = text[:letter_match.start()]

    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^0-9]", "", text)
    if text == "":
        return pd.NA

    return text.zfill(8)[-8:]


def preparar_base(df: pd.DataFrame, origen: str, columna_cliente: str) -> pd.DataFrame:
    out = pd.DataFrame(
        {
            "codigo_cliente_original": df[columna_cliente],
            "codigo_cliente": df[columna_cliente].apply(normalizar_codigo_cliente),
            "origen": origen,
        }
    )
    out = out.loc[out["codigo_cliente"].notna()].copy()
    out = out.drop_duplicates(subset=["codigo_cliente"], keep="first")
    return out


def unificar_clientes(df_rtm: pd.DataFrame, df_pauta: pd.DataFrame) -> pd.DataFrame:
    union = pd.concat([df_rtm, df_pauta], ignore_index=True)
    union["prioridad"] = union["origen"].map({"RTM": 0, "PAUTA": 1}).fillna(9)
    union = union.sort_values(["codigo_cliente", "prioridad", "codigo_cliente_original"])
    final = union.drop_duplicates(subset=["codigo_cliente"], keep="first").copy()
    final = final.sort_values(["origen", "codigo_cliente"], ascending=[True, True])
    return final[["codigo_cliente", "origen", "codigo_cliente_original"]]


def main() -> None:
    try:
        df_rtm_raw = cargar_excel(ARCHIVO_RTM)
        df_pauta_raw = cargar_excel(ARCHIVO_PAUTA)

        col_rtm = seleccionar_columna_cliente(df_rtm_raw)
        col_pauta = seleccionar_columna_cliente(df_pauta_raw)

        print(f"Archivo RTM: {ARCHIVO_RTM}")
        print(f"Columna cliente RTM: {col_rtm}")
        print(f"Filas RTM: {len(df_rtm_raw):,}")
        print(f"Archivo PAUTA: {ARCHIVO_PAUTA}")
        print(f"Columna cliente PAUTA: {col_pauta}")
        print(f"Filas PAUTA: {len(df_pauta_raw):,}")

        base_rtm = preparar_base(df_rtm_raw, "RTM", col_rtm)
        base_pauta = preparar_base(df_pauta_raw, "PAUTA", col_pauta)

        comunes = set(base_rtm["codigo_cliente"]).intersection(set(base_pauta["codigo_cliente"]))
        final = unificar_clientes(base_rtm, base_pauta)

        exportar_excel(final, str(ARCHIVO_SALIDA), hoja="clientes_unificados")

        total_final = len(final)
        total_rtm_final = int((final["origen"] == "RTM").sum())
        total_pauta_final = int((final["origen"] == "PAUTA").sum())

        print("\n===== RESUMEN =====")
        print(f"Clientes unicos RTM: {len(base_rtm):,}")
        print(f"Clientes unicos PAUTA: {len(base_pauta):,}")
        print(f"Clientes repetidos en ambos: {len(comunes):,}")
        print(f"Clientes unificados finales: {total_final:,}")
        print(f"Final con origen RTM: {total_rtm_final:,}")
        print(f"Final con origen PAUTA: {total_pauta_final:,}")
        print("===================")
        print(f"\nArchivo generado: {ARCHIVO_SALIDA}")

    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
