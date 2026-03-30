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
QUERY_PATH = BASE_DIR / "Cuenta Digital.sql"
EXPORT_DIR = BASE_DIR / "exports"
OUTPUT_LIMITADA = EXPORT_DIR / "BBDD_CD_BP_LIMITADA.xlsx"
OUTPUT_COMPLETA = EXPORT_DIR / "BBDD_CD_BP_COMPLETA.xlsx"
MAX_POR_GENERACION = 4000
RANDOM_SEED = 42

EXPECTED_COLUMNS = [
    "cif",
    "nombre_completo",
    "numero_celular",
    "correo",
    "segmentacion_generacional",
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


def normalizar_cif(series: pd.Series) -> pd.Series:
    codigos = (
        series.astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    return codigos.apply(lambda codigo: codigo.zfill(8) if codigo else codigo)


def validar_columnas(df: pd.DataFrame) -> None:
    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"La consulta no devolvio las columnas esperadas: {missing_cols}. "
            f"Columnas recibidas: {list(df.columns)}"
        )


def preparar_base(df: pd.DataFrame) -> pd.DataFrame:
    df = df[EXPECTED_COLUMNS].copy()
    df = df.where(pd.notna(df), "")

    df["cif"] = normalizar_cif(df["cif"])
    df["nombre_completo"] = df["nombre_completo"].astype(str).str.strip()
    df["numero_celular"] = df["numero_celular"].astype(str).str.strip()
    df["correo"] = df["correo"].astype(str).str.strip().str.lower()
    df["segmentacion_generacional"] = df["segmentacion_generacional"].astype(str).str.strip()

    duplicated_rows = int(df.duplicated(subset=["cif"]).sum())
    if duplicated_rows > 0:
        df = df.drop_duplicates(subset=["cif"], keep="first")
        print(f"- Duplicados removidos por cif: {duplicated_rows:,}")

    return df


def limitar_por_generacion(df: pd.DataFrame, random_seed: int) -> pd.DataFrame:
    def take_group(group: pd.DataFrame) -> pd.DataFrame:
        if len(group.index) <= MAX_POR_GENERACION:
            return group
        return group.sample(n=MAX_POR_GENERACION, random_state=random_seed)

    limitado = (
        df.groupby("segmentacion_generacional", group_keys=False)
        .apply(take_group)
        .reset_index(drop=True)
    )
    return limitado


def imprimir_resumen(df_completa: pd.DataFrame, df_limitada: pd.DataFrame) -> None:
    print(f"- Total base completa: {len(df_completa.index):,}")
    print(f"- Total base limitada: {len(df_limitada.index):,}")
    print("- Conteo por generacion (base limitada):")

    conteo = (
        df_limitada.groupby("segmentacion_generacional")["cif"]
        .count()
        .sort_values(ascending=False)
    )
    for generacion, total in conteo.items():
        print(f"  * {generacion}: {int(total):,}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Ejecuta Cuenta Digital.sql y genera dos Excels: "
            "BBDD_CD_BP_LIMITADA y BBDD_CD_BP_COMPLETA."
        )
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=RANDOM_SEED,
        help="Semilla para muestreo aleatorio en la base limitada. Default: 42.",
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
    validar_columnas(df)
    df_completa = preparar_base(df)

    df_limitada = limitar_por_generacion(df_completa, random_seed=args.seed)

    exportar_excel(df_completa, str(OUTPUT_COMPLETA), hoja="BBDD_CD_BP_COMPLETA")
    exportar_excel(df_limitada, str(OUTPUT_LIMITADA), hoja="BBDD_CD_BP_LIMITADA")

    imprimir_resumen(df_completa, df_limitada)
    print(f"- Archivo completo: {OUTPUT_COMPLETA}")
    print(f"- Archivo limitado: {OUTPUT_LIMITADA}")


if __name__ == "__main__":
    main()
