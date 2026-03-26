import argparse
import re
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.config import DB_DRIVER, DB_PASS, DB_SERVER, DB_USER
from core.utils import exportar_excel, exportar_excel_multi


BASE_DIR = Path("productos/Generacion de BBDD/SV CU")
EXPORT_DIR = BASE_DIR / "exports"
DB_NAME_SV_CU = "DWHSV"

QUERY_BY_TYPE = {
    "email": BASE_DIR / "Base Email.sql",
    "sms": BASE_DIR / "Base SMS.sql",
}

EXPECTED_COLUMNS = ["codigo_cliente", "nombre_cliente", "correo"]


class ExportError(Exception):
    """Error controlado para mostrar mensajes amigables en consola."""


def normalizar_codigo_cliente(series: pd.Series) -> pd.Series:
    codigos = (
        series.astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    return codigos.apply(lambda codigo: codigo.zfill(8) if codigo else codigo)


def build_output_path(tipo: str, output_arg: str) -> Path:
    if output_arg.strip():
        base_output = Path(output_arg.strip())
        if base_output.suffix.lower() == ".xlsx":
            return base_output
        return base_output.with_name(f"{base_output.name}.xlsx")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return EXPORT_DIR / f"base_{tipo}_{timestamp}.xlsx"


def get_engine_sv_cu():
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME_SV_CU};"
        f"UID={DB_USER};"
        f"PWD={DB_PASS};"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)


def run_query_file_sv_cu(path: Path) -> pd.DataFrame:
    sql = path.read_text(encoding="utf-8")
    engine = get_engine_sv_cu()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def extraer_detalle_permiso(error_msg: str):
    match = re.search(
        r"permission was denied on the object '([^']+)', database '([^']+)', schema '([^']+)'",
        error_msg,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return {
        "objeto": match.group(1),
        "base": match.group(2),
        "esquema": match.group(3),
    }


def construir_error_amigable(exc: Exception, tipo: str) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()

    if "permission was denied" in lower:
        detalle = extraer_detalle_permiso(raw)
        if detalle:
            return (
                f"[ERROR] Permiso denegado al ejecutar base '{tipo}' en "
                f"{detalle['base']}.{detalle['esquema']}.{detalle['objeto']}. "
                "Solicita permiso SELECT al DBA sobre ese objeto."
            )
        return (
            f"[ERROR] Permiso denegado al ejecutar base '{tipo}'. "
            "Solicita al DBA permisos SELECT en los objetos requeridos."
        )

    if "login timeout expired" in lower or "could not open a connection" in lower:
        return (
            f"[ERROR] No se pudo conectar a SQL Server usando la base {DB_NAME_SV_CU}. "
            "Verifica red/VPN, servidor y credenciales."
        )

    return f"[ERROR] Fallo al ejecutar base '{tipo}': {raw}"


def preparar_dataframe_tipo(tipo: str) -> pd.DataFrame:
    query_path = QUERY_BY_TYPE[tipo]
    print(f"Ejecutando query ({tipo}) en {DB_NAME_SV_CU}: {query_path}")

    try:
        df = run_query_file_sv_cu(query_path)
    except SQLAlchemyError as exc:
        raise ExportError(construir_error_amigable(exc, tipo)) from exc
    except Exception as exc:
        raise ExportError(construir_error_amigable(exc, tipo)) from exc

    if df.empty:
        print(f"No se obtuvieron registros para exportar en tipo '{tipo}'.")
        return df

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

    return df


def export_tipo(tipo: str, output_arg: str = "") -> None:
    df = preparar_dataframe_tipo(tipo)
    if df.empty:
        return

    output_path = build_output_path(tipo, output_arg)
    exportar_excel(df, str(output_path), hoja=f"Base_{tipo.upper()}")

    print(f"- Total filas exportadas ({tipo}): {len(df.index):,}")
    print(f"- Archivo generado ({tipo}): {output_path}")


def export_ambos(output_arg: str = "") -> None:
    df_email = preparar_dataframe_tipo("email")
    if df_email.empty:
        raise ExportError("[ERROR] La base 'email' no devolvio datos. Proceso cancelado.")

    df_sms = preparar_dataframe_tipo("sms")
    if df_sms.empty:
        raise ExportError("[ERROR] La base 'sms' no devolvio datos. Proceso cancelado.")

    output_path = build_output_path("ambos", output_arg)
    exportar_excel_multi(
        {
            "Base_EMAIL": df_email,
            "Base_SMS": df_sms,
        },
        str(output_path),
    )

    print(f"- Total filas exportadas (email): {len(df_email.index):,}")
    print(f"- Total filas exportadas (sms): {len(df_sms.index):,}")
    print(f"- Archivo generado (ambos): {output_path}")


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
        help="Ruta de salida del Excel (opcional). En modo ambos genera un solo archivo con dos hojas.",
    )
    args = parser.parse_args()

    try:
        if args.tipo == "ambos":
            export_ambos(args.output)
            return

        export_tipo(args.tipo, args.output)
    except ExportError as exc:
        print(str(exc))
        print("[ERROR] Proceso cancelado.")
        sys.exit(1)


if __name__ == "__main__":
    main()
