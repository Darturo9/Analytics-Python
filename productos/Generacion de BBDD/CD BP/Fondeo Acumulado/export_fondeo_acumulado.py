import argparse
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query_file
from core.utils import exportar_excel


BASE_DIR = Path(__file__).resolve().parent
QUERY_PATH = BASE_DIR / "historico_fondeo.sql"
EXPORT_DIR = BASE_DIR / "exports"

EXPECTED_COLUMNS = [
    "fecha_informacion",
    "cuentas_reportadas_dia",
    "cuentas_con_fondos_dia",
    "cuentas_acumuladas_con_fondos",
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


def mes_anterior_completo(hoy: date | None = None) -> tuple[date, date]:
    if hoy is None:
        hoy = date.today()
    primer_dia_mes_actual = hoy.replace(day=1)
    fin = primer_dia_mes_actual - timedelta(days=1)
    inicio = fin.replace(day=1)
    return inicio, fin


def parsear_fecha(valor: str, nombre: str) -> date:
    try:
        return date.fromisoformat(valor)
    except ValueError as exc:
        raise ValueError(f"{nombre} debe tener formato YYYY-MM-DD. Valor recibido: {valor}") from exc


def resolver_rango(args: argparse.Namespace) -> tuple[date, date]:
    if args.fecha_inicio and args.fecha_fin:
        inicio = parsear_fecha(args.fecha_inicio, "--fecha-inicio")
        fin = parsear_fecha(args.fecha_fin, "--fecha-fin")
    elif args.fecha_inicio or args.fecha_fin:
        raise ValueError("Debes enviar ambos parametros: --fecha-inicio y --fecha-fin.")
    else:
        inicio, fin = mes_anterior_completo()

    if inicio > fin:
        raise ValueError("El rango es invalido: fecha_inicio no puede ser mayor que fecha_fin.")
    return inicio, fin


def validar_columnas(df: pd.DataFrame) -> None:
    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(
            f"La consulta no devolvio las columnas esperadas: {missing_cols}. "
            f"Columnas recibidas: {list(df.columns)}"
        )


def preparar_resultado(df: pd.DataFrame) -> pd.DataFrame:
    df = df[EXPECTED_COLUMNS].copy()
    df["fecha_informacion"] = pd.to_datetime(df["fecha_informacion"], errors="coerce").dt.date
    for col in EXPECTED_COLUMNS[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df = df.sort_values("fecha_informacion").reset_index(drop=True)
    return df


def construir_salida(inicio: date, fin: date, output: str | None) -> Path:
    if output:
        ruta = Path(output)
        if not ruta.is_absolute():
            ruta = Path.cwd() / ruta
        return ruta
    nombre = f"BBDD_CD_BP_FONDEO_ACUMULADO_{inicio:%Y%m%d}_{fin:%Y%m%d}.xlsx"
    return EXPORT_DIR / nombre


def imprimir_resumen(df: pd.DataFrame, inicio: date, fin: date, salida: Path) -> None:
    print(f"- Rango evaluado: {inicio} a {fin}")
    print(f"- Dias en salida: {len(df.index):,}")

    if df.empty:
        print("- No se encontraron registros para ese rango.")
        return

    ultimo = df.iloc[-1]
    max_acumulado = int(df["cuentas_acumuladas_con_fondos"].max())
    print(f"- Maximo acumulado del mes: {max_acumulado:,}")
    print(
        "- Ultimo dia -> "
        f"reportadas: {int(ultimo['cuentas_reportadas_dia']):,}, "
        f"con fondos dia: {int(ultimo['cuentas_con_fondos_dia']):,}, "
        f"acumulado: {int(ultimo['cuentas_acumuladas_con_fondos']):,}"
    )
    print(f"- Archivo generado: {salida}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Genera historico diario de cuentas con fondos y acumulado mensual "
            "para Cuenta Digital (PRCODP=1, PRSUBP=51)."
        )
    )
    parser.add_argument("--fecha-inicio", help="Fecha inicio (YYYY-MM-DD).")
    parser.add_argument("--fecha-fin", help="Fecha fin (YYYY-MM-DD).")
    parser.add_argument(
        "--output",
        help="Ruta de salida .xlsx. Si no se envia, usa exports/ con nombre por rango.",
    )
    args = parser.parse_args()

    try:
        inicio, fin = resolver_rango(args)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)

    params = {"fecha_inicio": inicio.isoformat(), "fecha_fin": fin.isoformat()}
    print(f"Ejecutando query: {QUERY_PATH}")
    print(f"Parametros: {params}")

    try:
        df = run_query_file(str(QUERY_PATH), params=params)
    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)

    df.columns = [str(c) for c in df.columns]
    validar_columnas(df)
    df_final = preparar_resultado(df)

    salida = construir_salida(inicio, fin, args.output)
    exportar_excel(df_final, str(salida), hoja="Fondeo_Acumulado")
    imprimir_resumen(df_final, inicio, fin, salida)


if __name__ == "__main__":
    main()
