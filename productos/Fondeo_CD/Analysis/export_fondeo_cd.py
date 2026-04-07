import argparse
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query_file
from core.utils import exportar_excel_multi


BASE_DIR = Path(__file__).resolve().parents[1]
QUERY_DIR = BASE_DIR / "Queries"
EXPORT_DIR = BASE_DIR / "exports"
DEFAULT_FECHA_INICIO = date(2026, 3, 1)
DEFAULT_FECHA_FIN = date(2026, 3, 31)

QUERY_RESUMEN = QUERY_DIR / "FondeoResumenCuentas.sql"
QUERY_DIARIO = QUERY_DIR / "FondeoDiaro.sql"


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
        inicio, fin = DEFAULT_FECHA_INICIO, DEFAULT_FECHA_FIN

    if inicio > fin:
        raise ValueError("El rango es invalido: fecha_inicio no puede ser mayor que fecha_fin.")
    return inicio, fin


def preparar_df_resumen(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "fecha_apertura" in df.columns:
        df["fecha_apertura"] = pd.to_datetime(df["fecha_apertura"], errors="coerce").dt.date
    if "fecha_primer_fondeo" in df.columns:
        df["fecha_primer_fondeo"] = pd.to_datetime(df["fecha_primer_fondeo"], errors="coerce").dt.date

    for col in [
        "saldo_maximo_mes",
        "saldo_promedio_maximo_mes",
        "dias_con_fondos",
        "tuvo_fondos_mes",
        "dias_a_primer_fondeo",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def preparar_df_diario(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "fecha_informacion" in df.columns:
        df["fecha_informacion"] = pd.to_datetime(df["fecha_informacion"], errors="coerce").dt.date
    for col in [
        "cuentas_creadas_periodo",
        "cuentas_reportadas_dia",
        "cuentas_con_fondos_dia",
        "cuentas_con_primer_fondeo_dia",
        "cuentas_acumuladas_con_fondos",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df.sort_values("fecha_informacion").reset_index(drop=True)


def construir_kpis(df_resumen: pd.DataFrame, df_diario: pd.DataFrame) -> pd.DataFrame:
    total_cuentas = int(df_resumen["cuenta"].nunique()) if "cuenta" in df_resumen.columns else len(df_resumen)
    cuentas_fondeadas = int(df_resumen.loc[df_resumen["tuvo_fondos_mes"] == 1, "cuenta"].nunique()) if "tuvo_fondos_mes" in df_resumen.columns else 0
    tasa_fondeo = (cuentas_fondeadas / total_cuentas * 100) if total_cuentas > 0 else 0.0
    promedio_dias = float(
        pd.to_numeric(
            df_resumen.loc[df_resumen["tuvo_fondos_mes"] == 1, "dias_a_primer_fondeo"],
            errors="coerce",
        ).dropna().mean()
    ) if total_cuentas > 0 else float("nan")

    ultimo_acumulado = int(df_diario["cuentas_acumuladas_con_fondos"].max()) if not df_diario.empty else 0

    return pd.DataFrame(
        [
            {
                "total_cuentas_creadas_periodo": total_cuentas,
                "cuentas_con_fondos_periodo": cuentas_fondeadas,
                "tasa_fondeo_porcentaje": round(tasa_fondeo, 2),
                "promedio_dias_a_primer_fondeo": round(promedio_dias, 2) if pd.notna(promedio_dias) else None,
                "acumulado_maximo_con_fondos": ultimo_acumulado,
            }
        ]
    )


def construir_ruta_salida(inicio: date, fin: date, output: str | None) -> Path:
    if output:
        path = Path(output)
        if not path.is_absolute():
            path = Path.cwd() / path
        return path
    nombre = f"BBDD_FONDEO_CD_{inicio:%Y%m%d}_{fin:%Y%m%d}.xlsx"
    return EXPORT_DIR / nombre


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Exporta dataset ejecutivo de fondeo para cuentas digitales "
            "abiertas en el periodo (resumen por cuenta + historico diario + KPIs). "
            "Default: marzo 2026."
        )
    )
    parser.add_argument("--fecha-inicio", help="Fecha inicio (YYYY-MM-DD).")
    parser.add_argument("--fecha-fin", help="Fecha fin (YYYY-MM-DD).")
    parser.add_argument("--output", help="Ruta del Excel de salida.")
    parser.add_argument(
        "--modo",
        choices=["completo", "resumen"],
        default="resumen",
        help=(
            "completo: incluye historico diario. "
            "resumen: exporta KPIs + ResumenCuentas (mas rapido). Default: resumen."
        ),
    )
    args = parser.parse_args()

    try:
        inicio, fin = resolver_rango(args)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)

    params = {"fecha_inicio": inicio.isoformat(), "fecha_fin": fin.isoformat()}
    print(f"Ejecutando query resumen: {QUERY_RESUMEN}")
    if args.modo == "completo":
        print(f"Ejecutando query diario: {QUERY_DIARIO}")
    else:
        print("Modo resumen: se omite query diaria para acelerar.")
    print(f"Parametros: {params}")

    try:
        df_resumen = run_query_file(str(QUERY_RESUMEN), params=params)
        if args.modo == "completo":
            df_diario = run_query_file(str(QUERY_DIARIO), params=params)
        else:
            df_diario = pd.DataFrame(
                columns=[
                    "fecha_informacion",
                    "cuentas_creadas_periodo",
                    "cuentas_reportadas_dia",
                    "cuentas_con_fondos_dia",
                    "cuentas_con_primer_fondeo_dia",
                    "cuentas_acumuladas_con_fondos",
                ]
            )
    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)

    df_resumen.columns = [str(c) for c in df_resumen.columns]
    df_diario.columns = [str(c) for c in df_diario.columns]

    df_resumen = preparar_df_resumen(df_resumen)
    df_diario = preparar_df_diario(df_diario)
    df_kpis = construir_kpis(df_resumen, df_diario)

    output_path = construir_ruta_salida(inicio, fin, args.output)
    if args.modo == "completo":
        sheets = {"KPIs": df_kpis, "ResumenCuentas": df_resumen, "HistoricoDiario": df_diario}
    else:
        sheets = {"KPIs": df_kpis, "ResumenCuentas": df_resumen}

    exportar_excel_multi(sheets, str(output_path))

    print(f"- Rango evaluado: {inicio} a {fin}")
    print(f"- Cuentas creadas en periodo: {int(df_kpis.loc[0, 'total_cuentas_creadas_periodo']):,}")
    print(f"- Cuentas con fondos en periodo: {int(df_kpis.loc[0, 'cuentas_con_fondos_periodo']):,}")
    print(f"- Tasa de fondeo: {float(df_kpis.loc[0, 'tasa_fondeo_porcentaje']):.2f}%")
    print(f"- Modo de exportacion: {args.modo}")
    print(f"- Archivo generado: {output_path}")


if __name__ == "__main__":
    main()
