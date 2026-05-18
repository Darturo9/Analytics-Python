from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError


PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import run_query_file


RUTA_QUERY = (
    PROJECT_ROOT
    / "productos"
    / "Fondeo_CD"
    / "reporte_quincena"
    / "queries"
    / "cuentas_creadas_vs_fondeadas_quincena.sql"
)

# Configuracion editable (sin argumentos en terminal).
CONFIG_ANIO = 2026
CONFIG_MES = 5
CONFIG_DIA_INICIO = 1
CONFIG_DIA_FIN = 15


def validar_rango_quincena(anio: int, mes: int, dia_inicio: int, dia_fin: int) -> tuple[date, date]:
    if mes < 1 or mes > 12:
        raise ValueError("CONFIG_MES debe estar entre 1 y 12.")
    if dia_inicio < 1:
        raise ValueError("CONFIG_DIA_INICIO debe ser mayor o igual a 1.")
    if dia_fin < dia_inicio:
        raise ValueError("CONFIG_DIA_FIN debe ser mayor o igual a CONFIG_DIA_INICIO.")

    fecha_inicio = date(anio, mes, dia_inicio)

    if mes == 12:
        primer_dia_mes_siguiente = date(anio + 1, 1, 1)
    else:
        primer_dia_mes_siguiente = date(anio, mes + 1, 1)
    ultimo_dia_mes = primer_dia_mes_siguiente - timedelta(days=1)

    if dia_fin > ultimo_dia_mes.day:
        raise ValueError(
            f"CONFIG_DIA_FIN ({dia_fin}) excede el ultimo dia del mes ({ultimo_dia_mes.day}) para {anio}-{mes:02d}."
        )

    fecha_fin_quincena_exclusiva = date(anio, mes, dia_fin) + timedelta(days=1)
    return fecha_inicio, fecha_fin_quincena_exclusiva


def cargar_datos(params: dict) -> pd.DataFrame:
    df = run_query_file(str(RUTA_QUERY), params=params)
    df.columns = [str(c) for c in df.columns]
    for col in [
        "cuentas_creadas_quincena",
        "cuentas_fondeadas_periodo",
        "cuentas_sin_fondear_periodo",
        "tasa_fondeo_pct",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def imprimir_reporte(df: pd.DataFrame) -> None:
    if df.empty:
        print("No se devolvieron registros para la configuracion indicada.")
        return

    row = df.iloc[0]
    periodo_quincena = str(row.get("periodo_quincena", "N/D"))
    periodo_fondeo = str(row.get("periodo_fondeo", "N/D"))
    creadas = int(row.get("cuentas_creadas_quincena", 0) or 0)
    fondeadas = int(row.get("cuentas_fondeadas_periodo", 0) or 0)
    sin_fondear = int(row.get("cuentas_sin_fondear_periodo", 0) or 0)
    tasa = float(row.get("tasa_fondeo_pct", 0.0) or 0.0)

    print("=" * 84)
    print("FONDEO CUENTA DIGITAL - REPORTE QUINCENAL CONFIGURABLE")
    print("=" * 84)
    print(f"Periodo quincena (creacion):                          {periodo_quincena}")
    print(f"Periodo fondeo (mismo rango):                         {periodo_fondeo}")
    print(f"Cuentas creadas en quincena:                          {creadas:>10,}")
    print(f"Cuentas fondeadas >=1 vez en el mes:                  {fondeadas:>10,}")
    print(f"Cuentas sin fondear en el mes:                        {sin_fondear:>10,}")
    print(f"Tasa de fondeo sobre quincena:                        {tasa:>9.2f}%")
    print("=" * 84)


def main() -> None:
    try:
        fecha_inicio_quincena, fecha_fin_quincena_exclusiva = validar_rango_quincena(
            CONFIG_ANIO, CONFIG_MES, CONFIG_DIA_INICIO, CONFIG_DIA_FIN
        )
        fecha_inicio_fondeo = fecha_inicio_quincena
        fecha_fin_fondeo_exclusiva = fecha_fin_quincena_exclusiva

        params = {
            "fecha_inicio_quincena": fecha_inicio_quincena.isoformat(),
            "fecha_fin_quincena_exclusiva": fecha_fin_quincena_exclusiva.isoformat(),
            "fecha_inicio_fondeo": fecha_inicio_fondeo.isoformat(),
            "fecha_fin_fondeo_exclusiva": fecha_fin_fondeo_exclusiva.isoformat(),
            "periodo_quincena": (
                f"{fecha_inicio_quincena.isoformat()} a "
                f"{(fecha_fin_quincena_exclusiva - timedelta(days=1)).isoformat()}"
            ),
            "periodo_fondeo": (
                f"{fecha_inicio_fondeo.isoformat()} a "
                f"{(fecha_fin_fondeo_exclusiva - timedelta(days=1)).isoformat()}"
            ),
        }

        print(f"Cargando query: {RUTA_QUERY}")
        print(
            "Configuracion activa -> "
            f"anio={CONFIG_ANIO}, mes={CONFIG_MES}, dia_inicio={CONFIG_DIA_INICIO}, dia_fin={CONFIG_DIA_FIN}"
        )
        df = cargar_datos(params)
        imprimir_reporte(df)

    except SQLAlchemyError as exc:
        print(f"[ERROR] No se pudo ejecutar la query en SQL Server: {exc}")
        raise SystemExit(1) from exc
    except Exception as exc:
        print(f"[ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
