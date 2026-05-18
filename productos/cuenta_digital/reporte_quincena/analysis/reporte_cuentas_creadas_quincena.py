import argparse
import sys
from datetime import date, timedelta

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query_file


QUERY_PATH = "productos/cuenta_digital/reporte_quincena/queries/conteo_cuentas_creadas_quincena.sql"

# Configuracion editable (sin usar argumentos en terminal).
CONFIG_ANIO = 2026
CONFIG_MES = 5
CONFIG_DIA_INICIO = 1
CONFIG_DIA_FIN = 15


def construir_error_amigable(exc: Exception) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()

    if "permission was denied" in lower:
        return "[ERROR] Permiso denegado al consultar SQL Server. Solicita permiso SELECT al DBA."
    if "login timeout expired" in lower or "could not open a connection" in lower:
        return "[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales."
    return f"[ERROR] Fallo ejecutando la consulta: {raw}"


def validar_rango(anio: int, mes: int, dia_inicio: int, dia_fin: int) -> tuple[date, date]:
    if mes < 1 or mes > 12:
        raise ValueError("El mes debe estar entre 1 y 12.")
    if dia_inicio < 1:
        raise ValueError("El dia-inicio debe ser mayor o igual a 1.")
    if dia_fin < dia_inicio:
        raise ValueError("El dia-fin debe ser mayor o igual al dia-inicio.")

    fecha_inicio = date(anio, mes, dia_inicio)

    if mes == 12:
        primer_dia_mes_siguiente = date(anio + 1, 1, 1)
    else:
        primer_dia_mes_siguiente = date(anio, mes + 1, 1)
    ultimo_dia_mes = primer_dia_mes_siguiente - timedelta(days=1)

    if dia_fin > ultimo_dia_mes.day:
        raise ValueError(
            f"El dia-fin ({dia_fin}) excede el ultimo dia del mes ({ultimo_dia_mes.day}) para {anio}-{mes:02d}."
        )

    fecha_fin_inclusiva = date(anio, mes, dia_fin)
    fecha_fin_exclusiva = fecha_fin_inclusiva + timedelta(days=1)
    return fecha_inicio, fecha_fin_exclusiva


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Conteo quincenal de cuentas creadas de Cuenta Digital."
    )
    parser.add_argument(
        "--anio",
        type=int,
        default=CONFIG_ANIO,
        help=f"Anio del reporte (default desde codigo: {CONFIG_ANIO}).",
    )
    parser.add_argument(
        "--mes",
        type=int,
        default=CONFIG_MES,
        help=f"Mes del reporte 1-12 (default desde codigo: {CONFIG_MES}).",
    )
    parser.add_argument(
        "--dia-inicio",
        type=int,
        default=CONFIG_DIA_INICIO,
        help=f"Dia inicial inclusivo (default desde codigo: {CONFIG_DIA_INICIO}).",
    )
    parser.add_argument(
        "--dia-fin",
        type=int,
        default=CONFIG_DIA_FIN,
        help=f"Dia final inclusivo (default desde codigo: {CONFIG_DIA_FIN}).",
    )
    args = parser.parse_args()

    try:
        fecha_inicio, fecha_fin_exclusiva = validar_rango(
            anio=args.anio,
            mes=args.mes,
            dia_inicio=args.dia_inicio,
            dia_fin=args.dia_fin,
        )

        params = {
            "fecha_inicio": fecha_inicio.isoformat(),
            "fecha_fin_exclusiva": fecha_fin_exclusiva.isoformat(),
        }
        df = run_query_file(QUERY_PATH, params=params)

        periodo = f"{fecha_inicio.isoformat()} a {(fecha_fin_exclusiva - timedelta(days=1)).isoformat()}"
        print("\n===== CUENTA DIGITAL: REPORTE QUINCENA =====")
        print(f"Periodo: {periodo}")

        if df.empty:
            print("No se encontraron cuentas creadas en el periodo indicado.")
            print("============================================")
            return

        tabla = df.copy()
        tabla["fecha_apertura"] = pd.to_datetime(tabla["fecha_apertura"]).dt.strftime("%Y-%m-%d")
        tabla["cuentas_creadas"] = tabla["cuentas_creadas"].astype(int)

        total_cuentas = int(tabla["cuentas_creadas"].sum())
        tabla_impresion = tabla.copy()
        tabla_impresion["cuentas_creadas"] = tabla_impresion["cuentas_creadas"].map(lambda x: f"{x:,}")

        print("\nDetalle diario:")
        print(tabla_impresion.to_string(index=False))
        print("--------------------------------------------")
        print(f"Total cuentas creadas: {total_cuentas:,}")
        print("============================================")

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except ValueError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
