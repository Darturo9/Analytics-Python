import argparse
import sys
import time

sys.path.insert(0, ".")

from core.db import run_query_file


COUNT_QUERY_PATH = "productos/Pago de servicios/Queries/ConteoUniversoConSinPagoServicios_Agrupado_Optimizado.sql"
BREAKDOWN_QUERY_PATH = (
    "productos/Pago de servicios/Queries/DesgloseUniversoPagoServicios_Agrupado_Optimizado.sql"
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Muestra en consola el conteo de clientes con/sin pago y desglose por tipo."
    )
    parser.add_argument(
        "--fecha-inicio",
        default="2025-01-01",
        help="Fecha minima de pagos a evaluar (formato YYYY-MM-DD).",
    )
    args = parser.parse_args()

    params = {"fecha_inicio": args.fecha_inicio}

    print("Calculando universo con/sin pagos de servicios (agrupado)...")
    print(f"- Filtro fecha inicio de pago: {args.fecha_inicio}")

    t0 = time.perf_counter()
    df_count = run_query_file(COUNT_QUERY_PATH, params=params)
    t1 = time.perf_counter()

    if df_count.empty:
        print("No se obtuvieron resultados para el conteo.")
        return

    row = df_count.iloc[0]
    universo = int(row["universo_clientes"])
    con_pago = int(row["clientes_con_pago"])
    sin_pago = int(row["clientes_sin_pago"])
    pct_con_pago = (con_pago / universo * 100.0) if universo else 0.0
    pct_sin_pago = (sin_pago / universo * 100.0) if universo else 0.0

    print("\nResumen clientes:")
    print(f"- Universo clientes: {universo:,}")
    print(f"- Clientes con pago: {con_pago:,} ({pct_con_pago:.2f}%)")
    print(f"- Clientes sin pago: {sin_pago:,} ({pct_sin_pago:.2f}%)")
    print(f"- Tiempo query conteo: {(t1 - t0):.2f}s")

    t2 = time.perf_counter()
    df_breakdown = run_query_file(BREAKDOWN_QUERY_PATH, params=params)
    t3 = time.perf_counter()

    print("\nDesglose por tipo/origen:")
    if df_breakdown.empty:
        print("(sin transacciones clasificadas)")
    else:
        print(df_breakdown.to_string(index=False))
    print(f"- Tiempo query desglose: {(t3 - t2):.2f}s")
    print(f"- Tiempo total: {(t3 - t0):.2f}s")


if __name__ == "__main__":
    main()
