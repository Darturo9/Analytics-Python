import sys

sys.path.insert(0, ".")

from core.db import run_query_file


QUERY_PATH = "productos/Pago de servicios/Queries/ConteoUniversoPagosServicios.sql"


def main() -> None:
    print("Calculando conteos de pagos de servicios sobre universo de clientes...")
    df = run_query_file(QUERY_PATH)
    if df.empty:
        print("No se obtuvieron resultados.")
        return

    row = df.iloc[0]
    print(f"Clientes universo: {int(row['clientes_universo']):,}")
    print(f"Clientes con pago (todos los canales): {int(row['clientes_con_pago_todos_canales']):,}")
    print(f"Clientes sin pago (todos los canales): {int(row['clientes_sin_pago_todos_canales']):,}")
    print(f"Clientes con pago (solo App): {int(row['clientes_con_pago_app']):,}")
    print(f"Clientes sin pago (solo App): {int(row['clientes_sin_pago_app']):,}")


if __name__ == "__main__":
    main()
