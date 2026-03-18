import sys

sys.path.insert(0, ".")

from core.db import run_query_file


QUERY_PATH = "productos/Pago de servicios/Queries/ConteoUniversoPagosServicios_parametros.sql"


def main() -> None:
    print("Calculando conteos (parametros de PagosdeServicios.sql)...")
    df = run_query_file(QUERY_PATH)
    if df.empty:
        print("No se obtuvieron resultados.")
        return

    row = df.iloc[0]
    print(f"Clientes universo: {int(row['clientes_universo']):,}")
    print(
        "Clientes con pago (parametros PagosdeServicios): "
        f"{int(row['clientes_con_pago_parametros_pagosdeservicios']):,}"
    )
    print(
        "Clientes sin pago (parametros PagosdeServicios): "
        f"{int(row['clientes_sin_pago_parametros_pagosdeservicios']):,}"
    )


if __name__ == "__main__":
    main()
