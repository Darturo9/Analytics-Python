import sys

sys.path.insert(0, ".")

from core.db import run_query_file


QUERY_PATH = "productos/Pago de servicios/Queries/ConteoUniversoPagosServicios_parametros.sql"


def ejecutar_escenario(etiqueta: str, fecha_inicio: str | None) -> None:
    params = {"fecha_inicio": fecha_inicio}
    df = run_query_file(QUERY_PATH, params=params)
    if df.empty:
        print(f"{etiqueta}: sin resultados")
        return

    row = df.iloc[0]
    print(f"\n{etiqueta}")
    print(f"- Filtro fecha inicio: {fecha_inicio if fecha_inicio else 'SIN FECHA'}")
    print(f"- Clientes universo: {int(row['clientes_universo']):,}")
    print(
        "- Clientes con pago (parametros PagosdeServicios): "
        f"{int(row['clientes_con_pago_parametros_pagosdeservicios']):,}"
    )
    print(
        "- Clientes sin pago (parametros PagosdeServicios): "
        f"{int(row['clientes_sin_pago_parametros_pagosdeservicios']):,}"
    )


def main() -> None:
    print("Calculando conteos (parametros de PagosdeServicios.sql)...")
    escenarios = [
        ("Escenario 1", None),
        ("Escenario 2", "2025-01-01"),
        ("Escenario 3", "2026-01-01"),
    ]
    for etiqueta, fecha_inicio in escenarios:
        ejecutar_escenario(etiqueta, fecha_inicio)


if __name__ == "__main__":
    main()
