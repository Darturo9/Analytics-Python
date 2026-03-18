import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import pandas as pd

sys.path.insert(0, ".")

from core.db import run_query


@dataclass
class BlockResult:
    name: str
    ok: bool
    seconds: float
    rows: int | None = None
    df: pd.DataFrame | None = None
    error: str | None = None


def build_queries() -> dict[str, str]:
    return {
        "clientes_base": """
            SELECT
                LTRIM(RTRIM(c.cldoc)) AS codigo_cliente,
                RIGHT('00000000' + LTRIM(RTRIM(c.cldoc)), 8) AS padded_codigo_cliente,
                c.clnomb AS nombre_cliente
            FROM dw_cif_clientes c
            WHERE c.estatu = 'A'
              AND c.cltipe = 'N'
              AND c.clclco <> 10
              AND COALESCE(c.dw_sms_cnt, 0) >= 1
              AND c.cldoc IS NOT NULL
              AND EXISTS (
                    SELECT 1
                    FROM dw_bel_ibclie ibc
                    WHERE LTRIM(RTRIM(ibc.clccli)) = LTRIM(RTRIM(c.cldoc))
                      AND ibc.clstat = 'A'
              )
              AND EXISTS (
                    SELECT 1
                    FROM dw_bel_ibuser ibu
                    WHERE LTRIM(RTRIM(ibu.clccli)) = LTRIM(RTRIM(c.cldoc))
                      AND ibu.usstat = 'A'
              )
              AND EXISTS (
                    SELECT 1
                    FROM dw_dep_depositos d
                    WHERE LTRIM(RTRIM(d.cldoc)) = LTRIM(RTRIM(c.cldoc))
                      AND d.dw_estado_cuenta = 'ACTIVA'
              );
        """,
        "pagadores": """
            WITH pagos_toda_transaccion AS (
                SELECT DISTINCT
                    RIGHT('00000000' + RTRIM(LTRIM(txn_bxi.clccli)), 8) AS cif_toda_transaccion
                FROM dw_bel_ibjour txn_bxi
                INNER JOIN dw_bel_ibserv descripcion_servicio
                    ON txn_bxi.secode = descripcion_servicio.secode
                   AND descripcion_servicio.inserv = 'APP'
                   AND descripcion_servicio.tiserv = 'O'
                   AND descripcion_servicio.seuspr = 'S'
                   AND descripcion_servicio.secode IN (
                        'ap-pagclar','app-pagcla','ope-rccl','app-reccla',
                        'app-ptigo','pag-tigo','app-rectig','ope-rctg',
                        'app-paenee','pag-enee','app-asps','pag-asps',
                        'app-achtrf','app-trach','app-transf','app-transh','app-transt',
                        'app-tcpago','app-pagotc','app-paptmo','pago-ptmos','ope-psarah'
                   )
                WHERE txn_bxi.dw_fecha_journal >= :fecha_inicio
                  AND txn_bxi.dw_fecha_journal <  :fecha_fin
                  AND txn_bxi.jostat = 1
                  AND txn_bxi.josecu = 1
                  AND txn_bxi.jovalo > 0

                UNION ALL

                SELECT DISTINCT
                    RIGHT(
                        '00000000' + LTRIM(RTRIM(
                            CASE
                                WHEN datos_pago.spinus IS NULL THEN NULL
                                WHEN PATINDEX('%[A-Za-z]%', datos_pago.spinus) > 1
                                    THEN LEFT(datos_pago.spinus, PATINDEX('%[A-Za-z]%', datos_pago.spinus) - 1)
                                WHEN PATINDEX('%[A-Za-z]%', datos_pago.spinus) = 1 THEN NULL
                                ELSE datos_pago.spinus
                            END
                        )), 8
                    ) AS cif_toda_transaccion
                FROM dw_mul_sppadat datos_pago
                INNER JOIN dw_mul_spmaco maestro_multipago
                    ON datos_pago.spcodc = maestro_multipago.spcodc
                WHERE datos_pago.dw_fecha_operacion_sp >= :fecha_inicio
                  AND datos_pago.dw_fecha_operacion_sp <  :fecha_fin
                  AND datos_pago.spcpde = 'App'
                  AND datos_pago.sppafr = 'N'
                  AND datos_pago.spcodc IN (
                        '866','882','130','143','184','227','237','238','309','368','371',
                        '446','459','478','507','512','526','571','574','643','680','687',
                        '734','755','885','888','481','907','693','572','573','732','498',
                        '524','513','868','869','408','784'
                  )
            )
            SELECT DISTINCT
                cif_toda_transaccion
            FROM pagos_toda_transaccion
            WHERE cif_toda_transaccion IS NOT NULL;
        """,
    }


def run_block(name: str, sql: str, params: dict[str, str]) -> BlockResult:
    start = time.perf_counter()
    try:
        df = run_query(sql, params=params)
        rows = len(df.index)
        return BlockResult(
            name=name,
            ok=True,
            seconds=time.perf_counter() - start,
            rows=rows,
            df=df,
        )
    except Exception as exc:
        return BlockResult(
            name=name,
            ok=False,
            seconds=time.perf_counter() - start,
            error=str(exc),
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Diagnostico paralelo de bloques SQL para pagos de servicios."
    )
    parser.add_argument("--fecha-inicio", default="2026-03-10", help="Fecha inicio (YYYY-MM-DD)")
    parser.add_argument("--fecha-fin", default="2026-03-11", help="Fecha fin exclusiva (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=2, help="Cantidad de hilos paralelos")
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Muestra N filas de clientes sin pago al final (0 para no mostrar)",
    )
    args = parser.parse_args()

    params = {"fecha_inicio": args.fecha_inicio, "fecha_fin": args.fecha_fin}
    queries = build_queries()

    print("Iniciando diagnostico paralelo por etapas...")
    print(f"Rango: [{args.fecha_inicio}, {args.fecha_fin})")
    print(f"Bloques: {', '.join(queries.keys())}")
    print("-" * 80)

    started = time.perf_counter()
    results: list[BlockResult] = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(run_block, name, sql, params): name
            for name, sql in queries.items()
        }
        for future in as_completed(future_map):
            result = future.result()
            results.append(result)
            if result.ok:
                print(
                    f"[OK]  {result.name:<18} "
                    f"{result.seconds:>8.2f}s  filas={result.rows:,}"
                )
            else:
                print(
                    f"[ERR] {result.name:<18} "
                    f"{result.seconds:>8.2f}s  error={result.error}"
                )

    failed = [r for r in results if not r.ok]
    if failed:
        print("-" * 80)
        print("Finalizado con errores. Corrige los bloques en error y vuelve a ejecutar.")
        return

    by_name = {r.name: r for r in results}
    df_base = by_name["clientes_base"].df.copy()
    df_pagadores = by_name["pagadores"].df.copy()

    df_base["padded_codigo_cliente"] = df_base["padded_codigo_cliente"].astype(str).str.strip()
    df_pagadores["cif_toda_transaccion"] = df_pagadores["cif_toda_transaccion"].astype(str).str.strip()

    t_cross = time.perf_counter()
    df_sin_pago = df_base[~df_base["padded_codigo_cliente"].isin(df_pagadores["cif_toda_transaccion"])].copy()
    cross_seconds = time.perf_counter() - t_cross

    total_seconds = time.perf_counter() - started

    print(
        f"[OK]  {'cruce_python':<18} "
        f"{cross_seconds:>8.2f}s  filas={len(df_sin_pago.index):,}"
    )
    print("-" * 80)
    print(f"Tiempo total del diagnostico: {total_seconds:.2f}s")
    print("Ranking etapas (mas rapido -> mas lento):")
    ranking = sorted(results + [BlockResult("cruce_python", True, cross_seconds, len(df_sin_pago.index))], key=lambda x: x.seconds)
    for item in ranking:
        status = "OK" if item.ok else "ERR"
        extra = f"filas={item.rows:,}" if item.ok else "error"
        print(f"  - {item.name:<18} {item.seconds:>8.2f}s [{status}] {extra}")

    print("-" * 80)
    print("Resumen:")
    print(f"- clientes_base: {len(df_base.index):,}")
    print(f"- pagadores: {len(df_pagadores.index):,}")
    print(f"- clientes_sin_pago: {len(df_sin_pago.index):,}")

    if args.top > 0:
        print("-" * 80)
        print(f"Top {args.top} clientes sin pago:")
        cols = ["codigo_cliente", "padded_codigo_cliente", "nombre_cliente"]
        print(df_sin_pago[cols].head(args.top).to_string(index=False))


if __name__ == "__main__":
    main()
