import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

sys.path.insert(0, ".")

from core.db import run_query


@dataclass
class BlockResult:
    name: str
    ok: bool
    seconds: float
    rows: int | None = None
    error: str | None = None


def build_queries() -> dict[str, str]:
    return {
        "clientes_base": """
            SELECT COUNT(*) AS total
            FROM (
                SELECT DISTINCT
                    LTRIM(RTRIM(c.cldoc)) AS codigo_cliente
                FROM dw_cif_clientes c
                INNER JOIN dw_bel_ibclie ibc
                    ON LTRIM(RTRIM(ibc.clccli)) = LTRIM(RTRIM(c.cldoc))
                   AND ibc.clstat = 'A'
                INNER JOIN dw_bel_ibuser ibu
                    ON LTRIM(RTRIM(ibu.clccli)) = LTRIM(RTRIM(c.cldoc))
                   AND ibu.usstat = 'A'
                INNER JOIN dw_dep_depositos d
                    ON LTRIM(RTRIM(d.cldoc)) = LTRIM(RTRIM(c.cldoc))
                   AND d.dw_estado_cuenta = 'ACTIVA'
                WHERE c.estatu = 'A'
                  AND c.cltipe = 'N'
                  AND c.clclco <> 10
                  AND COALESCE(c.dw_sms_cnt, 0) >= 1
            ) x;
        """,
        "pagos_bxi": """
            SELECT COUNT(*) AS total
            FROM (
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
            ) x;
        """,
        "pagos_multi": """
            SELECT COUNT(*) AS total
            FROM (
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
            ) x;
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
            SELECT COUNT(*) AS total
            FROM (
                SELECT DISTINCT cif_toda_transaccion
                FROM pagos_toda_transaccion
                WHERE cif_toda_transaccion IS NOT NULL
            ) p;
        """,
        "clientes_sin_pago": """
            WITH clientes_base AS (
                SELECT DISTINCT
                    LTRIM(RTRIM(c.cldoc)) AS codigo_cliente,
                    RIGHT('00000000' + LTRIM(RTRIM(c.cldoc)), 8) AS padded_codigo_cliente,
                    c.clnomb AS nombre_cliente
                FROM dw_cif_clientes c
                INNER JOIN dw_bel_ibclie ibc
                    ON LTRIM(RTRIM(ibc.clccli)) = LTRIM(RTRIM(c.cldoc))
                   AND ibc.clstat = 'A'
                INNER JOIN dw_bel_ibuser ibu
                    ON LTRIM(RTRIM(ibu.clccli)) = LTRIM(RTRIM(c.cldoc))
                   AND ibu.usstat = 'A'
                INNER JOIN dw_dep_depositos d
                    ON LTRIM(RTRIM(d.cldoc)) = LTRIM(RTRIM(c.cldoc))
                   AND d.dw_estado_cuenta = 'ACTIVA'
                WHERE c.estatu = 'A'
                  AND c.cltipe = 'N'
                  AND c.clclco <> 10
                  AND COALESCE(c.dw_sms_cnt, 0) >= 1
            ),
            pagos_toda_transaccion AS (
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
            ),
            pagadores AS (
                SELECT DISTINCT cif_toda_transaccion
                FROM pagos_toda_transaccion
                WHERE cif_toda_transaccion IS NOT NULL
            )
            SELECT COUNT(*) AS total
            FROM clientes_base b
            LEFT JOIN pagadores p
                ON p.cif_toda_transaccion = b.padded_codigo_cliente
            WHERE p.cif_toda_transaccion IS NULL;
        """,
    }


def run_block(name: str, sql: str, params: dict[str, str]) -> BlockResult:
    start = time.perf_counter()
    try:
        df = run_query(sql, params=params)
        rows = int(df.iloc[0, 0]) if not df.empty else 0
        return BlockResult(name=name, ok=True, seconds=time.perf_counter() - start, rows=rows)
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
    parser.add_argument("--workers", type=int, default=5, help="Cantidad de hilos paralelos")
    args = parser.parse_args()

    params = {"fecha_inicio": args.fecha_inicio, "fecha_fin": args.fecha_fin}
    queries = build_queries()

    print("Iniciando diagnostico paralelo de bloques...")
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

    total_seconds = time.perf_counter() - started

    print("-" * 80)
    print(f"Tiempo total del diagnostico: {total_seconds:.2f}s")
    print("Ranking (mas rapido -> mas lento):")
    for item in sorted(results, key=lambda x: x.seconds):
        status = "OK" if item.ok else "ERR"
        extra = f"filas={item.rows:,}" if item.ok else "error"
        print(f"  - {item.name:<18} {item.seconds:>8.2f}s [{status}] {extra}")


if __name__ == "__main__":
    main()
