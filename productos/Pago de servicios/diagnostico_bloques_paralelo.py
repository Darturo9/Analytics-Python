import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

sys.path.insert(0, ".")

from core.db import run_query
from core.utils import exportar_excel


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
            WITH clientes_elegibles AS (
                SELECT
                    LTRIM(RTRIM(c.cldoc)) AS codigo_cliente,
                    RIGHT('00000000' + LTRIM(RTRIM(c.cldoc)), 8) AS padded_codigo_cliente,
                    c.clnomb AS nombre_cliente,
                    YEAR(c.DW_FECHA_NACIMIENTO) AS anio_nac,
                    CASE
                        WHEN c.DW_FECHA_NACIMIENTO IS NULL THEN NULL
                        ELSE
                            DATEDIFF(YEAR, c.DW_FECHA_NACIMIENTO, CAST(GETDATE() AS DATE))
                            - CASE
                                WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.DW_FECHA_NACIMIENTO, CAST(GETDATE() AS DATE)), c.DW_FECHA_NACIMIENTO) > CAST(GETDATE() AS DATE)
                                    THEN 1
                                    ELSE 0
                              END
                    END AS edad
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
                  )
            ),
            contacto_base AS (
                SELECT
                    ce.codigo_cliente,
                    ce.padded_codigo_cliente,
                    ce.nombre_cliente,
                    ce.anio_nac,
                    ce.edad,
                    CASE
                        WHEN sms.telefono_sms IS NOT NULL THEN sms.telefono_sms
                        WHEN COALESCE(LEN(telefonos.cltel1), 0) = 8
                             AND SUBSTRING(telefonos.cltel1, 1, 1) IN ('3', '7', '8', '9')
                            THEN telefonos.cltel1
                        WHEN COALESCE(LEN(telefonos.cltel2), 0) = 8
                             AND SUBSTRING(telefonos.cltel2, 1, 1) IN ('3', '7', '8', '9')
                            THEN telefonos.cltel2
                    END AS numero_telefono,
                    CASE
                        WHEN sms.telefono_sms IS NOT NULL THEN 'SMS_PERFIL'
                        WHEN COALESCE(LEN(telefonos.cltel1), 0) = 8
                             AND SUBSTRING(telefonos.cltel1, 1, 1) IN ('3', '7', '8', '9')
                            THEN 'DIRECCION_CLTEL1'
                        WHEN COALESCE(LEN(telefonos.cltel2), 0) = 8
                             AND SUBSTRING(telefonos.cltel2, 1, 1) IN ('3', '7', '8', '9')
                            THEN 'DIRECCION_CLTEL2'
                        ELSE 'SIN_DATO'
                    END AS fuente_telefono,
                    sms.dw_nombre_operador AS nombre_operador,
                    LOWER(RTRIM(LTRIM(correos.cldire))) AS correo,
                    COALESCE(NULLIF(LTRIM(RTRIM(telefonos.dw_nivel_geo2)), ''), 'SIN_DATO') AS departamento,
                    ROW_NUMBER() OVER (
                        PARTITION BY ce.codigo_cliente
                        ORDER BY sms.telefono_sms, telefonos.cltel1, telefonos.cltel2, correos.cldire
                    ) AS rn
                FROM clientes_elegibles ce
                LEFT JOIN dwhbi.dbo.dw_sms_perfil_usuario sms
                    ON ce.padded_codigo_cliente = RTRIM(LTRIM(sms.cif))
                   AND sms.dw_descripcion_status = 'ACTIVO'
                   AND sms.dw_nombre_operador IN ('Claro H', 'Hondutel', 'Tigo H')
                   AND LEN(sms.telefono_sms) = 8
                   AND SUBSTRING(sms.telefono_sms, 1, 1) IN ('3', '7', '8', '9')
                LEFT JOIN dw_cif_direcciones_principal telefonos
                    ON ce.codigo_cliente = RTRIM(LTRIM(telefonos.cldoc))
                LEFT JOIN dw_cif_direcciones correos
                    ON ce.codigo_cliente = RTRIM(LTRIM(correos.cldoc))
                   AND correos.cldico = 4
                   AND correos.cldire LIKE '%_@_%.__%'
            )
            SELECT
                codigo_cliente,
                padded_codigo_cliente,
                nombre_cliente,
                anio_nac,
                edad,
                numero_telefono,
                fuente_telefono,
                nombre_operador,
                correo,
                departamento
            FROM contacto_base
            WHERE rn = 1
              AND NOT (numero_telefono IS NULL AND correo IS NULL);
        """,
        "pagadores": """
            WITH pagos_toda_transaccion AS (
                SELECT DISTINCT
                    RIGHT('00000000' + RTRIM(LTRIM(txn_bxi.clccli)), 8) AS cif_toda_transaccion
                FROM dw_bel_ibjour txn_bxi
                INNER JOIN dw_bel_ibserv descripcion_servicio
                    ON txn_bxi.secode = descripcion_servicio.secode
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
        "--no-export",
        action="store_true",
        help="No exporta Excel al finalizar",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Ruta completa de salida Excel (opcional)",
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
    df_pagadores_unicos = df_pagadores[["cif_toda_transaccion"]].drop_duplicates()

    t_cross = time.perf_counter()
    df_sin_pago = df_base[~df_base["padded_codigo_cliente"].isin(df_pagadores_unicos["cif_toda_transaccion"])].copy()
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
    print(f"- clientes_sin_pago: {len(df_sin_pago.index):,}")

    preferred_cols = [
        "codigo_cliente",
        "padded_codigo_cliente",
        "nombre_cliente",
        "anio_nac",
        "edad",
        "numero_telefono",
        "fuente_telefono",
        "nombre_operador",
        "correo",
        "departamento",
    ]
    export_cols = [c for c in preferred_cols if c in df_sin_pago.columns]
    df_export = df_sin_pago[export_cols].sort_values("padded_codigo_cliente").reset_index(drop=True)
    # Normaliza nombres y valores para evitar errores de ancho al exportar en distintos pandas.
    df_export.columns = [str(c) for c in df_export.columns]
    df_export = df_export.where(pd.notna(df_export), "")
    df_export = df_export.astype(str)

    if not args.no_export:
        if args.output.strip():
            output_path = Path(args.output.strip())
        else:
            base_dir = Path("productos/Pago de servicios/exports")
            file_name = f"clientes_sin_pago_contacto_{args.fecha_inicio}_a_{args.fecha_fin}.xlsx"
            output_path = base_dir / file_name
        exportar_excel(df_export, str(output_path), hoja="ClientesSinPago")
        print(f"- Excel exportado: {output_path}")


if __name__ == "__main__":
    main()
