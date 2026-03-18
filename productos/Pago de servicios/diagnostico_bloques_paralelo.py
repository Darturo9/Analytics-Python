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
                    RIGHT('00000000' + RTRIM(LTRIM(txn_bxi.clccli)), 8) AS cif_toda_transaccion,
                    'BXI' AS origen_pago,
                    COALESCE(NULLIF(LTRIM(RTRIM(descripcion_servicio.inserv)), ''), 'SIN_DATO') AS canal_pago
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
                    ) AS cif_toda_transaccion,
                    'MULTIPAGO' AS origen_pago,
                    COALESCE(NULLIF(LTRIM(RTRIM(datos_pago.spcpde)), ''), 'SIN_DATO') AS canal_pago
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
                cif_toda_transaccion,
                origen_pago,
                canal_pago
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


def clientes_sin_pago(df_base: pd.DataFrame, df_pagadores: pd.DataFrame) -> pd.DataFrame:
    base = df_base.copy()
    pag = df_pagadores.copy()
    base["padded_codigo_cliente"] = base["padded_codigo_cliente"].astype(str).str.strip()
    pag["cif_toda_transaccion"] = pag["cif_toda_transaccion"].astype(str).str.strip()
    pag_unicos = pag[["cif_toda_transaccion"]].drop_duplicates()
    return base[~base["padded_codigo_cliente"].isin(pag_unicos["cif_toda_transaccion"])].copy()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Listado de clientes sin pago de servicios (export) y conteos por fechas."
    )
    parser.add_argument(
        "--fecha-listado",
        default="2025-01-01",
        help="Fecha inicio para listado/export Excel (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--fecha-conteo",
        default="2026-01-01",
        help="Fecha inicio para conteo adicional (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--fecha-conteo-2",
        default="2026-03-16",
        help="Segunda fecha de inicio para conteo adicional (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--fecha-fin",
        default="2100-01-01",
        help="Fecha fin exclusiva para ambos cortes (YYYY-MM-DD)",
    )
    parser.add_argument("--workers", type=int, default=3, help="Cantidad de hilos paralelos")
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

    queries = build_queries()

    print("Calculando clientes sin pago de servicios...")
    print(f"- Listado desde: {args.fecha_listado}")
    print(f"- Conteo adicional desde: {args.fecha_conteo}")
    print(f"- Conteo adicional 2 desde: {args.fecha_conteo_2}")
    print(f"- Fecha fin exclusiva: {args.fecha_fin}")
    print("-" * 80)

    params_listado = {"fecha_inicio": args.fecha_listado, "fecha_fin": args.fecha_fin}
    params_conteo = {"fecha_inicio": args.fecha_conteo, "fecha_fin": args.fecha_fin}
    params_conteo_2 = {"fecha_inicio": args.fecha_conteo_2, "fecha_fin": args.fecha_fin}

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(run_block, "clientes_base", queries["clientes_base"], {}): "clientes_base",
            executor.submit(run_block, "pagadores_listado", queries["pagadores"], params_listado): "pagadores_listado",
            executor.submit(run_block, "pagadores_conteo", queries["pagadores"], params_conteo): "pagadores_conteo",
            executor.submit(run_block, "pagadores_conteo_2", queries["pagadores"], params_conteo_2): "pagadores_conteo_2",
        }
        results: dict[str, BlockResult] = {}
        for future in as_completed(future_map):
            result = future.result()
            results[result.name] = result
            if result.ok:
                print(f"[OK]  {result.name:<18} {result.seconds:>8.2f}s")
            else:
                print(f"[ERR] {result.name:<18} {result.seconds:>8.2f}s  error={result.error}")

    failed = [r for r in results.values() if not r.ok]
    required = {"clientes_base", "pagadores_listado", "pagadores_conteo", "pagadores_conteo_2"}
    if failed or not required.issubset(results.keys()):
        print("-" * 80)
        print("Finalizado con errores. Corrige los bloques en error y vuelve a ejecutar.")
        return

    df_base = results["clientes_base"].df.copy()
    df_pagadores_listado = results["pagadores_listado"].df.copy()
    df_pagadores_conteo = results["pagadores_conteo"].df.copy()
    df_pagadores_conteo_2 = results["pagadores_conteo_2"].df.copy()

    df_sin_pago_listado = clientes_sin_pago(df_base, df_pagadores_listado)
    df_sin_pago_conteo = clientes_sin_pago(df_base, df_pagadores_conteo)
    df_sin_pago_conteo_2 = clientes_sin_pago(df_base, df_pagadores_conteo_2)

    print("Conteos:")
    print(f"- Clientes activos/naturales con banca (base): {len(df_base.index):,}")
    print(f"- Clientes sin pago desde {args.fecha_listado}: {len(df_sin_pago_listado.index):,}")
    print(f"- Clientes sin pago desde {args.fecha_conteo}: {len(df_sin_pago_conteo.index):,}")
    print(f"- Clientes sin pago desde {args.fecha_conteo_2}: {len(df_sin_pago_conteo_2.index):,}")
    print("-" * 80)

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
    export_cols = [c for c in preferred_cols if c in df_sin_pago_listado.columns]
    df_export = (
        df_sin_pago_listado[export_cols]
        .sort_values("padded_codigo_cliente")
        .reset_index(drop=True)
    )
    # Normaliza nombres y valores para evitar errores de ancho al exportar en distintos pandas.
    df_export.columns = [str(c) for c in df_export.columns]
    df_export = df_export.where(pd.notna(df_export), "")
    df_export = df_export.astype(str)

    if not args.no_export:
        if args.output.strip():
            output_path = Path(args.output.strip())
        else:
            base_dir = Path("productos/Pago de servicios/exports")
            file_name = f"clientes_sin_pago_contacto_desde_{args.fecha_listado}.xlsx"
            output_path = base_dir / file_name
        exportar_excel(df_export, str(output_path), hoja="ClientesSinPago")
        print(f"- Excel exportado: {output_path}")


if __name__ == "__main__":
    main()
