"""
debug_filtros_etapas_superpack_3m.py
------------------------------------
Diagnostica el embudo de filtros para Superpack CLARO en 3 meses:

S0: Base (spcodc + periodo + sin reversa + monto minimo + cliente mapeado)
S1: + Canal 1/7
S2: + Cliente natural (CLTIPE = 'N')
S3: + CLSTAT = 'A'
S4: + UsuarioActivo >= 1
S5: + BancaE >= 1
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query
from core.utils import exportar_excel_multi


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = BASE_DIR / "exports" / "debug_filtros_etapas_superpack_3m.xlsx"

SQL_RESUMEN_ETAPAS = """
WITH compras_mapeadas AS (
    SELECT
        TRY_CONVERT(BIGINT, u.CLCCLI) AS codigo_cliente_num,
        CONVERT(date, p.dw_fecha_operacion_sp) AS fecha_operacion,
        CAST(p.sppava AS DECIMAL(18, 2)) AS monto_operacion,
        TRY_CONVERT(INT, p.spcpco) AS canal_compra
    FROM dw_mul_sppadat p
    INNER JOIN (
        SELECT
            LTRIM(RTRIM(CLCCLI)) AS CLCCLI,
            LTRIM(RTRIM(USCODE)) AS USCODE
        FROM DW_BEL_IBUSER
    ) u
      ON LTRIM(RTRIM(p.spinus)) = (u.CLCCLI + u.USCODE)
    WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
      AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
      AND p.sppafr = 'N'
      AND TRY_CONVERT(INT, p.spcodc) = :codigo_superpack
      AND CAST(p.sppava AS DECIMAL(18, 2)) >= :monto_minimo
      AND TRY_CONVERT(BIGINT, u.CLCCLI) IS NOT NULL
),
clientes_naturales AS (
    SELECT DISTINCT TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLDOC))) AS codigo_cliente_num
    FROM DW_CIF_CLIENTES
    WHERE CLTIPE = 'N'
      AND TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLDOC))) IS NOT NULL
),
clientes_clstat_a AS (
    SELECT DISTINCT TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLCCLI))) AS codigo_cliente_num
    FROM DW_BEL_IBCLIE
    WHERE CLSTAT = 'A'
      AND TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLCCLI))) IS NOT NULL
),
clientes_usuario_activo AS (
    SELECT
        TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLCCLI))) AS codigo_cliente_num
    FROM DW_BEL_IBUSER
    WHERE TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLCCLI))) IS NOT NULL
    GROUP BY TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLCCLI)))
    HAVING SUM(CASE WHEN USSTAT = 'A' THEN 1 ELSE 0 END) >= 1
),
clientes_bancae AS (
    SELECT DISTINCT TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLDOC))) AS codigo_cliente_num
    FROM DW_CIF_CLIENTES
    WHERE ISNULL(dw_usuarios_bel_cnt, 0) >= 1
      AND TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLDOC))) IS NOT NULL
),
s0 AS (
    SELECT * FROM compras_mapeadas
),
s1 AS (
    SELECT * FROM s0 WHERE canal_compra IN (1, 7)
),
s2 AS (
    SELECT s1.*
    FROM s1
    INNER JOIN clientes_naturales n
        ON n.codigo_cliente_num = s1.codigo_cliente_num
),
s3 AS (
    SELECT s2.*
    FROM s2
    INNER JOIN clientes_clstat_a c
        ON c.codigo_cliente_num = s2.codigo_cliente_num
),
s4 AS (
    SELECT s3.*
    FROM s3
    INNER JOIN clientes_usuario_activo u
        ON u.codigo_cliente_num = s3.codigo_cliente_num
),
s5 AS (
    SELECT s4.*
    FROM s4
    INNER JOIN clientes_bancae b
        ON b.codigo_cliente_num = s4.codigo_cliente_num
)
SELECT
    etapa,
    total_tx,
    clientes_unicos,
    CAST(monto_total AS DECIMAL(18, 2)) AS monto_total
FROM (
    SELECT
        'S0_base_mapeada' AS etapa,
        COUNT(*) AS total_tx,
        COUNT(DISTINCT codigo_cliente_num) AS clientes_unicos,
        SUM(monto_operacion) AS monto_total
    FROM s0
    UNION ALL
    SELECT
        'S1_canal_1_7' AS etapa,
        COUNT(*) AS total_tx,
        COUNT(DISTINCT codigo_cliente_num) AS clientes_unicos,
        SUM(monto_operacion) AS monto_total
    FROM s1
    UNION ALL
    SELECT
        'S2_naturales' AS etapa,
        COUNT(*) AS total_tx,
        COUNT(DISTINCT codigo_cliente_num) AS clientes_unicos,
        SUM(monto_operacion) AS monto_total
    FROM s2
    UNION ALL
    SELECT
        'S3_clstat_A' AS etapa,
        COUNT(*) AS total_tx,
        COUNT(DISTINCT codigo_cliente_num) AS clientes_unicos,
        SUM(monto_operacion) AS monto_total
    FROM s3
    UNION ALL
    SELECT
        'S4_usuario_activo' AS etapa,
        COUNT(*) AS total_tx,
        COUNT(DISTINCT codigo_cliente_num) AS clientes_unicos,
        SUM(monto_operacion) AS monto_total
    FROM s4
    UNION ALL
    SELECT
        'S5_bancae_ge_1' AS etapa,
        COUNT(*) AS total_tx,
        COUNT(DISTINCT codigo_cliente_num) AS clientes_unicos,
        SUM(monto_operacion) AS monto_total
    FROM s5
) x
ORDER BY etapa
"""

SQL_MUESTRA_FINAL = """
WITH compras_mapeadas AS (
    SELECT
        TRY_CONVERT(BIGINT, u.CLCCLI) AS codigo_cliente_num,
        RIGHT('00000000' + LTRIM(RTRIM(u.CLCCLI)), 8) AS padded_codigo_cliente,
        CONVERT(date, p.dw_fecha_operacion_sp) AS fecha_operacion,
        CAST(p.sppava AS DECIMAL(18, 2)) AS monto_operacion,
        TRY_CONVERT(INT, p.spcpco) AS canal_compra
    FROM dw_mul_sppadat p
    INNER JOIN (
        SELECT
            LTRIM(RTRIM(CLCCLI)) AS CLCCLI,
            LTRIM(RTRIM(USCODE)) AS USCODE
        FROM DW_BEL_IBUSER
    ) u
      ON LTRIM(RTRIM(p.spinus)) = (u.CLCCLI + u.USCODE)
    WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
      AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
      AND p.sppafr = 'N'
      AND TRY_CONVERT(INT, p.spcodc) = :codigo_superpack
      AND CAST(p.sppava AS DECIMAL(18, 2)) >= :monto_minimo
      AND TRY_CONVERT(BIGINT, u.CLCCLI) IS NOT NULL
),
clientes_naturales AS (
    SELECT DISTINCT TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLDOC))) AS codigo_cliente_num
    FROM DW_CIF_CLIENTES
    WHERE CLTIPE = 'N'
      AND TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLDOC))) IS NOT NULL
),
clientes_clstat_a AS (
    SELECT DISTINCT TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLCCLI))) AS codigo_cliente_num
    FROM DW_BEL_IBCLIE
    WHERE CLSTAT = 'A'
      AND TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLCCLI))) IS NOT NULL
),
clientes_usuario_activo AS (
    SELECT
        TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLCCLI))) AS codigo_cliente_num
    FROM DW_BEL_IBUSER
    WHERE TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLCCLI))) IS NOT NULL
    GROUP BY TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLCCLI)))
    HAVING SUM(CASE WHEN USSTAT = 'A' THEN 1 ELSE 0 END) >= 1
),
clientes_bancae AS (
    SELECT DISTINCT TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLDOC))) AS codigo_cliente_num
    FROM DW_CIF_CLIENTES
    WHERE ISNULL(dw_usuarios_bel_cnt, 0) >= 1
      AND TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLDOC))) IS NOT NULL
),
final_rows AS (
    SELECT cm.*
    FROM compras_mapeadas cm
    INNER JOIN clientes_naturales n
        ON n.codigo_cliente_num = cm.codigo_cliente_num
    INNER JOIN clientes_clstat_a c
        ON c.codigo_cliente_num = cm.codigo_cliente_num
    INNER JOIN clientes_usuario_activo u
        ON u.codigo_cliente_num = cm.codigo_cliente_num
    INNER JOIN clientes_bancae b
        ON b.codigo_cliente_num = cm.codigo_cliente_num
    WHERE cm.canal_compra IN (1, 7)
)
SELECT TOP (200)
    padded_codigo_cliente,
    codigo_cliente_num,
    COUNT(*) AS total_tx_3m,
    CAST(SUM(monto_operacion) AS DECIMAL(18, 2)) AS monto_total_3m,
    MIN(fecha_operacion) AS primera_fecha_operacion,
    MAX(fecha_operacion) AS ultima_fecha_operacion,
    MIN(canal_compra) AS canal_min,
    MAX(canal_compra) AS canal_max
FROM final_rows
GROUP BY padded_codigo_cliente, codigo_cliente_num
ORDER BY total_tx_3m DESC, monto_total_3m DESC, padded_codigo_cliente
"""


def construir_error_amigable(exc: Exception) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()
    if "permission was denied" in lower:
        return "[ERROR] Permiso denegado al consultar SQL Server."
    if "login timeout expired" in lower or "could not open a connection" in lower:
        return "[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales."
    return f"[ERROR] Fallo ejecutando la consulta: {raw}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Diagnostico de filtros por etapas para Superpack CLARO."
    )
    parser.add_argument("--fecha-inicio", default="2026-02-01", help="Fecha inicio (YYYY-MM-DD).")
    parser.add_argument("--fecha-fin-exclusiva", default="2026-05-01", help="Fecha fin exclusiva (YYYY-MM-DD).")
    parser.add_argument("--codigo-superpack", type=int, default=498, help="Codigo superpack.")
    parser.add_argument("--monto-minimo", type=float, default=120.0, help="Monto minimo por transaccion.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Ruta del Excel de salida.")
    parser.add_argument("--no-export", action="store_true", help="No exporta Excel.")
    parser.add_argument(
        "--con-muestra",
        action="store_true",
        help="Incluye hoja de muestra final (mas lento).",
    )
    return parser.parse_args()


def imprimir_resumen_etapas(df: pd.DataFrame, args: argparse.Namespace) -> None:
    print("\n============================================================")
    print(" DEBUG FILTROS POR ETAPAS — SUPERPACK CLARO 3M")
    print("============================================================")
    print(f"Periodo          : {args.fecha_inicio} a {args.fecha_fin_exclusiva} (fin exclusivo)")
    print(f"Codigo superpack : {args.codigo_superpack}")
    print(f"Monto minimo trx : L {args.monto_minimo:,.2f}")
    print("------------------------------------------------------------")
    print(df.to_string(index=False))
    print("============================================================\n")


def main() -> None:
    args = parse_args()
    params = {
        "fecha_inicio": args.fecha_inicio,
        "fecha_fin_exclusiva": args.fecha_fin_exclusiva,
        "codigo_superpack": args.codigo_superpack,
        "monto_minimo": args.monto_minimo,
    }

    try:
        print("Consultando diagnostico por etapas...")
        df_resumen = run_query(SQL_RESUMEN_ETAPAS, params=params)
        imprimir_resumen_etapas(df_resumen, args)

        if not args.no_export:
            sheets = {"resumen_etapas": df_resumen}
            if args.con_muestra:
                print("Consultando muestra final (top 200)...")
                df_muestra = run_query(SQL_MUESTRA_FINAL, params=params)
                sheets["muestra_final_top200"] = df_muestra

            exportar_excel_multi(
                sheets,
                args.output,
            )
        else:
            print("Export omitido (--no-export).")

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
