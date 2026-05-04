"""
debug_compradores_naturales_superpack_3m.py
--------------------------------------------
Consulta base para validar existencia de datos:

- Compradores de Superpack (spcodc = 498)
- Ultimos 3 meses cerrados (default: 2026-02-01 a 2026-05-01 exclusivo)
- Monto por transaccion >= 120 L
- Solo clientes naturales (CLTIPE = 'N')

No aplica filtros de usuario activo, convenio activo ni banca en linea.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, ".")

from core.db import run_query
from core.utils import exportar_excel


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT = BASE_DIR / "exports" / "debug_compradores_naturales_superpack_3m_min120.xlsx"

SQL_DIAGNOSTICO_ETAPAS = """
WITH base_tx AS (
    SELECT
        LTRIM(RTRIM(p.spinus)) AS spinus,
        CONVERT(date, p.dw_fecha_operacion_sp) AS fecha_operacion,
        CAST(p.sppava AS DECIMAL(18, 2)) AS monto_operacion,
        TRY_CONVERT(INT, p.spcpco) AS canal_compra
    FROM dw_mul_sppadat p
    WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
      AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
      AND p.sppafr = 'N'
      AND TRY_CONVERT(INT, p.spcodc) = :codigo_superpack
      AND CAST(p.sppava AS DECIMAL(18, 2)) >= :monto_minimo
),
parse_map AS (
    SELECT
        b.spinus,
        TRY_CONVERT(BIGINT, x.codigo_extraido) AS codigo_parse_num
    FROM base_tx b
    CROSS APPLY (
        SELECT
            LTRIM(RTRIM(
                CASE
                    WHEN b.spinus IS NULL THEN NULL
                    WHEN PATINDEX('%[A-Za-z]%', b.spinus) > 1
                        THEN LEFT(b.spinus, PATINDEX('%[A-Za-z]%', b.spinus) - 1)
                    WHEN PATINDEX('%[A-Za-z]%', b.spinus) = 1 THEN NULL
                    ELSE b.spinus
                END
            )) AS codigo_extraido
    ) x
),
bel_users AS (
    SELECT
        LTRIM(RTRIM(CLCCLI)) AS CLCCLI,
        LTRIM(RTRIM(USCODE)) AS USCODE
    FROM DW_BEL_IBUSER
),
bel_map AS (
    SELECT
        b.spinus,
        TRY_CONVERT(BIGINT, u.CLCCLI) AS codigo_bel_num
    FROM base_tx b
    LEFT JOIN bel_users u
      ON b.spinus = (u.CLCCLI + u.USCODE)
),
clientes_naturales AS (
    SELECT DISTINCT
        TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLDOC))) AS codigo_nat_num
    FROM DW_CIF_CLIENTES
    WHERE CLTIPE = 'N'
)
SELECT
    (SELECT COUNT(*) FROM base_tx) AS tx_base,
    (SELECT COUNT(DISTINCT spinus) FROM base_tx) AS spinus_distintos,
    (SELECT COUNT(*) FROM parse_map WHERE codigo_parse_num IS NOT NULL) AS tx_parse_mapeadas,
    (SELECT COUNT(DISTINCT codigo_parse_num) FROM parse_map WHERE codigo_parse_num IS NOT NULL) AS clientes_parse_distintos,
    (SELECT COUNT(*) FROM bel_map WHERE codigo_bel_num IS NOT NULL) AS tx_bel_mapeadas,
    (SELECT COUNT(DISTINCT codigo_bel_num) FROM bel_map WHERE codigo_bel_num IS NOT NULL) AS clientes_bel_distintos,
    (SELECT COUNT(*) FROM parse_map p INNER JOIN clientes_naturales n ON n.codigo_nat_num = p.codigo_parse_num) AS tx_parse_naturales,
    (SELECT COUNT(DISTINCT p.codigo_parse_num) FROM parse_map p INNER JOIN clientes_naturales n ON n.codigo_nat_num = p.codigo_parse_num) AS clientes_parse_naturales,
    (SELECT COUNT(*) FROM bel_map b INNER JOIN clientes_naturales n ON n.codigo_nat_num = b.codigo_bel_num) AS tx_bel_naturales,
    (SELECT COUNT(DISTINCT b.codigo_bel_num) FROM bel_map b INNER JOIN clientes_naturales n ON n.codigo_nat_num = b.codigo_bel_num) AS clientes_bel_naturales
"""

SQL_DIAG_CANALES = """
SELECT
    TRY_CONVERT(INT, p.spcpco) AS canal_compra,
    COUNT(*) AS total_tx
FROM dw_mul_sppadat p
WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
  AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
  AND p.sppafr = 'N'
  AND TRY_CONVERT(INT, p.spcodc) = :codigo_superpack
  AND CAST(p.sppava AS DECIMAL(18, 2)) >= :monto_minimo
GROUP BY TRY_CONVERT(INT, p.spcpco)
ORDER BY total_tx DESC
"""

SQL_DEBUG_COMPRADORES_NATURALES = """
WITH compras_superpack AS (
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
),
clientes_naturales AS (
    SELECT DISTINCT
        TRY_CONVERT(BIGINT, LTRIM(RTRIM(CLDOC))) AS codigo_cliente_num,
        LTRIM(RTRIM(CLDOC)) AS codigo_cliente_cif,
        CLNOCL AS nombre_cliente
    FROM DW_CIF_CLIENTES
    WHERE CLTIPE = 'N'
),
compras_naturales AS (
    SELECT
        c.padded_codigo_cliente,
        c.codigo_cliente_num,
        n.codigo_cliente_cif,
        n.nombre_cliente,
        c.fecha_operacion,
        c.monto_operacion,
        c.canal_compra
    FROM compras_superpack c
    INNER JOIN clientes_naturales n
        ON n.codigo_cliente_num = c.codigo_cliente_num
    WHERE c.codigo_cliente_num IS NOT NULL
)
SELECT
    padded_codigo_cliente,
    codigo_cliente_cif,
    ISNULL(nombre_cliente, 'N/D') AS nombre_cliente,
    COUNT(*) AS total_tx_3m,
    CAST(SUM(monto_operacion) AS DECIMAL(18, 2)) AS monto_total_3m,
    MIN(fecha_operacion) AS primera_fecha_operacion,
    MAX(fecha_operacion) AS ultima_fecha_operacion,
    MIN(canal_compra) AS canal_min_detectado,
    MAX(canal_compra) AS canal_max_detectado
FROM compras_naturales
GROUP BY padded_codigo_cliente, codigo_cliente_cif, nombre_cliente
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
        description="Debug base de compradores naturales Superpack en 3 meses."
    )
    parser.add_argument("--fecha-inicio", default="2026-02-01", help="Fecha inicio (YYYY-MM-DD).")
    parser.add_argument("--fecha-fin-exclusiva", default="2026-05-01", help="Fecha fin exclusiva (YYYY-MM-DD).")
    parser.add_argument("--codigo-superpack", type=int, default=498, help="Codigo superpack.")
    parser.add_argument("--monto-minimo", type=float, default=120.0, help="Monto minimo por transaccion.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Ruta de salida del Excel.")
    parser.add_argument("--no-export", action="store_true", help="No exporta archivo, solo muestra resumen.")
    return parser.parse_args()


def imprimir_resumen(df: pd.DataFrame, args: argparse.Namespace) -> None:
    total_clientes = int(len(df))
    total_tx = int(pd.to_numeric(df["total_tx_3m"], errors="coerce").fillna(0).sum()) if not df.empty else 0
    monto_total = float(pd.to_numeric(df["monto_total_3m"], errors="coerce").fillna(0.0).sum()) if not df.empty else 0.0

    print("\n============================================================")
    print(" DEBUG BASE — COMPRADORES NATURALES SUPERPACK (3M)")
    print("============================================================")
    print(f"Periodo          : {args.fecha_inicio} a {args.fecha_fin_exclusiva} (fin exclusivo)")
    print(f"Codigo superpack : {args.codigo_superpack}")
    print(f"Monto minimo trx : L {args.monto_minimo:,.2f}")
    print("Filtro aplicado  : CLTIPE = 'N' (solo natural)")
    print("------------------------------------------------------------")
    print(f"Clientes base    : {total_clientes:,}")
    print(f"Total tx base    : {total_tx:,}")
    print(f"Monto total base : L {monto_total:,.2f}")
    print("============================================================\n")

    if not df.empty:
        print("Top 20 clientes:")
        print(df.head(20).to_string(index=False))
        print()


def imprimir_diagnostico(df_diag: pd.DataFrame, df_canales: pd.DataFrame) -> None:
    if df_diag.empty:
        print("[INFO] Diagnostico sin resultados.")
        return
    row = df_diag.iloc[0]
    print("\n================ DIAGNOSTICO ETAPAS ================")
    print(f"tx_base                  : {int(row['tx_base']):,}")
    print(f"spinus_distintos         : {int(row['spinus_distintos']):,}")
    print(f"tx_parse_mapeadas        : {int(row['tx_parse_mapeadas']):,}")
    print(f"clientes_parse_distintos : {int(row['clientes_parse_distintos']):,}")
    print(f"tx_bel_mapeadas          : {int(row['tx_bel_mapeadas']):,}")
    print(f"clientes_bel_distintos   : {int(row['clientes_bel_distintos']):,}")
    print(f"tx_parse_naturales       : {int(row['tx_parse_naturales']):,}")
    print(f"clientes_parse_naturales : {int(row['clientes_parse_naturales']):,}")
    print(f"tx_bel_naturales         : {int(row['tx_bel_naturales']):,}")
    print(f"clientes_bel_naturales   : {int(row['clientes_bel_naturales']):,}")
    print("=====================================================\n")

    if not df_canales.empty:
        print("Canales detectados (top):")
        print(df_canales.to_string(index=False))
        print()


def main() -> None:
    args = parse_args()
    params = {
        "fecha_inicio": args.fecha_inicio,
        "fecha_fin_exclusiva": args.fecha_fin_exclusiva,
        "codigo_superpack": args.codigo_superpack,
        "monto_minimo": args.monto_minimo,
    }

    try:
        df_diag = run_query(SQL_DIAGNOSTICO_ETAPAS, params=params)
        df_canales = run_query(SQL_DIAG_CANALES, params=params)
        imprimir_diagnostico(df_diag, df_canales)

        print("Consultando base de compradores naturales Superpack...")
        df = run_query(SQL_DEBUG_COMPRADORES_NATURALES, params=params)
        imprimir_resumen(df, args)

        if not args.no_export:
            exportar_excel(df, args.output, hoja="debug_base_naturales")
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
