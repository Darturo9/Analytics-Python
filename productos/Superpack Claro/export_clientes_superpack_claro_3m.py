"""
export_clientes_superpack_claro_3m.py
--------------------------------------
Genera un Excel con clientes que cumplen estas condiciones:

1) Compra de Superpack CLARO (spcodc = 498)
2) Canal de compra en (1, 7)
3) Transacciones sin reversa (sppafr = 'N')
4) Monto por transaccion >= 120 L
5) Periodo 3 meses cerrados (default: 2026-02-01 a 2026-05-01 exclusivo)
6) Cliente natural (CLTIPE = 'N')
7) Banca en linea (BancaE >= 1)
8) Convenio activo (CLSTAT = 'A')
9) Usuario activo (UsuarioActivo >= 1)
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
DEFAULT_OUTPUT = BASE_DIR / "exports" / "clientes_superpack_claro_3m_condiciones.xlsx"

SQL_CLIENTES_SUPERPACK_3M = """
WITH compras_superpack AS (
    SELECT
        RIGHT('00000000' + x.codigo_extraido, 8) AS padded_codigo_cliente,
        CONVERT(VARCHAR(20), TRY_CONVERT(BIGINT, x.codigo_extraido)) AS codigo_cliente_sin_padding,
        CONVERT(date, p.dw_fecha_operacion_sp) AS fecha_operacion,
        CAST(p.sppava AS DECIMAL(18, 2)) AS monto_operacion
    FROM dw_mul_sppadat p
    CROSS APPLY (
        SELECT
            LTRIM(RTRIM(
                CASE
                    WHEN p.spinus IS NULL THEN NULL
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) > 1
                        THEN LEFT(p.spinus, PATINDEX('%[A-Za-z]%', p.spinus) - 1)
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) = 1 THEN NULL
                    ELSE p.spinus
                END
            )) AS codigo_extraido
    ) x
    WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
      AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
      AND p.sppafr = 'N'
      AND p.spcodc = :codigo_superpack
      AND p.spcpco IN ('1', '7')
      AND p.sppava >= :monto_minimo
),
compras_agg AS (
    SELECT
        padded_codigo_cliente,
        codigo_cliente_sin_padding,
        COUNT(*) AS total_tx_3m,
        CAST(SUM(monto_operacion) AS DECIMAL(18, 2)) AS monto_total_3m,
        MIN(fecha_operacion) AS primera_fecha_operacion,
        MAX(fecha_operacion) AS ultima_fecha_operacion
    FROM compras_superpack
    WHERE padded_codigo_cliente IS NOT NULL
      AND codigo_cliente_sin_padding IS NOT NULL
    GROUP BY padded_codigo_cliente, codigo_cliente_sin_padding
),
conteo_usuarios AS (
    SELECT
        u.CLCCLI,
        SUM(CASE WHEN USSTAT = 'A' THEN 1 ELSE 0 END) AS usuario_activo_cnt,
        SUM(CASE WHEN USSTAT = 'I' THEN 1 ELSE 0 END) AS usuario_inactivo_cnt,
        COUNT(USCODE) AS cantidad_usuarios
    FROM DW_BEL_IBUSER u
    INNER JOIN compras_agg c
        ON u.CLCCLI = c.codigo_cliente_sin_padding
    GROUP BY u.CLCCLI
),
datos_bel_base AS (
    SELECT
        b.CLCCLI,
        b.CLSTAT,
        b.CLNOCL AS nombre_cliente,
        b.PECODE AS perfil_convenio,
        ROW_NUMBER() OVER (
            PARTITION BY b.CLCCLI
            ORDER BY CASE WHEN b.CLSTAT = 'A' THEN 1 ELSE 2 END, b.CLCCLI
        ) AS rn
    FROM DW_BEL_IBCLIE b
    INNER JOIN compras_agg c
        ON b.CLCCLI = c.codigo_cliente_sin_padding
),
datos_bel AS (
    SELECT
        CLCCLI,
        CLSTAT,
        nombre_cliente,
        perfil_convenio
    FROM datos_bel_base
    WHERE rn = 1
),
datos_cif_base AS (
    SELECT
        cf.CLDOC AS CLDOC,
        cf.CLTIPE AS tipo_cliente,
        ISNULL(cf.dw_usuarios_bel_cnt, 0) AS bancae,
        ROW_NUMBER() OVER (
            PARTITION BY cf.CLDOC
            ORDER BY
                CASE WHEN cf.CLTIPE = 'N' THEN 1 WHEN cf.CLTIPE IS NULL THEN 2 ELSE 3 END,
                CASE WHEN cf.dw_usuarios_bel_cnt IS NULL THEN 1 ELSE 0 END,
                cf.CLDOC
        ) AS rn
    FROM DW_CIF_CLIENTES cf
    INNER JOIN compras_agg c
        ON cf.CLDOC = c.codigo_cliente_sin_padding
)
SELECT
    c.padded_codigo_cliente,
    c.codigo_cliente_sin_padding,
    ISNULL(b.nombre_cliente, 'N/D') AS nombre_cliente,
    ISNULL(b.perfil_convenio, 'N/D') AS perfil_convenio,
    ISNULL(b.CLSTAT, 'N/D') AS clstat,
    ISNULL(u.usuario_activo_cnt, 0) AS usuario_activo_cnt,
    ISNULL(u.usuario_inactivo_cnt, 0) AS usuario_inactivo_cnt,
    ISNULL(u.cantidad_usuarios, 0) AS cantidad_usuarios,
    ISNULL(cf.tipo_cliente, 'N/D') AS tipo_cliente,
    ISNULL(cf.bancae, 0) AS bancae,
    c.total_tx_3m,
    c.monto_total_3m,
    c.primera_fecha_operacion,
    c.ultima_fecha_operacion
FROM compras_agg c
INNER JOIN datos_bel b
    ON b.CLCCLI = c.codigo_cliente_sin_padding
INNER JOIN conteo_usuarios u
    ON u.CLCCLI = c.codigo_cliente_sin_padding
INNER JOIN datos_cif_base cf
    ON cf.CLDOC = c.codigo_cliente_sin_padding
   AND cf.rn = 1
WHERE b.CLSTAT = 'A'
  AND u.usuario_activo_cnt >= 1
  AND cf.tipo_cliente = 'N'
  AND cf.bancae >= 1
ORDER BY c.total_tx_3m DESC, c.monto_total_3m DESC, c.padded_codigo_cliente
"""


def construir_error_amigable(exc: Exception) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()
    if "permission was denied" in lower:
        return "[ERROR] Permiso denegado al consultar SQL Server."
    if "login timeout expired" in lower or "could not open a connection" in lower:
        return "[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales."
    return f"[ERROR] Fallo ejecutando la consulta: {raw}"


def validar_reglas(df: pd.DataFrame) -> None:
    if df.empty:
        return

    tipo = df["tipo_cliente"].astype(str).str.strip()
    clstat = df["clstat"].astype(str).str.strip()
    bancae = pd.to_numeric(df["bancae"], errors="coerce").fillna(0)
    activos = pd.to_numeric(df["usuario_activo_cnt"], errors="coerce").fillna(0)

    invalido = (tipo != "N") | (clstat != "A") | (bancae < 1) | (activos < 1)
    if invalido.any():
        total = int(invalido.sum())
        raise ValueError(
            f"Se detectaron {total} filas que no cumplen filtros finales "
            "(tipo_cliente='N', CLSTAT='A', BancaE>=1, UsuarioActivo>=1)."
        )


def imprimir_resumen(df: pd.DataFrame, args: argparse.Namespace) -> None:
    total_clientes = int(len(df))
    total_tx = int(pd.to_numeric(df["total_tx_3m"], errors="coerce").fillna(0).sum()) if not df.empty else 0
    monto_total = float(pd.to_numeric(df["monto_total_3m"], errors="coerce").fillna(0.0).sum()) if not df.empty else 0.0

    print("\n============================================================")
    print(" CLIENTES SUPERPACK CLARO (3 MESES CERRADOS)")
    print("============================================================")
    print(f"Periodo              : {args.fecha_inicio} a {args.fecha_fin_exclusiva} (fin exclusivo)")
    print(f"Codigo superpack     : {args.codigo_superpack}")
    print("Canales permitidos   : 1 y 7")
    print(f"Monto minimo trx     : L {args.monto_minimo:,.2f}")
    print("Filtros cliente      : CLTIPE='N', BancaE>=1, CLSTAT='A', UsuarioActivo>=1")
    print("------------------------------------------------------------")
    print(f"Clientes finales     : {total_clientes:,}")
    print(f"Total transacciones  : {total_tx:,}")
    print(f"Monto total          : L {monto_total:,.2f}")
    print("============================================================\n")

    if not df.empty:
        preview = df.head(10).copy()
        print("Top 10 filas:")
        print(preview.to_string(index=False))
        print()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exporta clientes Superpack CLARO con condiciones de actividad."
    )
    parser.add_argument("--fecha-inicio", default="2026-02-01", help="Fecha inicio (YYYY-MM-DD).")
    parser.add_argument(
        "--fecha-fin-exclusiva",
        default="2026-05-01",
        help="Fecha fin exclusiva (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--codigo-superpack",
        type=int,
        default=498,
        help="Codigo de superpack en Multipagos.",
    )
    parser.add_argument(
        "--monto-minimo",
        type=float,
        default=120.0,
        help="Monto minimo por transaccion en lempiras.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Ruta de salida del Excel.",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Ejecuta consulta y resumen sin exportar Excel.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    params = {
        "fecha_inicio": args.fecha_inicio,
        "fecha_fin_exclusiva": args.fecha_fin_exclusiva,
        "codigo_superpack": str(args.codigo_superpack),
        "monto_minimo": args.monto_minimo,
    }

    try:
        print("Consultando clientes con condiciones de Superpack CLARO...")
        df = run_query(SQL_CLIENTES_SUPERPACK_3M, params=params)
        validar_reglas(df)
        imprimir_resumen(df, args)

        if not args.no_export:
            exportar_excel(df, args.output, hoja="clientes_superpack_3m")
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
