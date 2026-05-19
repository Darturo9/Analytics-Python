"""
comparar_quincenas_superpack.py
================================
Compara hasta 3 quincenas: total de transacciones y clientes unicos por periodo.

Configura los periodos en QUINCENAS al inicio del archivo.
Cada entrada: ("Etiqueta", "YYYY-MM-DD inicio", "YYYY-MM-DD fin exclusivo")
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT))

from core.db import run_query

# ── Periodos a comparar ───────────────────────────────────────────────────────
# Formato: ("Etiqueta", "YYYY-MM-DD inicio inclusivo", "YYYY-MM-DD fin exclusivo")
QUINCENAS = [
    ("Mar 1-15",  "2026-03-01", "2026-03-16"),
    ("Abr 1-15",  "2026-04-01", "2026-04-16"),
    ("May 1-15",  "2026-05-01", "2026-05-16"),
]
# ─────────────────────────────────────────────────────────────────────────────

SQL = """
SELECT
    COUNT(*)                                        AS total_trx,
    COUNT(DISTINCT
        RIGHT(
            '00000000' + LTRIM(RTRIM(
                CASE
                    WHEN p.spinus IS NULL                      THEN NULL
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) > 1 THEN LEFT(p.spinus, PATINDEX('%[A-Za-z]%', p.spinus) - 1)
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) = 1 THEN NULL
                    ELSE p.spinus
                END
            )), 8
        )
    )                                               AS clientes_unicos
FROM dw_mul_sppadat p
INNER JOIN dw_mul_spmaco m ON m.spcodc = p.spcodc
WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
  AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
  AND TRY_CONVERT(INT, p.spcodc) = 498
  AND p.spcpco IN (1, 7)
  AND m.CLMOCO IN ('001', 'L')
  AND p.sppafr = 'N'
"""


def main() -> None:
    filas = []

    for etiqueta, inicio, fin in QUINCENAS:
        try:
            df = run_query(SQL, params={"fecha_inicio": inicio, "fecha_fin_exclusiva": fin})
            if df.empty:
                total_trx, clientes = 0, 0
            else:
                total_trx = int(df["total_trx"].iloc[0])
                clientes  = int(df["clientes_unicos"].iloc[0])
        except SQLAlchemyError as exc:
            print(f"[ERROR] {etiqueta}: {exc}")
            sys.exit(1)

        filas.append({
            "Periodo":          etiqueta,
            "Total Trx":        total_trx,
            "Clientes Unicos":  clientes,
        })

    resultado = pd.DataFrame(filas)

    sep = "=" * 48
    print(f"\n{sep}")
    print("  SUPERPACK CLARO — COMPARACION QUINCENAS")
    print(sep)
    print(f"  {'Periodo':<14} {'Total Trx':>12} {'Clientes Unicos':>16}")
    print(f"  {'-'*14} {'-'*12} {'-'*16}")
    for _, r in resultado.iterrows():
        print(f"  {r['Periodo']:<14} {int(r['Total Trx']):>12,} {int(r['Clientes Unicos']):>16,}")
    print(f"{sep}\n")


if __name__ == "__main__":
    main()
