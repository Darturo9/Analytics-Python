"""
estadisticas_superpack_abril.py
--------------------------------
Estadisticas de compras Superpack Claro - Abril 2026:
  - Monto mas comprado (mas frecuente)
  - Promedio de compra por transaccion
  - Ranking completo de montos con conteo y porcentaje

Solo transacciones efectivas (excluye reversas).

Ejecucion:
    python3 "productos/Superpack Claro/abril 2026/estadisticas_superpack_abril.py"
"""

import sys
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from core.db import run_query_file

QUERY_PATH = Path(__file__).resolve().parent / "queries" / "estadisticas_superpack_abril_2026.sql"


def imprimir_estadisticas(df) -> None:
    if df.empty:
        print("Sin datos para abril 2026.")
        return

    df.columns = [c.lower() for c in df.columns]

    total_trx = int(df["total_trx"].sum())
    promedio  = (df["monto_operacion"] * df["total_trx"]).sum() / total_trx

    top = df.iloc[0]
    monto_top = float(top["monto_operacion"])
    trx_top   = int(top["total_trx"])
    pct_top   = float(top["pct_del_total"])

    print("\n" + "=" * 60)
    print("  SUPERPACK CLARO - ESTADISTICAS DE COMPRAS - ABRIL 2026")
    print("  (canales 1 y 7, solo lempiras, excluye reversas y juridicos)")
    print("=" * 60)
    print(f"  Total transacciones efectivas:  {total_trx:>10,}")
    print(f"  Promedio de compra:             L.{promedio:>9,.2f}")
    print()
    print(f"  Monto mas comprado:             L.{monto_top:>9,.2f}")
    print(f"    -> {trx_top:,} transacciones  ({pct_top:.2f}% del total)")
    print()
    print("  Ranking de montos:")
    print(f"  {'Monto (L.)':>12}  {'Trx':>8}  {'%':>7}")
    print("  " + "-" * 33)
    for _, row in df.iterrows():
        print(
            f"  {float(row['monto_operacion']):>12,.2f}"
            f"  {int(row['total_trx']):>8,}"
            f"  {float(row['pct_del_total']):>6.2f}%"
        )
    print("=" * 60 + "\n")


def main() -> None:
    try:
        print("Consultando estadisticas Superpack Claro abril 2026...")
        df = run_query_file(str(QUERY_PATH))
        imprimir_estadisticas(df)
    except SQLAlchemyError as exc:
        msg = " ".join(str(exc).split())
        lower = msg.lower()
        if "permission was denied" in lower:
            print("[ERROR] Permiso denegado. Solicita permiso SELECT al DBA.")
        elif "login timeout" in lower or "could not open a connection" in lower:
            print("[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales.")
        else:
            print(f"[ERROR] {msg}")
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
