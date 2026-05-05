"""
compradores_superpack_120_marzo.py
------------------------------------
Reporte de clientes del universo completo de compradores de Superpack Claro
en marzo 2026 que realizaron al menos una compra >= L.120 (no revertida).

Ejecucion:
    python3 "productos/Superpack Claro/marzo 2026/compradores_superpack_120_marzo.py"
"""

import sys
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from core.db import run_query_file

QUERY_PATH = Path(__file__).resolve().parent / "queries" / "compradores_superpack_120_marzo_2026.sql"


def imprimir_resultados(df) -> None:
    if df.empty:
        print("Sin resultados para marzo 2026.")
        return

    row = df.iloc[0]
    total   = int(row["total_compradores"])
    con_120 = int(row["compradores_120_o_mas"])
    sin_120 = int(row["compradores_menos_120"])
    pct     = float(row["pct_120_o_mas"]) if row["pct_120_o_mas"] is not None else 0.0

    print("\n" + "=" * 60)
    print("  SUPERPACK CLARO - COMPRADORES >= L.120 - MARZO 2026")
    print("  (universo completo, canales 1 y 7, excluye juridicos)")
    print("=" * 60)
    print(f"  Total compradores efectivos marzo:    {total:>10,}")
    print(f"  Con al menos 1 trx >= L.120:          {con_120:>10,}  ({pct:.2f}%)")
    print(f"  Sin ninguna trx >= L.120:             {sin_120:>10,}  ({100 - pct:.2f}%)")
    print("=" * 60 + "\n")


def main() -> None:
    try:
        print("Consultando compradores Superpack >= L.120 en marzo 2026...")
        df = run_query_file(str(QUERY_PATH))
        imprimir_resultados(df)
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
