import sys
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from core.db import run_query_file

QUERY_PATH = Path(__file__).resolve().parent / "queries" / "trx_por_dia_superpack_marzo_2026.sql"


def imprimir_resultados(df) -> None:
    if df.empty:
        print("Sin resultados para SUPERPACK-CLARO en marzo 2026.")
        return

    print("\n" + "=" * 50)
    print("  SUPERPACK-CLARO - TRX POR DIA - MARZO 2026")
    print("=" * 50)
    print(f"  {'Fecha':<12} {'Transacciones':>15} {'Clientes':>10}")
    print("-" * 50)

    for _, row in df.iterrows():
        print(f"  {str(row['Fecha']):<12} {int(row['TotalTransacciones']):>15,} {int(row['ClientesUnicos']):>10,}")

    print("-" * 50)
    print(f"  {'TOTAL':<12} {int(df['TotalTransacciones'].sum()):>15,} {'':>10}")
    print("=" * 50 + "\n")


def main() -> None:
    try:
        print("Consultando transacciones por dia SUPERPACK-CLARO marzo 2026...")
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
