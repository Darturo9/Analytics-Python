import sys
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.db import run_query_file

QUERY_PATH = Path(__file__).resolve().parent / "queries" / "Query_Multipagos_SuperpackClaro_Abril2026_Resumen.sql"


def imprimir_resultados(df) -> None:
    if df.empty:
        print("Sin resultados para SUPERPACK-CLARO en abril 2026.")
        return

    print("\n" + "=" * 60)
    print("  MULTIPAGOS - SUPERPACK-CLARO - ABRIL 2026")
    print("=" * 60)

    totales = {
        "TotalClientes":       int(df["TotalClientes"].sum()),
        "TotalTransacciones":  int(df["TotalTransacciones"].sum()),
        "MontoTotal":          float(df["MontoTotal"].sum()),
        "MontoTotalLempiras":  float(df["MontoTotalLempiras"].sum()),
        "MontoTotalDolares":   float(df["MontoTotalDolares"].sum()),
    }

    print("\nPOR CANAL / TIPO BANCA / TIPO CLIENTE:")
    print("-" * 60)
    for _, row in df.iterrows():
        print(f"  Canal               : {row['Canal']}")
        print(f"  Tipo Banca          : {row['TipoBanca']}")
        print(f"  Tipo Cliente        : {row['Tipo_Cliente']}")
        print(f"  Clientes unicos     : {int(row['TotalClientes']):,}")
        print(f"  Transacciones       : {int(row['TotalTransacciones']):,}")
        print(f"  Monto total         : {float(row['MontoTotal']):>18,.2f}")
        print(f"  Monto en Lempiras   : L {float(row['MontoTotalLempiras']):>15,.2f}")
        print(f"  Monto en Dolares    : $ {float(row['MontoTotalDolares']):>15,.2f}")
        print("-" * 60)

    print("\nTOTAL GENERAL:")
    print("-" * 60)
    print(f"  Clientes unicos     : {totales['TotalClientes']:,}")
    print(f"  Transacciones       : {totales['TotalTransacciones']:,}")
    print(f"  Monto total         : {totales['MontoTotal']:>18,.2f}")
    print(f"  Monto en Lempiras   : L {totales['MontoTotalLempiras']:>15,.2f}")
    print(f"  Monto en Dolares    : $ {totales['MontoTotalDolares']:>15,.2f}")
    print("=" * 60 + "\n")


def main() -> None:
    try:
        print("Consultando SUPERPACK-CLARO en SQL Server...")
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
