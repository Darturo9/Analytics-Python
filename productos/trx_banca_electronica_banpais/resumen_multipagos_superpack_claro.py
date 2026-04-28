import sys
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.db import run_query, run_query_file

QUERY_PATH = Path(__file__).resolve().parent / "queries" / "Query_Multipagos_SuperpackClaro_Abril2026_Resumen.sql"

SQL_TOTAL_CLIENTES = """
SELECT COUNT(DISTINCT ClientesBel.CLCCLI) AS TotalClientesUnicos
FROM DW_MUL_SPMACO
  INNER JOIN DW_MUL_SPPADAT ON (DW_MUL_SPMACO.SPCODC = DW_MUL_SPPADAT.SPCODC)
  LEFT JOIN (
    SELECT LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
           LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
    FROM DW_BEL_IBUSER
  ) ClientesBel ON LTRIM(RTRIM(DW_MUL_SPPADAT.SPINUS)) = (ClientesBel.CLCCLI + ClientesBel.USCODE)
  LEFT JOIN (
    SELECT LTRIM(RTRIM(CLDOC)) CLDOC, CLTIPE,
      ROW_NUMBER() OVER (PARTITION BY LTRIM(RTRIM(CLDOC)) ORDER BY CLDOC) AS RN
    FROM DW_CIF_CLIENTES
  ) CIF ON CIF.CLDOC = ClientesBel.CLCCLI AND CIF.RN = 1
WHERE
  DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP >= '2026-04-01'
  AND DW_MUL_SPPADAT.DW_FECHA_OPERACION_SP <  '2026-05-01'
  AND DW_MUL_SPPADAT.SPCPCO IN (1, 7)
  AND DW_MUL_SPMACO.SPNOMC = 'SUPERPACK-CLARO'
  AND (CIF.CLTIPE <> 'J' OR CIF.CLTIPE IS NULL)
"""


def imprimir_resultados(df, total_clientes_unicos: int) -> None:
    if df.empty:
        print("Sin resultados para SUPERPACK-CLARO en abril 2026.")
        return

    print("\n" + "=" * 60)
    print("  MULTIPAGOS - SUPERPACK-CLARO - ABRIL 2026")
    print("=" * 60)

    totales = {
        "TotalTransacciones": int(df["TotalTransacciones"].sum()),
        "MontoTotal":         float(df["MontoTotal"].sum()),
        "MontoTotalLempiras": float(df["MontoTotalLempiras"].sum()),
        "MontoTotalDolares":  float(df["MontoTotalDolares"].sum()),
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
    print(f"  Clientes unicos     : {total_clientes_unicos:,}  (sin duplicar por canal)")
    print(f"  Transacciones       : {totales['TotalTransacciones']:,}")
    print(f"  Monto total         : {totales['MontoTotal']:>18,.2f}")
    print(f"  Monto en Lempiras   : L {totales['MontoTotalLempiras']:>15,.2f}")
    print(f"  Monto en Dolares    : $ {totales['MontoTotalDolares']:>15,.2f}")
    print("=" * 60 + "\n")


def main() -> None:
    try:
        print("Consultando SUPERPACK-CLARO en SQL Server...")
        df = run_query_file(str(QUERY_PATH))
        df_total = run_query(SQL_TOTAL_CLIENTES)
        total_clientes_unicos = int(df_total["TotalClientesUnicos"].iloc[0])
        imprimir_resultados(df, total_clientes_unicos)
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
