import argparse
import sys

sys.path.insert(0, ".")

from sqlalchemy.exc import SQLAlchemyError

from core.db import run_query


SQL_RANGOS_EDAD_APERTURAS = """
WITH aperturas AS (
    SELECT
        d.dw_cuenta_corporativa AS numero_cuenta,
        CAST(d.dw_feha_apertura AS DATE) AS fecha_apertura,
        CAST(c.dw_fecha_nacimiento AS DATE) AS fecha_nacimiento
    FROM dw_dep_depositos d
    LEFT JOIN dw_cif_clientes c
        ON RTRIM(LTRIM(d.cldoc)) = RTRIM(LTRIM(c.cldoc))
    WHERE d.dw_feha_apertura >= :fecha_inicio
      AND d.dw_feha_apertura <  :fecha_fin_exclusiva
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
),
edades AS (
    SELECT
        numero_cuenta,
        fecha_apertura,
        fecha_nacimiento,
        CASE
            WHEN fecha_nacimiento IS NULL THEN NULL
            ELSE
                DATEDIFF(YEAR, fecha_nacimiento, fecha_apertura)
                - CASE
                    WHEN DATEADD(
                        YEAR,
                        DATEDIFF(YEAR, fecha_nacimiento, fecha_apertura),
                        fecha_nacimiento
                    ) > fecha_apertura THEN 1
                    ELSE 0
                  END
        END AS edad
    FROM aperturas
),
rangos AS (
    SELECT
        CASE
            WHEN edad IS NULL THEN 'Sin fecha nacimiento'
            WHEN edad < 18 THEN 'Menor de 18'
            WHEN edad BETWEEN 18 AND 24 THEN '18-24'
            WHEN edad BETWEEN 25 AND 34 THEN '25-34'
            WHEN edad BETWEEN 35 AND 44 THEN '35-44'
            WHEN edad BETWEEN 45 AND 54 THEN '45-54'
            WHEN edad BETWEEN 55 AND 64 THEN '55-64'
            ELSE '65+'
        END AS rango_edad,
        numero_cuenta
    FROM edades
)
SELECT
    rango_edad,
    COUNT(DISTINCT numero_cuenta) AS cuentas_aperturadas
FROM rangos
GROUP BY rango_edad
ORDER BY
    CASE rango_edad
        WHEN 'Menor de 18' THEN 1
        WHEN '18-24' THEN 2
        WHEN '25-34' THEN 3
        WHEN '35-44' THEN 4
        WHEN '45-54' THEN 5
        WHEN '55-64' THEN 6
        WHEN '65+' THEN 7
        WHEN 'Sin fecha nacimiento' THEN 8
        ELSE 99
    END
"""


def construir_error_amigable(exc: Exception) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()

    if "permission was denied" in lower:
        return "[ERROR] Permiso denegado al consultar SQL Server. Solicita permiso SELECT al DBA."
    if "login timeout expired" in lower or "could not open a connection" in lower:
        return "[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales."
    return f"[ERROR] Fallo ejecutando la consulta: {raw}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resumen por rango de edad de cuentas digitales aperturadas en abril 2026."
    )
    parser.add_argument("--fecha-inicio", default="2026-04-01", help="Fecha inicio inclusiva (YYYY-MM-DD).")
    parser.add_argument(
        "--fecha-fin-exclusiva",
        default="2026-05-01",
        help="Fecha fin exclusiva (YYYY-MM-DD).",
    )
    args = parser.parse_args()

    try:
        df = run_query(
            SQL_RANGOS_EDAD_APERTURAS,
            params={
                "fecha_inicio": args.fecha_inicio,
                "fecha_fin_exclusiva": args.fecha_fin_exclusiva,
            },
        )

        if df.empty:
            print("No se encontraron aperturas de cuenta digital en el periodo indicado.")
            return

        total_cuentas = int(df["cuentas_aperturadas"].sum())
        tabla = df.copy()
        tabla["cuentas_aperturadas"] = tabla["cuentas_aperturadas"].astype(int).map(lambda x: f"{x:,}")

        print("\n===== CUENTA DIGITAL: APERTURAS POR RANGO DE EDAD =====")
        print(f"Periodo: {args.fecha_inicio} a {args.fecha_fin_exclusiva} (fin exclusiva)")
        print(tabla.to_string(index=False))
        print("--------------------------------------------------------")
        print(f"Total cuentas aperturadas: {total_cuentas:,}")
        print("========================================================")

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
