import sys

sys.path.insert(0, ".")

from sqlalchemy.exc import SQLAlchemyError

from core.db import run_query

# ── IDs de campañas ─────────────────────────────────────────────────────────
CAMPAIGN_IDS = (
    # Grupo A
    47188, 47190, 47194, 47245, 47246, 47339, 47370, 47446, 47448,
    47494, 47499, 47500, 47668, 47670, 47671, 47750, 47789, 47848,
    47908, 47909, 47911, 48026, 48028, 48029,
    # Grupo B
    48070, 48071, 48072, 48262, 48263, 48264, 48338, 48379, 48380,
    48457, 48512, 48513, 48569, 48608, 48649, 48735, 48736, 48737,
    48759, 48760, 48812, 48911, 48912,
    # Grupo C
    48950, 48952, 49027, 49058, 49220,
)

_ids_str = ", ".join(str(i) for i in CAMPAIGN_IDS)

SQL_RESUMEN = f"""
WITH campanas AS (
    SELECT DISTINCT
        RIGHT(
            '00000000' + LEFT(
                RTRIM(LTRIM(h.Codigo_Cliente)),
                LEN(RTRIM(LTRIM(h.Codigo_Cliente))) - 1
            ),
            8
        ) AS padded_codigo_cliente
    FROM DWHBI.dbo.DW_RTM_APP_CAMPAIGN c
    INNER JOIN DWHBI.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO h
        ON h.CampaignID = c.CampaignID
    WHERE c.CampaignID IN ({_ids_str})
),
aperturas AS (
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(d.cldoc)), 8) AS padded_codigo_cliente,
        d.dw_cuenta_corporativa                       AS numero_cuenta,
        ROW_NUMBER() OVER (
            PARTITION BY RTRIM(LTRIM(d.cldoc))
            ORDER BY d.dw_feha_apertura ASC
        ) AS rn
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= '2026-03-01'
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP      = 1
      AND d.PRSUBP      = 51
),
aperturas_dedup AS (
    SELECT padded_codigo_cliente, numero_cuenta
    FROM aperturas
    WHERE rn = 1
)
SELECT
    COUNT(DISTINCT cam.padded_codigo_cliente) AS total_clientes_campanas,
    COUNT(DISTINCT ape.numero_cuenta)          AS total_abrieron_cuenta
FROM campanas cam
LEFT JOIN aperturas_dedup ape
    ON cam.padded_codigo_cliente = ape.padded_codigo_cliente
"""

SQL_POR_GENERACION = f"""
WITH campanas AS (
    SELECT DISTINCT
        RIGHT(
            '00000000' + LEFT(
                RTRIM(LTRIM(h.Codigo_Cliente)),
                LEN(RTRIM(LTRIM(h.Codigo_Cliente))) - 1
            ),
            8
        ) AS padded_codigo_cliente
    FROM DWHBI.dbo.DW_RTM_APP_CAMPAIGN c
    INNER JOIN DWHBI.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO h
        ON h.CampaignID = c.CampaignID
    WHERE c.CampaignID IN ({_ids_str})
),
aperturas AS (
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(d.cldoc)), 8) AS padded_codigo_cliente,
        d.dw_cuenta_corporativa                       AS numero_cuenta,
        CASE
            WHEN YEAR(cli.DW_FECHA_NACIMIENTO) BETWEEN 1965 AND 1980
                THEN 'Generation X (1965-1980)'
            WHEN YEAR(cli.DW_FECHA_NACIMIENTO) BETWEEN 1981 AND 1996
                THEN 'Gen Y - Millennials (1981-1996)'
            WHEN YEAR(cli.DW_FECHA_NACIMIENTO) BETWEEN 1997 AND 2012
                THEN 'Generación Z (1997-2012)'
            ELSE 'Otra Generación'
        END AS generacion,
        ROW_NUMBER() OVER (
            PARTITION BY RTRIM(LTRIM(d.cldoc))
            ORDER BY d.dw_feha_apertura ASC
        ) AS rn
    FROM dw_dep_depositos d
    LEFT JOIN dw_cif_clientes cli
        ON RTRIM(LTRIM(d.cldoc)) = RTRIM(LTRIM(cli.cldoc))
    WHERE d.dw_feha_apertura >= '2026-03-01'
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP      = 1
      AND d.PRSUBP      = 51
),
aperturas_dedup AS (
    SELECT padded_codigo_cliente, numero_cuenta, generacion
    FROM aperturas
    WHERE rn = 1
)
SELECT
    ape.generacion,
    COUNT(DISTINCT cam.padded_codigo_cliente) AS clientes_unicos,
    COUNT(DISTINCT ape.numero_cuenta)          AS cuentas_aperturadas
FROM campanas cam
INNER JOIN aperturas_dedup ape
    ON cam.padded_codigo_cliente = ape.padded_codigo_cliente
GROUP BY ape.generacion
ORDER BY cuentas_aperturadas DESC
"""


SQL_POR_MES_GENERACION = f"""
WITH campanas AS (
    SELECT DISTINCT
        RIGHT(
            '00000000' + LEFT(
                RTRIM(LTRIM(h.Codigo_Cliente)),
                LEN(RTRIM(LTRIM(h.Codigo_Cliente))) - 1
            ),
            8
        ) AS padded_codigo_cliente
    FROM DWHBI.dbo.DW_RTM_APP_CAMPAIGN c
    INNER JOIN DWHBI.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO h
        ON h.CampaignID = c.CampaignID
    WHERE c.CampaignID IN ({_ids_str})
),
aperturas AS (
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(d.cldoc)), 8) AS padded_codigo_cliente,
        d.dw_cuenta_corporativa                       AS numero_cuenta,
        DATEFROMPARTS(YEAR(d.dw_feha_apertura), MONTH(d.dw_feha_apertura), 1) AS mes_apertura,
        CASE
            WHEN YEAR(cli.DW_FECHA_NACIMIENTO) BETWEEN 1965 AND 1980
                THEN 'Generation X (1965-1980)'
            WHEN YEAR(cli.DW_FECHA_NACIMIENTO) BETWEEN 1981 AND 1996
                THEN 'Gen Y - Millennials (1981-1996)'
            WHEN YEAR(cli.DW_FECHA_NACIMIENTO) BETWEEN 1997 AND 2012
                THEN 'Generación Z (1997-2012)'
            ELSE 'Otra Generación'
        END AS generacion,
        ROW_NUMBER() OVER (
            PARTITION BY RTRIM(LTRIM(d.cldoc))
            ORDER BY d.dw_feha_apertura ASC
        ) AS rn
    FROM dw_dep_depositos d
    LEFT JOIN dw_cif_clientes cli
        ON RTRIM(LTRIM(d.cldoc)) = RTRIM(LTRIM(cli.cldoc))
    WHERE d.dw_feha_apertura >= '2026-03-01'
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP      = 1
      AND d.PRSUBP      = 51
),
aperturas_dedup AS (
    SELECT padded_codigo_cliente, numero_cuenta, mes_apertura, generacion
    FROM aperturas
    WHERE rn = 1
)
SELECT
    CONVERT(VARCHAR(7), ape.mes_apertura, 120) AS mes_apertura,
    ape.generacion,
    COUNT(DISTINCT ape.numero_cuenta)           AS cuentas_aperturadas
FROM campanas cam
INNER JOIN aperturas_dedup ape
    ON cam.padded_codigo_cliente = ape.padded_codigo_cliente
GROUP BY ape.mes_apertura, ape.generacion
ORDER BY ape.mes_apertura ASC, cuentas_aperturadas DESC
"""


def construir_error_amigable(exc: Exception) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()
    if "permission was denied" in lower:
        return "[ERROR] Permiso denegado. Solicita permiso SELECT al DBA."
    if "login timeout expired" in lower or "could not open a connection" in lower:
        return "[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales."
    return f"[ERROR] Fallo ejecutando la consulta: {raw}"


def main() -> None:
    print(f"\nCampañas analizadas: {len(CAMPAIGN_IDS)}")

    try:
        df_resumen = run_query(SQL_RESUMEN)

        if df_resumen.empty:
            print("No se encontraron datos.")
            return

        total_campanas = int(df_resumen["total_clientes_campanas"].iloc[0])
        total_abrieron = int(df_resumen["total_abrieron_cuenta"].iloc[0])
        pct = (total_abrieron / total_campanas * 100) if total_campanas > 0 else 0

        print("\n===== RESUMEN GENERAL =====")
        print(f"Clientes únicos contactados : {total_campanas:>10,}")
        print(f"Abrieron Cuenta Digital     : {total_abrieron:>10,}  ({pct:.2f}%)")
        print("===========================")

        if total_abrieron == 0:
            print("\nNingún cliente de las campañas abrió cuenta en el periodo.")
            return

        df_gen = run_query(SQL_POR_GENERACION)

        if df_gen.empty:
            print("No hay datos de generación.")
            return

        df_gen["clientes_unicos"] = df_gen["clientes_unicos"].astype(int)
        df_gen["cuentas_aperturadas"] = df_gen["cuentas_aperturadas"].astype(int)
        df_gen["pct_del_total"] = (
            df_gen["cuentas_aperturadas"] / total_abrieron * 100
        ).round(2)

        tabla = df_gen.copy()
        tabla["clientes_unicos"] = tabla["clientes_unicos"].map(lambda x: f"{x:,}")
        tabla["cuentas_aperturadas"] = tabla["cuentas_aperturadas"].map(lambda x: f"{x:,}")
        tabla["pct_del_total"] = tabla["pct_del_total"].map(lambda x: f"{x:.2f}%")

        print("\n===== APERTURAS POR GENERACIÓN =====")
        print(tabla.to_string(index=False))
        print(f"\nTotal aperturas: {total_abrieron:,}")
        print("=====================================")

        df_mes = run_query(SQL_POR_MES_GENERACION)

        if not df_mes.empty:
            df_mes["cuentas_aperturadas"] = df_mes["cuentas_aperturadas"].astype(int)

            print("\n===== APERTURAS POR MES Y GENERACIÓN =====")
            for mes, grupo in df_mes.groupby("mes_apertura", sort=False):
                subtotal = int(grupo["cuentas_aperturadas"].sum())
                print(f"\n  {mes}  (total: {subtotal:,})")
                for _, row in grupo.iterrows():
                    print(f"    {row['generacion']:<35} {row['cuentas_aperturadas']:>6,}")
            print("\n==========================================")

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
