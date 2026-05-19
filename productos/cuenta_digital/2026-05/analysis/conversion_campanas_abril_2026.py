import sys

sys.path.insert(0, ".")

from sqlalchemy.exc import SQLAlchemyError

from core.db import run_query

CAMPAIGN_IDS = (
    48070, 48071, 48072,
    48262, 48263, 48264,
    48338, 48379, 48380,
    48457, 48512, 48513,
    48569, 48608, 48649,
    48735, 48736, 48737,
    48759, 48760, 48812,
    48911, 48912,
)

_ids_str = ", ".join(str(i) for i in CAMPAIGN_IDS)

SQL_POR_CAMPANA = f"""
WITH campanas AS (
    SELECT
        c.CampaignID,
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
        RIGHT('00000000' + RTRIM(LTRIM(d.cldoc)), 8) AS padded_codigo_cliente
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= '2026-04-01'
      AND d.dw_feha_apertura <  '2026-05-01'
      AND d.dw_producto      = 'CUENTA DIGITAL'
      AND d.PRCODP           = 1
      AND d.PRSUBP           = 51
)
SELECT
    cam.CampaignID                                        AS campana,
    COUNT(DISTINCT cam.padded_codigo_cliente)             AS clientes_unicos,
    COUNT(DISTINCT ape.padded_codigo_cliente)             AS apertura_abril,
    COUNT(DISTINCT CASE WHEN ape.padded_codigo_cliente IS NULL
                        THEN cam.padded_codigo_cliente END) AS sin_apertura
FROM campanas cam
LEFT JOIN aperturas ape
    ON cam.padded_codigo_cliente = ape.padded_codigo_cliente
GROUP BY cam.CampaignID
ORDER BY cam.CampaignID
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
    print(f"\nCampañas analizadas : {len(CAMPAIGN_IDS)}")
    print(f"Periodo de apertura : Abril 2026\n")

    try:
        df = run_query(SQL_POR_CAMPANA)

        if df.empty:
            print("Sin datos.")
            return

        for col in ("clientes_unicos", "apertura_abril", "sin_apertura"):
            df[col] = df[col].astype(int)

        total_clientes = int(df["clientes_unicos"].sum())
        total_abril    = int(df["apertura_abril"].sum())
        total_sin      = int(df["sin_apertura"].sum())
        pct_abril      = (total_abril / total_clientes * 100) if total_clientes else 0

        print("===== CONVERSIÓN POR CAMPAÑA — ABRIL 2026 =====")
        print(
            f"{'Campaña':>10}  {'Clientes':>10}  {'Apertura Abr':>13}  {'Sin Apertura':>13}"
        )
        print("-" * 55)
        for _, row in df.iterrows():
            print(
                f"{int(row['campana']):>10,}  "
                f"{int(row['clientes_unicos']):>10,}  "
                f"{int(row['apertura_abril']):>13,}  "
                f"{int(row['sin_apertura']):>13,}"
            )

        print("=" * 55)
        print(
            f"{'TOTAL':>10}  "
            f"{total_clientes:>10,}  "
            f"{total_abril:>13,}  "
            f"{total_sin:>13,}"
        )
        print()

        print("===== RESUMEN GENERAL =====")
        print(f"Clientes únicos en campañas : {total_clientes:>10,}")
        print(f"Aperturaron en Abril        : {total_abril:>10,}  ({pct_abril:.2f}%)")
        print(f"Sin apertura                : {total_sin:>10,}  ({100 - pct_abril:.2f}%)")
        print("===========================\n")

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
