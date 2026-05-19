import sys

sys.path.insert(0, ".")

from sqlalchemy.exc import SQLAlchemyError

from core.db import run_query

CAMPAIGN_IDS = (
    45538, 45704, 45750, 45841, 45919, 45998,
    46105, 46151, 46280, 46383, 46498, 46707, 46944,
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
        RIGHT('00000000' + RTRIM(LTRIM(d.cldoc)), 8) AS padded_codigo_cliente,
        MONTH(d.dw_feha_apertura)                     AS mes
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= '2026-01-01'
      AND d.dw_feha_apertura <  '2026-03-01'
      AND d.dw_producto      = 'CUENTA DIGITAL'
      AND d.PRCODP           = 1
      AND d.PRSUBP           = 51
)
SELECT
    cam.CampaignID                                                     AS campana,
    COUNT(DISTINCT cam.padded_codigo_cliente)                          AS clientes_unicos,
    COUNT(DISTINCT CASE WHEN ape.mes = 1 THEN ape.padded_codigo_cliente END) AS apertura_enero,
    COUNT(DISTINCT CASE WHEN ape.mes = 2 THEN ape.padded_codigo_cliente END) AS apertura_febrero,
    COUNT(DISTINCT ape.padded_codigo_cliente)                          AS apertura_total
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
    print(f"Periodo de apertura : Enero – Febrero 2026\n")

    try:
        df = run_query(SQL_POR_CAMPANA)

        if df.empty:
            print("Sin datos.")
            return

        for col in ("clientes_unicos", "apertura_enero", "apertura_febrero", "apertura_total"):
            df[col] = df[col].astype(int)

        # ── Totales generales ────────────────────────────────────────────────
        total_clientes = int(df["clientes_unicos"].sum())
        total_enero    = int(df["apertura_enero"].sum())
        total_febrero  = int(df["apertura_febrero"].sum())
        total_general  = int(df["apertura_total"].sum())
        pct_enero      = (total_enero   / total_clientes * 100) if total_clientes else 0
        pct_febrero    = (total_febrero / total_clientes * 100) if total_clientes else 0
        pct_total      = (total_general / total_clientes * 100) if total_clientes else 0

        # ── Tabla por campaña ────────────────────────────────────────────────
        tabla = df.copy()
        for col in ("clientes_unicos", "apertura_enero", "apertura_febrero", "apertura_total"):
            tabla[col] = tabla[col].map(lambda x: f"{x:,}")

        print("===== CONVERSIÓN POR CAMPAÑA =====")
        print(
            f"{'Campaña':>10}  {'Clientes':>10}  {'Apertura Ene':>13}  "
            f"{'Apertura Feb':>13}  {'Total Apert':>12}"
        )
        print("-" * 67)
        for _, row in df.iterrows():
            print(
                f"{int(row['campana']):>10,}  "
                f"{int(row['clientes_unicos']):>10,}  "
                f"{int(row['apertura_enero']):>13,}  "
                f"{int(row['apertura_febrero']):>13,}  "
                f"{int(row['apertura_total']):>12,}"
            )

        print("=" * 67)
        print(
            f"{'TOTAL':>10}  "
            f"{total_clientes:>10,}  "
            f"{total_enero:>13,}  "
            f"{total_febrero:>13,}  "
            f"{total_general:>12,}"
        )
        print()

        # ── Resumen general ──────────────────────────────────────────────────
        print("===== RESUMEN GENERAL =====")
        print(f"Clientes únicos en campañas : {total_clientes:>10,}")
        print(f"Aperturaron en Enero        : {total_enero:>10,}  ({pct_enero:.2f}%)")
        print(f"Aperturaron en Febrero      : {total_febrero:>10,}  ({pct_febrero:.2f}%)")
        print(f"Total con apertura (ene+feb): {total_general:>10,}  ({pct_total:.2f}%)")
        print("===========================\n")

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
