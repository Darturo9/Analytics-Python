import sys

sys.path.insert(0, ".")

from sqlalchemy.exc import SQLAlchemyError

from core.db import run_query

# Campañas árbol Sin Login
CAMPAIGN_IDS = (47516, 47619, 47723, 47955, 48101, 48311, 48514, 48739)

SQL = """
WITH campanas_abril AS (
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
    WHERE c.CampaignID IN (47516, 47619, 47723, 47955, 48101, 48311, 48514, 48739)
      AND CAST(h.FechaAplica AS DATE) >= '2026-04-01'
      AND CAST(h.FechaAplica AS DATE) <  '2026-05-01'
),
logins_abril AS (
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(t.clccli)), 8) AS padded_codigo_cliente,
        COUNT(*)                                        AS total_logins
    FROM dw_bel_IBSTTRA_VIEW t
    WHERE t.dw_fecha_trx >= '2026-04-01'
      AND t.dw_fecha_trx <  '2026-05-01'
      AND t.secode IN ('app-login', 'web-login', 'login')
      AND t.clccli IS NOT NULL
    GROUP BY RTRIM(LTRIM(t.clccli))
),
cambios_pass_abril AS (
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(u.CLCCLI)), 8) AS padded_codigo_cliente,
        COUNT(*)                                        AS total_cambios
    FROM DW_BEL_IBUSER u
    WHERE u.dw_fecha_cambio_pass >= '2026-04-01'
      AND u.dw_fecha_cambio_pass <  '2026-05-01'
      AND u.CLCCLI IS NOT NULL
    GROUP BY RTRIM(LTRIM(u.CLCCLI))
)
SELECT
    COUNT(DISTINCT cam.padded_codigo_cliente)
        AS clientes_contactados_abril,
    COUNT(DISTINCT CASE WHEN l.padded_codigo_cliente IS NOT NULL
        THEN cam.padded_codigo_cliente END)
        AS clientes_unicos_login,
    COALESCE(SUM(l.total_logins), 0)
        AS total_logins,
    COUNT(DISTINCT CASE WHEN cp.padded_codigo_cliente IS NOT NULL
        THEN cam.padded_codigo_cliente END)
        AS clientes_unicos_cambio_pass,
    COALESCE(SUM(cp.total_cambios), 0)
        AS total_cambios_pass
FROM campanas_abril cam
LEFT JOIN logins_abril l
    ON cam.padded_codigo_cliente = l.padded_codigo_cliente
LEFT JOIN cambios_pass_abril cp
    ON cam.padded_codigo_cliente = cp.padded_codigo_cliente
"""


def pct(parte, total):
    return (parte / total * 100) if total > 0 else 0


def construir_error_amigable(exc: Exception) -> str:
    raw = " ".join(str(exc).split())
    lower = raw.lower()
    if "permission was denied" in lower:
        return "[ERROR] Permiso denegado. Solicita permiso SELECT al DBA."
    if "login timeout expired" in lower or "could not open a connection" in lower:
        return "[ERROR] No se pudo conectar a SQL Server. Verifica red/VPN y credenciales."
    return f"[ERROR] Fallo ejecutando la consulta: {raw}"


def main() -> None:
    print("\nÁrbol Sin Login — Contactados, Login y Cambio de Contraseña")
    print("Periodo de contacto : abril 2026 (FechaAplica)")
    print(f"Campañas            : {', '.join(str(i) for i in CAMPAIGN_IDS)}")

    try:
        df = run_query(SQL)

        if df.empty:
            print("No se encontraron datos.")
            return

        row = df.iloc[0]
        contactados    = int(row["clientes_contactados_abril"])
        unicos_login   = int(row["clientes_unicos_login"])
        total_logins   = int(row["total_logins"])
        unicos_pass    = int(row["clientes_unicos_cambio_pass"])
        total_pass     = int(row["total_cambios_pass"])

        print("\n" + "=" * 50)
        print(f"  Clientes contactados en abril  : {contactados:>8,}")
        print("-" * 50)
        print(f"  LOGIN")
        print(f"    Clientes únicos              : {unicos_login:>8,}  ({pct(unicos_login, contactados):.2f}%)")
        print(f"    Total eventos de login       : {total_logins:>8,}")
        print("-" * 50)
        print(f"  CAMBIO DE CONTRASEÑA")
        print(f"    Clientes únicos              : {unicos_pass:>8,}  ({pct(unicos_pass, contactados):.2f}%)")
        print(f"    Total cambios de contraseña  : {total_pass:>8,}")
        print("=" * 50)

    except SQLAlchemyError as exc:
        print(construir_error_amigable(exc))
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
