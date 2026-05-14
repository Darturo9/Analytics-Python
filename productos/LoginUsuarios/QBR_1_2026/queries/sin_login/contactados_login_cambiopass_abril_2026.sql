/*
==============================================================================
contactados_login_cambiopass_abril_2026.sql
------------------------------------------------------------------------------
Clientes del árbol Sin Login contactados con FechaAplica en abril 2026,
cuántos hicieron login y cuántos cambiaron contraseña en el mismo mes.

Campañas:
  47516 - Email  16/03  Oferta Inicial
  47619 - SMS    18/03  Recordatorio 1
  47723 - Email  21/03  Recordatorio 2
  47955 - SMS    28/03  Recordatorio 3
  48101 - Email  04/04  Recordatorio 4
  48311 - SMS    11/04  Recordatorio 5
  48514 - Email  18/04  Recordatorio 6
  48739 - SMS    25/04  Recordatorio 7

Reglas:
  - Contactado en abril = FechaAplica entre 2026-04-01 y 2026-04-30
  - Login y cambio de contraseña: cualquier evento en abril (no requiere
    ser posterior al contacto)
==============================================================================
*/

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
    ON cam.padded_codigo_cliente = cp.padded_codigo_cliente;
