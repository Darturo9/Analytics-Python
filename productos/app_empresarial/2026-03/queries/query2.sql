WITH campanas_objetivo AS (
    SELECT
        c.campaignid,
        c.start_date
    FROM dwhbi.dbo.dw_rtm_app_campaign c
    WHERE c.start_date >= '2026-01-01'
      AND c.countryid = 3
      AND c.name LIKE '%app empresarial%'
      AND c.name LIKE '%72049%'
      AND CAST(c.description AS NVARCHAR(MAX)) LIKE '%reg sqvd%'
),
base AS (
    SELECT
        RIGHT('00000000' + y.codigo_sin_dv, 8) AS padded_codigo_cliente,
        co.start_date
    FROM dwhbi.dbo.dw_rtm_app_hiscampaignuniverso h
    INNER JOIN campanas_objetivo co
        ON co.campaignid = h.campaignid
    CROSS APPLY (
        SELECT LTRIM(RTRIM(h.codigo_cliente)) AS codigo_limpio
    ) x
    CROSS APPLY (
        SELECT
            CASE
                WHEN LEN(x.codigo_limpio) > 1
                THEN LEFT(x.codigo_limpio, LEN(x.codigo_limpio) - 1)
                ELSE NULL
            END AS codigo_sin_dv
    ) y
    WHERE y.codigo_sin_dv IS NOT NULL
      AND y.codigo_sin_dv <> ''
),
primer_envio AS (
    SELECT
        padded_codigo_cliente,
        MIN(start_date) AS fecha
    FROM base
    GROUP BY padded_codigo_cliente
)
SELECT
    padded_codigo_cliente,
    MONTH(fecha) AS mes,
    YEAR(fecha) AS año,
    fecha
FROM primer_envio
ORDER BY fecha ASC;
