-- Cuentas fondeadas unicas por semana (Q1 2026)
-- Universo: cuentas de CUENTA DIGITAL abiertas en Q1 2026.

WITH universo_cuentas AS (
    SELECT DISTINCT
        d.DW_CUENTA_CORPORATIVA
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= '2026-01-01'
      AND d.dw_feha_apertura <  '2026-04-01'
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
),
base_fondeo_semanal AS (
    SELECT
        CAST(
            DATEADD(
                DAY,
                -((DATEPART(WEEKDAY, CAST(h.dw_fecha_informacion AS DATE)) + @@DATEFIRST - 2) % 7),
                CAST(h.dw_fecha_informacion AS DATE)
            ) AS DATE
        ) AS semana_inicio,
        h.DW_CUENTA_CORPORATIVA
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN universo_cuentas u
        ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_fecha_informacion >= '2026-01-01'
      AND h.dw_fecha_informacion <  '2026-04-01'
      AND h.ctt001 > 0
    GROUP BY
        CAST(
            DATEADD(
                DAY,
                -((DATEPART(WEEKDAY, CAST(h.dw_fecha_informacion AS DATE)) + @@DATEFIRST - 2) % 7),
                CAST(h.dw_fecha_informacion AS DATE)
            ) AS DATE
        ),
        h.DW_CUENTA_CORPORATIVA
)
SELECT
    semana_inicio,
    DATEADD(DAY, 6, semana_inicio) AS semana_fin,
    COUNT(DISTINCT DW_CUENTA_CORPORATIVA) AS cuentas_fondeadas,
    ROW_NUMBER() OVER (ORDER BY semana_inicio ASC) AS orden_semana
FROM base_fondeo_semanal
GROUP BY semana_inicio
ORDER BY semana_inicio ASC;
