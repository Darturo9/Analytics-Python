-- Top dias con mas cuentas fondeadas (Q1 2026)
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
resumen_diario AS (
    SELECT
        CAST(h.dw_fecha_informacion AS DATE) AS fecha,
        COUNT(DISTINCT h.DW_CUENTA_CORPORATIVA) AS cuentas_fondeadas
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN universo_cuentas u
        ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_fecha_informacion >= '2026-01-01'
      AND h.dw_fecha_informacion <  '2026-04-01'
      AND h.ctt001 > 0
    GROUP BY CAST(h.dw_fecha_informacion AS DATE)
)
SELECT
    fecha,
    cuentas_fondeadas,
    ROW_NUMBER() OVER (ORDER BY cuentas_fondeadas DESC, fecha ASC) AS ranking_dia
FROM resumen_diario
ORDER BY cuentas_fondeadas DESC, fecha ASC;
