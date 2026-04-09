-- Cuentas fondeadas unicas en todo el trimestre Q1 2026
-- Regla: si una cuenta fondea en enero y marzo, cuenta solo 1 vez en Q1.
-- Universo: cuentas CD que abrieron entre enero y marzo 2026.

WITH UniversoCuentas AS (
    SELECT DISTINCT
        DW_CUENTA_CORPORATIVA
    FROM dw_dep_depositos
    WHERE dw_feha_apertura >= '2026-01-01'
      AND dw_feha_apertura <  '2026-04-01'
      AND dw_producto = 'CUENTA DIGITAL'
      AND PRCODP = 1
      AND PRSUBP = 51
)
SELECT
    'Q1 2026' AS trimestre,
    1 AS orden,
    COUNT(DISTINCT u.DW_CUENTA_CORPORATIVA) AS cuentas_abiertas_q1,
    COUNT(DISTINCT CASE
        WHEN h.dw_fecha_informacion >= '2026-01-01'
         AND h.dw_fecha_informacion <  '2026-04-01'
         AND h.ctt001 > 0
        THEN u.DW_CUENTA_CORPORATIVA
    END) AS cuentas_fondeadas_unicas_q1
FROM UniversoCuentas u
LEFT JOIN HIS_DEP_DEPOSITOS_VIEW h
    ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA;
