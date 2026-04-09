-- Cuentas fondeadas unicas por mes (Q1 2026)
-- Universo: cuentas que abrieron entre enero y marzo 2026
-- Regla: una cuenta cuenta 1 vez por mes si tuvo saldo > 0 en ese mes,
-- sin importar en que mes del Q1 abrio.

WITH UniversoCuentas AS (
    SELECT DISTINCT
        DW_CUENTA_CORPORATIVA
    FROM dw_dep_depositos
    WHERE dw_feha_apertura BETWEEN '2026-01-01' AND '2026-03-31'
      AND dw_producto = 'CUENTA DIGITAL'
      AND PRCODP = 1
      AND PRSUBP = 51
)
SELECT
    'Enero 2026' AS mes,
    1 AS orden,
    COUNT(DISTINCT CASE
        WHEN h.dw_fecha_informacion BETWEEN '2026-01-01' AND '2026-01-31'
         AND h.ctt001 > 0
        THEN h.DW_CUENTA_CORPORATIVA
    END) AS cuentas_fondeadas
FROM UniversoCuentas u
LEFT JOIN HIS_DEP_DEPOSITOS_VIEW h
    ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA

UNION ALL

SELECT
    'Febrero 2026' AS mes,
    2 AS orden,
    COUNT(DISTINCT CASE
        WHEN h.dw_fecha_informacion BETWEEN '2026-02-01' AND '2026-02-28'
         AND h.ctt001 > 0
        THEN h.DW_CUENTA_CORPORATIVA
    END) AS cuentas_fondeadas
FROM UniversoCuentas u
LEFT JOIN HIS_DEP_DEPOSITOS_VIEW h
    ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA

UNION ALL

SELECT
    'Marzo 2026' AS mes,
    3 AS orden,
    COUNT(DISTINCT CASE
        WHEN h.dw_fecha_informacion BETWEEN '2026-03-01' AND '2026-03-31'
         AND h.ctt001 > 0
        THEN h.DW_CUENTA_CORPORATIVA
    END) AS cuentas_fondeadas
FROM UniversoCuentas u
LEFT JOIN HIS_DEP_DEPOSITOS_VIEW h
    ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA

ORDER BY orden;
