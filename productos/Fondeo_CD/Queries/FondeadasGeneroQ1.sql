-- Cuentas fondeadas por genero (Q1 2026)
-- Universo: cuentas de CUENTA DIGITAL abiertas en Q1 2026
-- Regla de fondeo: saldo diario (ctt001) > 0 en al menos un dia del Q1.

WITH universo_cuentas AS (
    SELECT
        d.DW_CUENTA_CORPORATIVA,
        LTRIM(RTRIM(d.CLDOC)) AS codigo_cliente
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= '2026-01-01'
      AND d.dw_feha_apertura <  '2026-04-01'
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
),
cuentas_fondeadas_q1 AS (
    SELECT DISTINCT
        h.DW_CUENTA_CORPORATIVA
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN universo_cuentas u
        ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_fecha_informacion >= '2026-01-01'
      AND h.dw_fecha_informacion <  '2026-04-01'
      AND h.ctt001 > 0
)
SELECT
    CASE
        WHEN UPPER(LTRIM(RTRIM(c.clisex))) IN ('F', 'FEMENINO', 'MUJER') THEN 'MUJER'
        WHEN UPPER(LTRIM(RTRIM(c.clisex))) IN ('M', 'H', 'MASCULINO', 'HOMBRE') THEN 'HOMBRE'
        ELSE 'SIN_DATO'
    END AS genero,
    COUNT(DISTINCT u.DW_CUENTA_CORPORATIVA) AS cuentas_fondeadas
FROM cuentas_fondeadas_q1 f
INNER JOIN universo_cuentas u
    ON u.DW_CUENTA_CORPORATIVA = f.DW_CUENTA_CORPORATIVA
LEFT JOIN DW_CIF_CLIENTES c
    ON LTRIM(RTRIM(c.CLDOC)) = u.codigo_cliente
GROUP BY
    CASE
        WHEN UPPER(LTRIM(RTRIM(c.clisex))) IN ('F', 'FEMENINO', 'MUJER') THEN 'MUJER'
        WHEN UPPER(LTRIM(RTRIM(c.clisex))) IN ('M', 'H', 'MASCULINO', 'HOMBRE') THEN 'HOMBRE'
        ELSE 'SIN_DATO'
    END
ORDER BY cuentas_fondeadas DESC, genero ASC;
