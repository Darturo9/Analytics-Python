-- Clientes y cuentas fondeadas por generacion (Q1 2026)
-- Universo: cuentas de CUENTA DIGITAL abiertas en Q1 2026
-- Regla de fondeo: saldo diario (ctt001) > 0 en al menos un dia del Q1.

WITH universo_cuentas AS (
    SELECT
        d.DW_CUENTA_CORPORATIVA,
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente
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
),
clientes_latest AS (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) AS padded_codigo_cliente,
        c.DW_FECHA_NACIMIENTO AS fecha_nacimiento,
        ROW_NUMBER() OVER (
            PARTITION BY RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8)
            ORDER BY c.dw_fecha_informacion DESC
        ) AS rn
    FROM DW_CIF_CLIENTES c
),
base AS (
    SELECT
        u.DW_CUENTA_CORPORATIVA,
        u.padded_codigo_cliente,
        cl.fecha_nacimiento,
        CASE
            WHEN cl.fecha_nacimiento IS NULL THEN 'SIN DATO'
            WHEN YEAR(cl.fecha_nacimiento) BETWEEN 1965 AND 1980 THEN 'Generation X (1965-1980)'
            WHEN YEAR(cl.fecha_nacimiento) BETWEEN 1981 AND 1996 THEN 'Gen Y - Millennials (1981-1996)'
            WHEN YEAR(cl.fecha_nacimiento) BETWEEN 1997 AND 2012 THEN 'Generacion Z (1997-2012)'
            ELSE 'OTRA GENERACION'
        END AS generacion
    FROM cuentas_fondeadas_q1 f
    INNER JOIN universo_cuentas u
        ON u.DW_CUENTA_CORPORATIVA = f.DW_CUENTA_CORPORATIVA
    LEFT JOIN clientes_latest cl
        ON cl.padded_codigo_cliente = u.padded_codigo_cliente
       AND cl.rn = 1
)
SELECT
    generacion,
    COUNT(DISTINCT padded_codigo_cliente) AS clientes_fondeadores,
    COUNT(DISTINCT DW_CUENTA_CORPORATIVA) AS cuentas_fondeadas
FROM base
GROUP BY generacion
ORDER BY cuentas_fondeadas DESC, generacion ASC;
