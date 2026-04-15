-- Top departamentos con mas cuentas fondeadas (Q1 2026)
-- Universo: cuentas de CUENTA DIGITAL abiertas en Q1 2026
-- Regla: cuenta fondeada = tuvo saldo > 0 al menos un dia de Q1.

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
),
direccion_cliente AS (
    SELECT
        LTRIM(RTRIM(d.CLDOC)) AS codigo_cliente,
        d.DW_NIVEL_GEO2 AS depto,
        ROW_NUMBER() OVER (
            PARTITION BY LTRIM(RTRIM(d.CLDOC))
            ORDER BY d.dw_fecha DESC
        ) AS rn
    FROM DW_CIF_DIRECCIONES d
    WHERE d.CLDICO = 1
)
SELECT
    COALESCE(NULLIF(LTRIM(RTRIM(dc.depto)), ''), 'SIN DEPTO') AS depto,
    COUNT(DISTINCT u.DW_CUENTA_CORPORATIVA) AS cuentas_fondeadas,
    ROW_NUMBER() OVER (
        ORDER BY COUNT(DISTINCT u.DW_CUENTA_CORPORATIVA) DESC,
                 COALESCE(NULLIF(LTRIM(RTRIM(dc.depto)), ''), 'SIN DEPTO') ASC
    ) AS ranking_depto
FROM cuentas_fondeadas_q1 f
INNER JOIN universo_cuentas u
    ON u.DW_CUENTA_CORPORATIVA = f.DW_CUENTA_CORPORATIVA
LEFT JOIN direccion_cliente dc
    ON dc.codigo_cliente = u.codigo_cliente
   AND dc.rn = 1
GROUP BY COALESCE(NULLIF(LTRIM(RTRIM(dc.depto)), ''), 'SIN DEPTO')
ORDER BY cuentas_fondeadas DESC, depto ASC;
