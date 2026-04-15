-- Monto fondeado promedio por cuenta (Q1 2026)
-- Universo: cuentas de CUENTA DIGITAL abiertas en Q1 2026.
-- Regla de fondeo diario: ctt001 > 0.

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
base_diaria AS (
    SELECT
        CAST(h.dw_fecha_informacion AS DATE) AS fecha,
        h.DW_CUENTA_CORPORATIVA,
        CAST(COALESCE(h.ctt001, 0) AS DECIMAL(18, 2)) AS saldo_fondeado
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN universo_cuentas u
        ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_fecha_informacion >= '2026-01-01'
      AND h.dw_fecha_informacion <  '2026-04-01'
      AND h.ctt001 > 0
),
resumen_diario AS (
    SELECT
        fecha,
        COUNT(DISTINCT DW_CUENTA_CORPORATIVA) AS cuentas_fondeadas,
        SUM(saldo_fondeado) AS monto_total_fondeado
    FROM base_diaria
    GROUP BY fecha
)
SELECT
    fecha,
    cuentas_fondeadas,
    monto_total_fondeado,
    CAST(
        monto_total_fondeado / NULLIF(cuentas_fondeadas, 0)
        AS DECIMAL(18, 2)
    ) AS monto_promedio_por_cuenta
FROM resumen_diario
ORDER BY fecha ASC;
