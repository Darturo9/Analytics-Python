WITH UniversoCuentas AS (
    SELECT DISTINCT
        DW_CUENTA_CORPORATIVA
    FROM dw_dep_depositos
    WHERE dw_feha_apertura BETWEEN '2026-03-01' AND '2026-03-31'
      AND dw_producto = 'CUENTA DIGITAL'
      AND PRCODP = 1 AND PRSUBP = 51
)
SELECT
    h.dw_fecha_informacion AS fecha,
    COUNT(DISTINCT h.DW_CUENTA_CORPORATIVA) AS cuentas_fondeadas
FROM UniversoCuentas u
INNER JOIN HIS_DEP_DEPOSITOS_VIEW h
    ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
WHERE h.dw_fecha_informacion BETWEEN '2026-03-01' AND '2026-04-01'
  AND h.ctt001 > 0
GROUP BY h.dw_fecha_informacion
ORDER BY cuentas_fondeadas DESC;
