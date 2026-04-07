SELECT
    dw_feha_apertura AS fecha_apertura,
    COUNT(DISTINCT DW_CUENTA_CORPORATIVA) AS cuentas_abiertas
FROM dw_dep_depositos
WHERE dw_feha_apertura BETWEEN '2026-03-01' AND '2026-03-31'
  AND dw_producto = 'CUENTA DIGITAL'
  AND PRCODP = 1 AND PRSUBP = 51
GROUP BY dw_feha_apertura
ORDER BY dw_feha_apertura;
