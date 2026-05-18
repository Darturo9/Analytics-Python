WITH cuentas AS (
    SELECT
        CAST(d.dw_feha_apertura AS DATE) AS fecha_apertura,
        d.dw_cuenta_corporativa          AS numero_cuenta
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= :fecha_inicio
      AND d.dw_feha_apertura <  :fecha_fin_exclusiva
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
)
SELECT
    fecha_apertura,
    COUNT(DISTINCT numero_cuenta) AS cuentas_creadas
FROM cuentas
GROUP BY fecha_apertura
ORDER BY fecha_apertura;
