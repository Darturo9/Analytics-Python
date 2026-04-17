-- Cuentas de Cuenta Digital creadas en marzo 2026
-- y movimiento transaccional asociado (ctctrx).
--
-- Objetivo:
-- - Cuantas cuentas creadas en marzo tuvieron al menos 1 movimiento.
-- - Cuantas transacciones acumulan esas cuentas en total.

WITH cuentas_marzo AS (
    SELECT
        d.DW_CUENTA_CORPORATIVA,
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente,
        MAX(COALESCE(TRY_CONVERT(BIGINT, d.ctctrx), 0)) AS cant_transacciones
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= '2026-03-01'
      AND d.dw_feha_apertura <  '2026-04-01'
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
    GROUP BY
        d.DW_CUENTA_CORPORATIVA,
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
)
SELECT
    COUNT(*) AS cuentas_creadas_marzo,
    SUM(CASE WHEN cant_transacciones > 0 THEN 1 ELSE 0 END) AS cuentas_con_movimiento,
    SUM(CASE WHEN cant_transacciones <= 0 THEN 1 ELSE 0 END) AS cuentas_sin_movimiento,
    CAST(
        CASE
            WHEN COUNT(*) = 0 THEN 0
            ELSE (
                SUM(CASE WHEN cant_transacciones > 0 THEN 1 ELSE 0 END) * 100.0
                / COUNT(*)
            )
        END
        AS DECIMAL(10, 2)
    ) AS pct_cuentas_con_movimiento,
    SUM(cant_transacciones) AS total_transacciones,
    CAST(AVG(CAST(cant_transacciones AS DECIMAL(18, 2))) AS DECIMAL(18, 2)) AS promedio_transacciones_por_cuenta,
    MAX(cant_transacciones) AS max_transacciones_en_una_cuenta
FROM cuentas_marzo;

