-- Cuentas de Cuenta Digital creadas en marzo 2026
-- con dos vistas de movimiento/transacciones:
-- 1) SOLO_MARZO: movimiento y transacciones ocurridas en marzo 2026.
-- 2) LIBRE: movimiento y transacciones acumuladas (ctctrx, sin filtro de mes).

WITH cuentas_marzo AS (
    SELECT
        d.DW_CUENTA_CORPORATIVA,
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente,
        MAX(COALESCE(TRY_CONVERT(BIGINT, d.ctctrx), 0)) AS cant_transacciones_libre
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= '2026-03-01'
      AND d.dw_feha_apertura <  '2026-04-01'
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
    GROUP BY
        d.DW_CUENTA_CORPORATIVA,
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
),
transacciones_marzo AS (
    SELECT
        m.DW_CUENTA_CORPORATIVA,
        COUNT_BIG(*) AS cant_transacciones_marzo
    FROM DW_DEP_DPMOVM_VIEW m
    INNER JOIN cuentas_marzo c
        ON c.DW_CUENTA_CORPORATIVA = m.DW_CUENTA_CORPORATIVA
    WHERE m.dw_fecha_operacion >= '2026-03-01'
      AND m.dw_fecha_operacion <  '2026-04-01'
      AND COALESCE(m.MVSTAT, '') <> 'R'
    GROUP BY
        m.DW_CUENTA_CORPORATIVA
)
SELECT
    'SOLO_MARZO' AS escenario,
    COUNT(*) AS cuentas_creadas_marzo,
    SUM(CASE WHEN COALESCE(tm.cant_transacciones_marzo, 0) > 0 THEN 1 ELSE 0 END) AS cuentas_con_movimiento,
    SUM(CASE WHEN COALESCE(tm.cant_transacciones_marzo, 0) <= 0 THEN 1 ELSE 0 END) AS cuentas_sin_movimiento,
    CAST(
        CASE
            WHEN COUNT(*) = 0 THEN 0
            ELSE (
                SUM(CASE WHEN COALESCE(tm.cant_transacciones_marzo, 0) > 0 THEN 1 ELSE 0 END) * 100.0
                / COUNT(*)
            )
        END
        AS DECIMAL(10, 2)
    ) AS pct_cuentas_con_movimiento,
    SUM(COALESCE(tm.cant_transacciones_marzo, 0)) AS total_transacciones,
    CAST(AVG(CAST(COALESCE(tm.cant_transacciones_marzo, 0) AS DECIMAL(18, 2))) AS DECIMAL(18, 2)) AS promedio_transacciones_por_cuenta,
    MAX(COALESCE(tm.cant_transacciones_marzo, 0)) AS max_transacciones_en_una_cuenta
FROM cuentas_marzo c
LEFT JOIN transacciones_marzo tm
    ON tm.DW_CUENTA_CORPORATIVA = c.DW_CUENTA_CORPORATIVA

UNION ALL

SELECT
    'LIBRE' AS escenario,
    COUNT(*) AS cuentas_creadas_marzo,
    SUM(CASE WHEN cant_transacciones_libre > 0 THEN 1 ELSE 0 END) AS cuentas_con_movimiento,
    SUM(CASE WHEN cant_transacciones_libre <= 0 THEN 1 ELSE 0 END) AS cuentas_sin_movimiento,
    CAST(
        CASE
            WHEN COUNT(*) = 0 THEN 0
            ELSE (
                SUM(CASE WHEN cant_transacciones_libre > 0 THEN 1 ELSE 0 END) * 100.0
                / COUNT(*)
            )
        END
        AS DECIMAL(10, 2)
    ) AS pct_cuentas_con_movimiento,
    SUM(cant_transacciones_libre) AS total_transacciones,
    CAST(AVG(CAST(cant_transacciones_libre AS DECIMAL(18, 2))) AS DECIMAL(18, 2)) AS promedio_transacciones_por_cuenta,
    MAX(cant_transacciones_libre) AS max_transacciones_en_una_cuenta
FROM cuentas_marzo;
