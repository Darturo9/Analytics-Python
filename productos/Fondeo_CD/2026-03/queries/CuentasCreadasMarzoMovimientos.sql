-- Cuentas de Cuenta Digital creadas en marzo 2026
-- con dos vistas de movimiento/transacciones usando la misma metrica:
-- Cant_transacciones (CTCTRX).
--
-- 1) SOLO_MARZO:
--    CTCTRX maximo observado dentro de marzo 2026 por cuenta.
--
-- 2) LIBRE:
--    CTCTRX de la fecha mas reciente disponible por cuenta (sin filtro de mes).
--
-- Nota de optimizacion:
-- Se materializa el universo de cuentas de marzo en tabla temporal indexada
-- para reducir lecturas sobre historicos.

SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#cuentas_marzo') IS NOT NULL
    DROP TABLE #cuentas_marzo;

IF OBJECT_ID('tempdb..#trx_marzo') IS NOT NULL DROP TABLE #trx_marzo;
IF OBJECT_ID('tempdb..#trx_libre') IS NOT NULL DROP TABLE #trx_libre;

SELECT
    d.DW_CUENTA_CORPORATIVA,
    RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente
INTO #cuentas_marzo
FROM dw_dep_depositos d
WHERE d.dw_feha_apertura >= '2026-03-01'
  AND d.dw_feha_apertura <  '2026-04-01'
  AND d.dw_producto = 'CUENTA DIGITAL'
  AND d.PRCODP = 1
  AND d.PRSUBP = 51
GROUP BY
    d.DW_CUENTA_CORPORATIVA,
    RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8);

CREATE CLUSTERED INDEX IX_cuentas_marzo_cuenta
    ON #cuentas_marzo (DW_CUENTA_CORPORATIVA);

SELECT
    h.DW_CUENTA_CORPORATIVA,
    MAX(COALESCE(TRY_CONVERT(BIGINT, h.CTCTRX), 0)) AS cant_transacciones_marzo
INTO #trx_marzo
FROM HIS_DEP_DEPOSITOS_VIEW h
INNER JOIN #cuentas_marzo c
    ON c.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
WHERE h.dw_fecha_informacion >= '2026-03-01'
  AND h.dw_fecha_informacion <  '2026-04-01'
  AND h.dw_producto = 'CUENTA DIGITAL'
  AND h.PRCODP = 1
  AND h.PRSUBP = 51
GROUP BY
    h.DW_CUENTA_CORPORATIVA
OPTION (RECOMPILE);

CREATE UNIQUE CLUSTERED INDEX IX_trx_marzo_cuenta
    ON #trx_marzo (DW_CUENTA_CORPORATIVA);

WITH ultimos AS (
    SELECT
        h.DW_CUENTA_CORPORATIVA,
        COALESCE(TRY_CONVERT(BIGINT, h.CTCTRX), 0) AS cant_transacciones_libre,
        ROW_NUMBER() OVER (
            PARTITION BY h.DW_CUENTA_CORPORATIVA
            ORDER BY h.dw_fecha_informacion DESC
        ) AS rn
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN #cuentas_marzo c
        ON c.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_producto = 'CUENTA DIGITAL'
      AND h.PRCODP = 1
      AND h.PRSUBP = 51
)
SELECT
    DW_CUENTA_CORPORATIVA,
    cant_transacciones_libre
INTO #trx_libre
FROM ultimos
WHERE rn = 1
OPTION (RECOMPILE);

CREATE UNIQUE CLUSTERED INDEX IX_trx_libre_cuenta
    ON #trx_libre (DW_CUENTA_CORPORATIVA);

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
FROM #cuentas_marzo c
LEFT JOIN #trx_marzo tm
    ON tm.DW_CUENTA_CORPORATIVA = c.DW_CUENTA_CORPORATIVA

UNION ALL

SELECT
    'LIBRE' AS escenario,
    COUNT(*) AS cuentas_creadas_marzo,
    SUM(CASE WHEN COALESCE(tl.cant_transacciones_libre, 0) > 0 THEN 1 ELSE 0 END) AS cuentas_con_movimiento,
    SUM(CASE WHEN COALESCE(tl.cant_transacciones_libre, 0) <= 0 THEN 1 ELSE 0 END) AS cuentas_sin_movimiento,
    CAST(
        CASE
            WHEN COUNT(*) = 0 THEN 0
            ELSE (
                SUM(CASE WHEN COALESCE(tl.cant_transacciones_libre, 0) > 0 THEN 1 ELSE 0 END) * 100.0
                / COUNT(*)
            )
        END
        AS DECIMAL(10, 2)
    ) AS pct_cuentas_con_movimiento,
    SUM(COALESCE(tl.cant_transacciones_libre, 0)) AS total_transacciones,
    CAST(AVG(CAST(COALESCE(tl.cant_transacciones_libre, 0) AS DECIMAL(18, 2))) AS DECIMAL(18, 2)) AS promedio_transacciones_por_cuenta,
    MAX(COALESCE(tl.cant_transacciones_libre, 0)) AS max_transacciones_en_una_cuenta
FROM #cuentas_marzo c
LEFT JOIN #trx_libre tl
    ON tl.DW_CUENTA_CORPORATIVA = c.DW_CUENTA_CORPORATIVA
OPTION (RECOMPILE);
