-- Cuentas de Cuenta Digital creadas en marzo 2026.
-- Fuente principal: HIS_DEP_DEPOSITOS_VIEW.
-- Devuelve detalle diario por cuenta (delta de CTCTRX) + datos de corte.
--
-- Reglas:
-- 1) Universo: solo cuentas creadas en marzo 2026.
-- 2) Movimiento diario: incremento positivo de CTCTRX por cuenta y fecha.
-- 3) Corte: ultima foto disponible <= 2026-03-31.

SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#cuentas_marzo') IS NOT NULL DROP TABLE #cuentas_marzo;
IF OBJECT_ID('tempdb..#hist_marzo') IS NOT NULL DROP TABLE #hist_marzo;
IF OBJECT_ID('tempdb..#hist_marzo_delta') IS NOT NULL DROP TABLE #hist_marzo_delta;
IF OBJECT_ID('tempdb..#corte_31') IS NOT NULL DROP TABLE #corte_31;

SELECT
    d.DW_CUENTA_CORPORATIVA AS cuenta,
    RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente,
    MIN(CAST(d.dw_feha_apertura AS DATE)) AS fecha_apertura
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

CREATE UNIQUE CLUSTERED INDEX IX_cuentas_marzo_cuenta
    ON #cuentas_marzo (cuenta);

SELECT
    h.DW_CUENTA_CORPORATIVA AS cuenta,
    CAST(h.dw_fecha_informacion AS DATE) AS fecha_informacion,
    MAX(COALESCE(TRY_CONVERT(BIGINT, h.CTCTRX), 0)) AS ctctrx_dia
INTO #hist_marzo
FROM HIS_DEP_DEPOSITOS_VIEW h
INNER JOIN #cuentas_marzo c
    ON c.cuenta = h.DW_CUENTA_CORPORATIVA
WHERE h.dw_fecha_informacion >= '2026-03-01'
  AND h.dw_fecha_informacion <  '2026-04-01'
  AND h.dw_producto = 'CUENTA DIGITAL'
  AND h.PRCODP = 1
  AND h.PRSUBP = 51
GROUP BY
    h.DW_CUENTA_CORPORATIVA,
    CAST(h.dw_fecha_informacion AS DATE)
OPTION (RECOMPILE);

CREATE UNIQUE CLUSTERED INDEX IX_hist_marzo_cuenta_fecha
    ON #hist_marzo (cuenta, fecha_informacion);

WITH calculo AS (
    SELECT
        h.cuenta,
        h.fecha_informacion,
        h.ctctrx_dia,
        LAG(h.ctctrx_dia) OVER (
            PARTITION BY h.cuenta
            ORDER BY h.fecha_informacion
        ) AS ctctrx_prev
    FROM #hist_marzo h
)
SELECT
    c.cuenta,
    c.fecha_informacion,
    c.ctctrx_dia,
    COALESCE(c.ctctrx_prev, 0) AS ctctrx_prev,
    CASE
        WHEN c.ctctrx_dia - COALESCE(c.ctctrx_prev, 0) > 0
            THEN c.ctctrx_dia - COALESCE(c.ctctrx_prev, 0)
        ELSE 0
    END AS delta_transacciones_dia,
    CASE
        WHEN c.ctctrx_dia - COALESCE(c.ctctrx_prev, 0) > 0 THEN 1
        ELSE 0
    END AS movimiento_dia
INTO #hist_marzo_delta
FROM calculo c
OPTION (RECOMPILE);

CREATE UNIQUE CLUSTERED INDEX IX_hist_marzo_delta_cuenta_fecha
    ON #hist_marzo_delta (cuenta, fecha_informacion);

WITH corte AS (
    SELECT
        h.DW_CUENTA_CORPORATIVA AS cuenta,
        CAST(h.dw_fecha_informacion AS DATE) AS fecha_informacion_corte,
        COALESCE(TRY_CONVERT(BIGINT, h.CTCTRX), 0) AS transacciones_corte_31,
        CAST(h.DW_FECHA_ULTIMO_MOVIMIENTO AS DATE) AS fecha_ultimo_movimiento,
        h.dw_moneda AS moneda,
        COALESCE(h.ctt001, 0) AS saldo_ayer_corte,
        COALESCE(h.dw_saldo_promedio, 0) AS saldo_promedio_corte,
        h.CTSTA AS estatus_cuenta,
        ROW_NUMBER() OVER (
            PARTITION BY h.DW_CUENTA_CORPORATIVA
            ORDER BY h.dw_fecha_informacion DESC
        ) AS rn
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN #cuentas_marzo c
        ON c.cuenta = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_fecha_informacion >= '2026-03-01'
      AND h.dw_fecha_informacion <= '2026-03-31'
      AND h.dw_producto = 'CUENTA DIGITAL'
      AND h.PRCODP = 1
      AND h.PRSUBP = 51
)
SELECT
    cuenta,
    fecha_informacion_corte,
    transacciones_corte_31,
    fecha_ultimo_movimiento,
    moneda,
    saldo_ayer_corte,
    saldo_promedio_corte,
    estatus_cuenta
INTO #corte_31
FROM corte
WHERE rn = 1
OPTION (RECOMPILE);

CREATE UNIQUE CLUSTERED INDEX IX_corte_31_cuenta
    ON #corte_31 (cuenta);

SELECT
    c.padded_codigo_cliente,
    c.cuenta,
    c.fecha_apertura,
    d.fecha_informacion,
    COALESCE(d.ctctrx_dia, 0) AS ctctrx_dia,
    COALESCE(d.ctctrx_prev, 0) AS ctctrx_prev,
    COALESCE(d.delta_transacciones_dia, 0) AS delta_transacciones_dia,
    COALESCE(d.movimiento_dia, 0) AS movimiento_dia,
    COALESCE(k.transacciones_corte_31, 0) AS transacciones_corte_31,
    k.fecha_ultimo_movimiento,
    k.moneda,
    COALESCE(k.saldo_ayer_corte, 0) AS saldo_ayer_corte,
    COALESCE(k.saldo_promedio_corte, 0) AS saldo_promedio_corte,
    k.estatus_cuenta,
    k.fecha_informacion_corte
FROM #cuentas_marzo c
LEFT JOIN #hist_marzo_delta d
    ON d.cuenta = c.cuenta
LEFT JOIN #corte_31 k
    ON k.cuenta = c.cuenta
ORDER BY
    c.padded_codigo_cliente,
    d.fecha_informacion;
