/*
Uso de dinero para cuentas fondeadas en abril 2026 (mes completo) - version TEMP

Objetivo:
- Materializar universo y joins criticos en tablas temporales indexadas
  para reducir recalculo de CTEs y mejorar tiempos en volumen alto.

Salida:
- tipo_uso
- total_transacciones
- clientes_unicos
- monto_total
- monto_promedio
*/

SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#cuentas_abiertas_1a15') IS NOT NULL DROP TABLE #cuentas_abiertas_1a15;
IF OBJECT_ID('tempdb..#universo_fondeado') IS NOT NULL DROP TABLE #universo_fondeado;
IF OBJECT_ID('tempdb..#pagos_bxi') IS NOT NULL DROP TABLE #pagos_bxi;
IF OBJECT_ID('tempdb..#pagos_multi') IS NOT NULL DROP TABLE #pagos_multi;

SELECT
    d.DW_CUENTA_CORPORATIVA,
    RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente
INTO #cuentas_abiertas_1a15
FROM dw_dep_depositos d
WHERE d.dw_producto = 'CUENTA DIGITAL'
  AND d.PRCODP = 1
  AND d.PRSUBP = 51
  AND d.dw_feha_apertura >= '2026-04-01'
  AND d.dw_feha_apertura <  '2026-05-01'
GROUP BY
    d.DW_CUENTA_CORPORATIVA,
    RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8);

CREATE CLUSTERED INDEX IX_CA_1 ON #cuentas_abiertas_1a15 (DW_CUENTA_CORPORATIVA);
CREATE NONCLUSTERED INDEX IX_CA_2 ON #cuentas_abiertas_1a15 (padded_codigo_cliente);

SELECT
    a.padded_codigo_cliente
INTO #universo_fondeado
FROM #cuentas_abiertas_1a15 a
WHERE EXISTS (
    SELECT 1
    FROM HIS_DEP_DEPOSITOS_VIEW h
    WHERE h.DW_CUENTA_CORPORATIVA = a.DW_CUENTA_CORPORATIVA
      AND h.dw_fecha_informacion >= '2026-04-01'
      AND h.dw_fecha_informacion <  '2026-05-01'
      AND h.ctt001 > 0
)
GROUP BY a.padded_codigo_cliente;

CREATE UNIQUE CLUSTERED INDEX IX_UF_1 ON #universo_fondeado (padded_codigo_cliente);

SELECT
    u.padded_codigo_cliente,
    CONVERT(date, j.dw_fecha_journal) AS fecha_transaccion,
    CASE
        WHEN j.secode IN ('ap-pagclar', 'app-pagcla', 'ope-rccl', 'app-reccla') THEN 'Pago Claro'
        WHEN j.secode IN ('app-ptigo', 'pag-tigo', 'app-rectig', 'ope-rctg') THEN 'Pago Tigo'
        WHEN j.secode IN ('app-paenee', 'pag-enee') THEN 'Pago Electricidad'
        WHEN j.secode IN ('app-asps', 'pag-asps') THEN 'Pago Agua'
        WHEN j.secode IN ('app-achtrf', 'app-trach', 'app-transh') THEN 'Transferencias ACH'
        WHEN j.secode = 'app-transf' THEN 'Transferencias Propias'
        WHEN j.secode = 'app-transt' THEN 'Transferencias a terceros'
        WHEN j.secode = 'app-tcpago' THEN 'Pago TC'
        WHEN j.secode = 'app-pagotc' THEN 'Pago TC terceros'
        ELSE 'OTRAS_TRANSACCIONES_BXI'
    END AS tipo_uso,
    CAST(j.jovalo AS DECIMAL(18, 2)) AS valor
INTO #pagos_bxi
FROM dw_bel_ibjour j
CROSS APPLY (
    VALUES (RIGHT('00000000' + LTRIM(RTRIM(j.clccli)), 8))
) AS n(padded_codigo_cliente)
INNER JOIN #universo_fondeado u
    ON u.padded_codigo_cliente = n.padded_codigo_cliente
WHERE j.dw_fecha_journal >= '2026-04-01'
  AND j.dw_fecha_journal <  '2026-05-01'
  AND j.jostat = 1
  AND j.josecu = 1
  AND j.jovalo > 0;

CREATE NONCLUSTERED INDEX IX_PB_1 ON #pagos_bxi (padded_codigo_cliente);
CREATE NONCLUSTERED INDEX IX_PB_2 ON #pagos_bxi (tipo_uso);

SELECT
    u.padded_codigo_cliente,
    CONVERT(date, p.dw_fecha_operacion_sp) AS fecha_transaccion,
    CASE
        WHEN cv.categoria_int = 3 THEN 'Pago Educacion'
        WHEN cv.codigo_int IN (481, 907) THEN 'Pago Electricidad'
        WHEN cv.codigo_int = 693 THEN 'Pago Licencia'
        WHEN cv.codigo_int = 524 THEN 'Pago Tigo'
        WHEN cv.codigo_int IN (572, 573, 732) THEN 'Pago Cable'
        WHEN cv.codigo_int = 498 THEN 'Pago Claro'
        WHEN cv.codigo_int IN (866, 882) THEN 'Pago Impuestos'
        WHEN cv.categoria_int = 11 THEN 'Pago Agua'
        WHEN cv.codigo_int IN (513, 868, 869) THEN 'Pago Matricula vehiculos'
        ELSE 'OTRAS_TRANSACCIONES_MULTIPAGO'
    END AS tipo_uso,
    CAST(p.sppava AS DECIMAL(18, 2)) AS valor
INTO #pagos_multi
FROM dw_mul_sppadat p
INNER JOIN dw_mul_spmaco m
    ON p.spcodc = m.spcodc
CROSS APPLY (
    VALUES (
        RIGHT(
            '00000000' + LTRIM(RTRIM(
                CASE
                    WHEN p.spinus IS NULL THEN NULL
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) > 1
                        THEN LEFT(p.spinus, PATINDEX('%[A-Za-z]%', p.spinus) - 1)
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) = 1 THEN NULL
                    ELSE p.spinus
                END
            )),
            8
        )
    )
) AS n(padded_codigo_cliente)
CROSS APPLY (
    VALUES (
        TRY_CONVERT(INT, p.spcodc),
        TRY_CONVERT(INT, m.SPCCAT)
    )
) AS cv(codigo_int, categoria_int)
INNER JOIN #universo_fondeado u
    ON u.padded_codigo_cliente = n.padded_codigo_cliente
WHERE p.dw_fecha_operacion_sp >= '2026-04-01'
  AND p.dw_fecha_operacion_sp <  '2026-05-01'
  AND p.sppafr = 'N'
  AND (
      cv.codigo_int IN (481, 907, 693, 524, 572, 573, 732, 498, 866, 882, 513, 868, 869)
      OR cv.categoria_int IN (3, 11)
  );

CREATE NONCLUSTERED INDEX IX_PM_1 ON #pagos_multi (padded_codigo_cliente);
CREATE NONCLUSTERED INDEX IX_PM_2 ON #pagos_multi (tipo_uso);

SELECT
    t.tipo_uso,
    COUNT(*) AS total_transacciones,
    COUNT(DISTINCT t.padded_codigo_cliente) AS clientes_unicos,
    CAST(SUM(t.valor) AS DECIMAL(18, 2)) AS monto_total,
    CAST(AVG(t.valor) AS DECIMAL(18, 2)) AS monto_promedio
FROM (
    SELECT padded_codigo_cliente, tipo_uso, valor FROM #pagos_bxi
    UNION ALL
    SELECT padded_codigo_cliente, tipo_uso, valor FROM #pagos_multi
) t
GROUP BY t.tipo_uso
ORDER BY total_transacciones DESC, tipo_uso ASC;
