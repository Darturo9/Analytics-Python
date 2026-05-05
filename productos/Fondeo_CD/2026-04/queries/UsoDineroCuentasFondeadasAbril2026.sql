/*
Uso de dinero para cuentas fondeadas en abril 2026 (mes completo)

Universo:
- Cuentas de CUENTA DIGITAL abiertas en abril 2026.
- De ese universo, solo cuentas con fondeo (ctt001 > 0) al menos un dia
  en el mismo rango (abril 2026 completo).

Salida:
- Tipo de uso/transaccion
- Total de transacciones
- Clientes unicos
- Monto total y promedio
*/

WITH cuentas_abiertas_1a15 AS (
    SELECT
        d.DW_CUENTA_CORPORATIVA,
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente
    FROM dw_dep_depositos d
    WHERE d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
      AND d.dw_feha_apertura >= '2026-04-01'
      AND d.dw_feha_apertura <  '2026-05-01'
    GROUP BY
        d.DW_CUENTA_CORPORATIVA,
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
),
universo_fondeado AS (
    SELECT
        a.padded_codigo_cliente
    FROM cuentas_abiertas_1a15 a
    WHERE EXISTS (
        SELECT 1
        FROM HIS_DEP_DEPOSITOS_VIEW h
        WHERE h.DW_CUENTA_CORPORATIVA = a.DW_CUENTA_CORPORATIVA
          AND h.dw_fecha_informacion >= '2026-04-01'
          AND h.dw_fecha_informacion <  '2026-05-01'
          AND h.ctt001 > 0
    )
    GROUP BY a.padded_codigo_cliente
),
pagos_bxi AS (
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
        END AS tipo_uso,
        CAST(j.jovalo AS DECIMAL(18, 2)) AS valor,
        'BXI' AS origen
    FROM dw_bel_ibjour j
    CROSS APPLY (
        VALUES (RIGHT('00000000' + LTRIM(RTRIM(j.clccli)), 8))
    ) AS n(padded_codigo_cliente)
    INNER JOIN universo_fondeado u
        ON u.padded_codigo_cliente = n.padded_codigo_cliente
    WHERE j.dw_fecha_journal >= '2026-04-01'
      AND j.dw_fecha_journal <  '2026-05-01'
      AND j.jostat = 1
      AND j.josecu = 1
      AND j.jovalo > 0
      AND j.secode IN (
          'ap-pagclar', 'app-pagcla', 'ope-rccl', 'app-reccla',
          'app-ptigo', 'pag-tigo', 'app-rectig', 'ope-rctg',
          'app-paenee', 'pag-enee',
          'app-asps', 'pag-asps',
          'app-achtrf', 'app-trach', 'app-transh',
          'app-transf', 'app-transt', 'app-tcpago', 'app-pagotc'
      )
),
pagos_multi_raw AS (
    SELECT
        u.padded_codigo_cliente,
        CONVERT(date, p.dw_fecha_operacion_sp) AS fecha_transaccion,
        cv.codigo_int,
        cv.categoria_int,
        CAST(p.sppava AS DECIMAL(18, 2)) AS valor
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
    INNER JOIN universo_fondeado u
        ON u.padded_codigo_cliente = n.padded_codigo_cliente
    WHERE p.dw_fecha_operacion_sp >= '2026-04-01'
      AND p.dw_fecha_operacion_sp <  '2026-05-01'
      AND p.sppafr = 'N'
      AND (
          cv.codigo_int IN (481, 907, 693, 524, 572, 573, 732, 498, 866, 882, 513, 868, 869)
          OR cv.categoria_int IN (3, 11)
      )
),
pagos_multi AS (
    SELECT
        padded_codigo_cliente,
        fecha_transaccion,
        CASE
            WHEN categoria_int = 3 THEN 'Pago Educacion'
            WHEN codigo_int IN (481, 907) THEN 'Pago Electricidad'
            WHEN codigo_int = 693 THEN 'Pago Licencia'
            WHEN codigo_int = 524 THEN 'Pago Tigo'
            WHEN codigo_int IN (572, 573, 732) THEN 'Pago Cable'
            WHEN codigo_int = 498 THEN 'Pago Claro'
            WHEN codigo_int IN (866, 882) THEN 'Pago Impuestos'
            WHEN categoria_int = 11 THEN 'Pago Agua'
            WHEN codigo_int IN (513, 868, 869) THEN 'Pago Matricula vehiculos'
        END AS tipo_uso,
        valor,
        'MULTIPAGO' AS origen
    FROM pagos_multi_raw
),
uso_dinero AS (
    SELECT padded_codigo_cliente, fecha_transaccion, tipo_uso, origen, valor
    FROM pagos_bxi
    UNION ALL
    SELECT padded_codigo_cliente, fecha_transaccion, tipo_uso, origen, valor
    FROM pagos_multi
)
SELECT
    origen,
    tipo_uso,
    COUNT(*) AS total_transacciones,
    COUNT(DISTINCT padded_codigo_cliente) AS clientes_unicos,
    CAST(SUM(valor) AS DECIMAL(18, 2)) AS monto_total,
    CAST(AVG(valor) AS DECIMAL(18, 2)) AS monto_promedio
FROM uso_dinero
GROUP BY origen, tipo_uso
ORDER BY origen, total_transacciones DESC, tipo_uso ASC;
