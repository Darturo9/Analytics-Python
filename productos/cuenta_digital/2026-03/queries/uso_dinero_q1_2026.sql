/*
Uso del dinero - Cuenta Digital Q1 2026
Universo: clientes con cuenta digital abierta entre 2026-01-01 y 2026-03-31.
Transacciones consideradas en Q1 2026 para ese universo.
*/

WITH universo_q1 AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM(d.cldoc)), 8) AS padded_codigo_cliente
    FROM dw_dep_depositos d
    WHERE d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
      AND CAST(d.dw_feha_apertura AS DATE) >= '2026-01-01'
      AND CAST(d.dw_feha_apertura AS DATE) < '2026-04-01'
),
pagos_bxi AS (
    SELECT
        n.padded_codigo_cliente,
        CAST(j.dw_fecha_journal AS DATE) AS fecha_transaccion,
        CASE
            WHEN s.secode IN ('ap-pagclar', 'app-pagcla', 'ope-rccl', 'app-reccla') THEN 'Pago Claro'
            WHEN s.secode IN ('app-ptigo', 'pag-tigo', 'app-rectig', 'ope-rctg') THEN 'Pago Tigo'
            WHEN s.secode IN ('app-paenee', 'pag-enee') THEN 'Pago Electricidad'
            WHEN s.secode IN ('app-asps', 'pag-asps') THEN 'Pago Agua'
            WHEN s.secode IN ('app-achtrf', 'app-trach', 'app-transh') THEN 'Transferencias ACH'
            WHEN s.secode = 'app-transf' THEN 'Transferencias Propias'
            WHEN s.secode = 'app-transt' THEN 'Transferencias a terceros'
            WHEN s.secode = 'app-tcpago' THEN 'Pago TC'
            WHEN s.secode = 'app-pagotc' THEN 'Pago TC terceros'
            ELSE NULL
        END AS tipo_uso,
        CAST(j.jovalo AS DECIMAL(18, 2)) AS valor,
        'BXI' AS origen_pago
    FROM dw_bel_ibjour j
    INNER JOIN dw_bel_ibserv s
        ON j.secode = s.secode
       AND s.inserv = 'APP'
       AND s.tiserv = 'O'
       AND s.seuspr = 'S'
       AND s.secode IN (
            'ap-pagclar', 'app-pagcla', 'ope-rccl', 'app-reccla',
            'app-ptigo', 'pag-tigo', 'app-rectig', 'ope-rctg',
            'app-paenee', 'pag-enee', 'app-asps', 'pag-asps',
            'app-achtrf', 'app-trach', 'app-transh',
            'app-transf', 'app-transt', 'app-tcpago', 'app-pagotc'
       )
    CROSS APPLY (
        VALUES (RIGHT('00000000' + LTRIM(RTRIM(j.clccli)), 8))
    ) AS n(padded_codigo_cliente)
    INNER JOIN universo_q1 u
        ON u.padded_codigo_cliente = n.padded_codigo_cliente
    WHERE j.dw_fecha_journal >= '2026-01-01'
      AND j.dw_fecha_journal < '2026-04-01'
      AND j.jostat = 1
      AND j.josecu = 1
      AND j.jovalo > 0
),
pagos_multi_raw AS (
    SELECT
        n.padded_codigo_cliente,
        CAST(p.dw_fecha_operacion_sp AS DATE) AS fecha_transaccion,
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
    INNER JOIN universo_q1 u
        ON u.padded_codigo_cliente = n.padded_codigo_cliente
    WHERE p.dw_fecha_operacion_sp >= '2026-01-01'
      AND p.dw_fecha_operacion_sp < '2026-04-01'
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
            ELSE NULL
        END AS tipo_uso,
        valor,
        'MULTIPAGO' AS origen_pago
    FROM pagos_multi_raw
),
uso_dinero AS (
    SELECT
        p.padded_codigo_cliente,
        p.fecha_transaccion,
        CONVERT(VARCHAR(7), p.fecha_transaccion, 120) AS periodo_mes,
        p.tipo_uso,
        p.origen_pago,
        p.valor
    FROM pagos_bxi p
    WHERE p.tipo_uso IS NOT NULL
    UNION ALL
    SELECT
        p.padded_codigo_cliente,
        p.fecha_transaccion,
        CONVERT(VARCHAR(7), p.fecha_transaccion, 120) AS periodo_mes,
        p.tipo_uso,
        p.origen_pago,
        p.valor
    FROM pagos_multi p
    WHERE p.tipo_uso IS NOT NULL
)
SELECT
    padded_codigo_cliente,
    fecha_transaccion,
    periodo_mes,
    tipo_uso,
    origen_pago,
    valor
FROM uso_dinero;
