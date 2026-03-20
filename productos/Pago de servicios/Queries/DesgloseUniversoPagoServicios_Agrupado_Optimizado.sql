/*
Desglose optimizado por tipo de pago
(mismo universo y mismas exclusiones del conteo optimizado)
*/

WITH universo_clientes AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) AS padded_codigo_cliente
    FROM DW_CIF_CLIENTES c
    WHERE c.ESTATU = 'A'
      AND c.CLTIPE = 'N'
      AND c.dw_usuarios_bel_cnt > 0
),
pagos_bxi AS (
    SELECT
        n.padded_codigo_cliente,
        CASE
            WHEN s.secode IN ('ap-pagclar', 'app-pagcla', 'ope-rccl', 'app-reccla') THEN 'Claro'
            WHEN s.secode IN ('app-ptigo', 'pag-tigo', 'app-rectig', 'ope-rctg') THEN 'Tigo'
            WHEN s.secode IN ('app-paenee', 'pag-enee') THEN 'Electricidad'
            WHEN s.secode IN ('app-asps', 'pag-asps') THEN 'Agua'
            WHEN s.secode IN ('app-achtrf', 'app-trach', 'app-transh') THEN 'Transferencias ACH'
            ELSE NULL
        END AS tipo_pago,
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
            'app-achtrf', 'app-trach', 'app-transh'
       )
    CROSS APPLY (
        VALUES (RIGHT('00000000' + LTRIM(RTRIM(j.clccli)), 8))
    ) AS n(padded_codigo_cliente)
    INNER JOIN universo_clientes u
        ON u.padded_codigo_cliente = n.padded_codigo_cliente
    WHERE j.dw_fecha_journal >= :fecha_inicio
      AND j.jostat = 1
      AND j.josecu = 1
      AND j.jovalo > 0
),
pagos_multi_raw AS (
    SELECT
        n.padded_codigo_cliente,
        cv.codigo_int,
        cv.categoria_int
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
    INNER JOIN universo_clientes u
        ON u.padded_codigo_cliente = n.padded_codigo_cliente
    WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
      AND p.sppafr = 'N'
      AND (
          cv.codigo_int IN (481, 907, 693, 524, 572, 573, 732, 498, 866, 882, 513, 868, 869)
          OR cv.categoria_int IN (3, 11)
      )
),
pagos_multi AS (
    SELECT
        padded_codigo_cliente,
        CASE
            WHEN categoria_int = 3 THEN 'Educacion'
            WHEN codigo_int IN (481, 907) THEN 'Electricidad'
            WHEN codigo_int = 693 THEN 'Licencia'
            WHEN codigo_int = 524 THEN 'Tigo'
            WHEN codigo_int IN (572, 573, 732) THEN 'Cable'
            WHEN codigo_int = 498 THEN 'Claro'
            WHEN codigo_int IN (866, 882) THEN 'Impuestos'
            WHEN categoria_int = 11 THEN 'Agua'
            WHEN codigo_int IN (513, 868, 869) THEN 'Matricula de vehiculos'
            ELSE NULL
        END AS tipo_pago,
        'MULTIPAGO' AS origen_pago
    FROM pagos_multi_raw
),
pagos_filtrados AS (
    SELECT padded_codigo_cliente, tipo_pago, origen_pago
    FROM pagos_bxi
    WHERE tipo_pago IS NOT NULL
    UNION ALL
    SELECT padded_codigo_cliente, tipo_pago, origen_pago
    FROM pagos_multi
    WHERE tipo_pago IS NOT NULL
)
SELECT
    tipo_pago,
    origen_pago,
    COUNT(*) AS total_transacciones,
    COUNT(DISTINCT padded_codigo_cliente) AS clientes_unicos
FROM pagos_filtrados
GROUP BY tipo_pago, origen_pago
ORDER BY clientes_unicos DESC, tipo_pago ASC, origen_pago ASC;
