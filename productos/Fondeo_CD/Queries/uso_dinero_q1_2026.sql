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
clientes_raw AS (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(c.cldoc)), 8) AS padded_codigo_cliente,
        c.clisex AS genero,
        c.DW_FECHA_NACIMIENTO AS fecha_nacimiento,
        c.DW_ESTADO_CIVIL_DESCRIPCION AS estado_civil,
        ROW_NUMBER() OVER (
            PARTITION BY RIGHT('00000000' + LTRIM(RTRIM(c.cldoc)), 8)
            ORDER BY c.dw_fecha_informacion DESC
        ) AS rn
    FROM DW_CIF_CLIENTES c
    INNER JOIN universo_q1 u
        ON u.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(c.cldoc)), 8)
),
direcciones_raw AS (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(d.cldoc)), 8) AS padded_codigo_cliente,
        MAX(d.dw_nivel_geo1) AS direccion_lvl_1,
        MAX(d.dw_nivel_geo2) AS direccion_lvl_2,
        MAX(d.dw_nivel_geo3) AS direccion_lvl_3
    FROM DW_CIF_DIRECCIONES_PRINCIPAL d
    INNER JOIN universo_q1 u
        ON u.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(d.cldoc)), 8)
    GROUP BY RIGHT('00000000' + LTRIM(RTRIM(d.cldoc)), 8)
),
clientes_demo AS (
    SELECT
        c.padded_codigo_cliente,
        c.genero,
        c.fecha_nacimiento,
        c.estado_civil,
        d.direccion_lvl_1,
        d.direccion_lvl_2,
        d.direccion_lvl_3,
        CASE
            WHEN c.fecha_nacimiento IS NULL THEN NULL
            ELSE DATEDIFF(YEAR, c.fecha_nacimiento, '2026-03-31')
                 - CASE
                     WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.fecha_nacimiento, '2026-03-31'), c.fecha_nacimiento) > '2026-03-31'
                     THEN 1 ELSE 0
                   END
        END AS edad,
        CASE
            WHEN c.fecha_nacimiento IS NULL THEN 'SIN DATO'
            WHEN YEAR(c.fecha_nacimiento) BETWEEN 1965 AND 1980 THEN 'Generation X (1965-1980)'
            WHEN YEAR(c.fecha_nacimiento) BETWEEN 1981 AND 1996 THEN 'Gen Y - Millennials (1981-1996)'
            WHEN YEAR(c.fecha_nacimiento) BETWEEN 1997 AND 2012 THEN 'Generacion Z (1997-2012)'
            ELSE 'OTRA GENERACION'
        END AS generacion
    FROM clientes_raw c
    LEFT JOIN direcciones_raw d
        ON d.padded_codigo_cliente = c.padded_codigo_cliente
    WHERE c.rn = 1
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
    u.padded_codigo_cliente,
    u.fecha_transaccion,
    u.periodo_mes,
    u.tipo_uso,
    u.origen_pago,
    u.valor,
    d.genero,
    d.fecha_nacimiento,
    d.edad,
    CASE
        WHEN d.edad IS NULL THEN 'SIN DATO'
        WHEN d.edad BETWEEN 18 AND 25 THEN '18-25'
        WHEN d.edad BETWEEN 26 AND 35 THEN '26-35'
        WHEN d.edad BETWEEN 36 AND 45 THEN '36-45'
        WHEN d.edad BETWEEN 46 AND 55 THEN '46-55'
        WHEN d.edad >= 56 THEN '56+'
        ELSE 'SIN DATO'
    END AS rango_edad,
    d.generacion,
    d.estado_civil,
    d.direccion_lvl_1,
    d.direccion_lvl_2,
    d.direccion_lvl_3
FROM uso_dinero u
LEFT JOIN clientes_demo d
    ON d.padded_codigo_cliente = u.padded_codigo_cliente;
