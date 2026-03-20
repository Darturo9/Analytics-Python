/*
Conteo de clientes del universo (naturales activos con banca)
que SI / NO han realizado pagos, agrupando servicios segun descripcionServicios.txt
y excluyendo temporalmente:
- Pago de Tarjeta de Credito
- Pago de Tarjeta de Credito Terceros
- Transferencias a terceros
- Transferencias Propias
*/

DECLARE @fecha_inicio DATE = '2025-01-01';

IF OBJECT_ID('tempdb..#universo_clientes') IS NOT NULL DROP TABLE #universo_clientes;
IF OBJECT_ID('tempdb..#pagos_filtrados') IS NOT NULL DROP TABLE #pagos_filtrados;

WITH universo_clientes AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) AS padded_codigo_cliente
    FROM DW_CIF_CLIENTES c
    WHERE c.ESTATU = 'A'
      AND c.CLTIPE = 'N'
      AND c.dw_usuarios_bel_cnt > 0
)
SELECT
    padded_codigo_cliente
INTO #universo_clientes
FROM universo_clientes;

WITH pagos_bxi AS (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(j.clccli)), 8) AS padded_codigo_cliente,
        j.dw_fecha_journal AS fecha_pago,
        CASE
            WHEN s.secode IN ('ap-pagclar', 'app-pagcla', 'ope-rccl', 'app-reccla') THEN 'Claro'
            WHEN s.secode IN ('app-ptigo', 'pag-tigo', 'app-rectig', 'ope-rctg') THEN 'Tigo'
            WHEN s.secode IN ('app-paenee', 'pag-enee') THEN 'Electricidad'
            WHEN s.secode IN ('app-asps', 'pag-asps') THEN 'Agua'
            WHEN s.secode IN ('app-achtrf', 'app-trach', 'app-transh') THEN 'Transferencias ACH'
            WHEN s.secode = 'app-transf' THEN 'Transferencias Propias'
            WHEN s.secode = 'app-transt' THEN 'Transferencias a terceros'
            WHEN s.secode = 'app-pagotc' THEN 'Pago de Tarjeta de Credito Terceros'
            WHEN s.secode = 'app-tcpago' THEN 'Pago de Tarjeta de Credito'
            ELSE NULL
        END AS tipo_pago
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
            'app-achtrf', 'app-trach', 'app-transf', 'app-transh', 'app-transt',
            'app-tcpago', 'app-pagotc'
       )
    WHERE j.dw_fecha_journal >= @fecha_inicio
      AND j.jostat = 1
      AND j.josecu = 1
      AND j.jovalo > 0
),
pagos_multi AS (
    SELECT
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
        ) AS padded_codigo_cliente,
        p.DW_FECHA_OPERACION_SP AS fecha_pago,
        CASE
            WHEN TRY_CONVERT(INT, m.SPCCAT) = 3 THEN 'Educacion'
            WHEN TRY_CONVERT(INT, p.spcodc) IN (481, 907) THEN 'Electricidad'
            WHEN TRY_CONVERT(INT, p.spcodc) = 693 THEN 'Licencia'
            WHEN TRY_CONVERT(INT, p.spcodc) = 524 THEN 'Tigo'
            WHEN TRY_CONVERT(INT, p.spcodc) IN (572, 573, 732) THEN 'Cable'
            WHEN TRY_CONVERT(INT, p.spcodc) = 498 THEN 'Claro'
            WHEN TRY_CONVERT(INT, p.spcodc) IN (866, 882) THEN 'Impuestos'
            WHEN TRY_CONVERT(INT, m.SPCCAT) = 11 THEN 'Agua'
            WHEN TRY_CONVERT(INT, p.spcodc) IN (513, 868, 869) THEN 'Matricula de vehiculos'
            ELSE NULL
        END AS tipo_pago
    FROM dw_mul_sppadat p
    INNER JOIN dw_mul_spmaco m
        ON p.spcodc = m.spcodc
    WHERE p.dw_fecha_operacion_sp >= @fecha_inicio
      AND p.sppafr = 'N'
      AND TRY_CONVERT(INT, p.spcodc) IN (
            866, 882, 130, 143, 184, 227, 237, 238, 309, 368, 371,
            446, 459, 478, 507, 512, 526, 571, 574, 643, 680, 687,
            734, 755, 885, 888, 481, 907, 693, 572, 573, 732, 498,
            524, 513, 868, 869
      )
),
pagos_clasificados AS (
    SELECT padded_codigo_cliente, fecha_pago, tipo_pago
    FROM pagos_bxi
    UNION ALL
    SELECT padded_codigo_cliente, fecha_pago, tipo_pago
    FROM pagos_multi
),
pagos_filtrados AS (
    SELECT p.padded_codigo_cliente, p.fecha_pago, p.tipo_pago
    FROM pagos_clasificados p
    INNER JOIN #universo_clientes u
        ON u.padded_codigo_cliente = p.padded_codigo_cliente
    WHERE p.tipo_pago IS NOT NULL
      AND p.tipo_pago NOT IN (
          'Pago de Tarjeta de Credito',
          'Pago de Tarjeta de Credito Terceros',
          'Transferencias a terceros',
          'Transferencias Propias'
      )
)
SELECT
    padded_codigo_cliente,
    fecha_pago,
    tipo_pago
INTO #pagos_filtrados
FROM pagos_filtrados;

WITH pagadores_unicos AS (
    SELECT DISTINCT padded_codigo_cliente
    FROM #pagos_filtrados
)
SELECT
    COUNT(*) AS universo_clientes,
    SUM(CASE WHEN pu.padded_codigo_cliente IS NOT NULL THEN 1 ELSE 0 END) AS clientes_con_pago,
    SUM(CASE WHEN pu.padded_codigo_cliente IS NULL THEN 1 ELSE 0 END) AS clientes_sin_pago
FROM #universo_clientes u
LEFT JOIN pagadores_unicos pu
    ON pu.padded_codigo_cliente = u.padded_codigo_cliente;

-- Desglose opcional por tipo de pago
SELECT
    tipo_pago,
    COUNT(*) AS total_transacciones,
    COUNT(DISTINCT padded_codigo_cliente) AS clientes_unicos
 FROM #pagos_filtrados
GROUP BY tipo_pago
ORDER BY clientes_unicos DESC, tipo_pago ASC;

DROP TABLE #pagos_filtrados;
DROP TABLE #universo_clientes;
