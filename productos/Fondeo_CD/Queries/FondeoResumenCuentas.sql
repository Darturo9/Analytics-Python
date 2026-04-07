WITH CohorteCuentas AS (
    SELECT
        d.DW_CUENTA_CORPORATIVA AS cuenta,
        MAX(RIGHT('00000000' + RTRIM(LTRIM(d.cldoc)), 8)) AS padded_codigo_cliente,
        MIN(CAST(d.dw_feha_apertura AS DATE)) AS fecha_apertura,
        MAX(d.dw_moneda) AS moneda
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= :fecha_inicio
      AND d.dw_feha_apertura < DATEADD(DAY, 1, CAST(:fecha_fin AS DATE))
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
    GROUP BY d.DW_CUENTA_CORPORATIVA
),
HistoricoFiltrado AS (
    SELECT
        h.DW_CUENTA_CORPORATIVA AS cuenta,
        CAST(h.dw_fecha_informacion AS DATE) AS fecha_informacion,
        COALESCE(h.ctt001, 0) AS saldo_ayer,
        COALESCE(h.dw_saldo_promedio, 0) AS saldo_promedio
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN CohorteCuentas c
        ON c.cuenta = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_fecha_informacion >= :fecha_inicio
      AND h.dw_fecha_informacion < DATEADD(DAY, 1, CAST(:fecha_fin AS DATE))
      AND h.dw_producto = 'CUENTA DIGITAL'
      AND h.PRCODP = 1
      AND h.PRSUBP = 51
),
ResumenCuenta AS (
    SELECT
        c.padded_codigo_cliente,
        c.cuenta,
        c.moneda,
        c.fecha_apertura,
        COALESCE(MAX(h.saldo_ayer), 0) AS saldo_maximo_mes,
        COALESCE(MAX(h.saldo_promedio), 0) AS saldo_promedio_maximo_mes,
        COUNT(
            DISTINCT CASE
                WHEN h.saldo_ayer > 0 OR h.saldo_promedio > 0
                THEN h.fecha_informacion
            END
        ) AS dias_con_fondos,
        MIN(
            CASE
                WHEN h.saldo_ayer > 0 OR h.saldo_promedio > 0
                THEN h.fecha_informacion
            END
        ) AS fecha_primer_fondeo,
        CASE
            WHEN COUNT(
                DISTINCT CASE
                    WHEN h.saldo_ayer > 0 OR h.saldo_promedio > 0
                    THEN h.fecha_informacion
                END
            ) > 0
            THEN 1 ELSE 0
        END AS tuvo_fondos_mes
    FROM CohorteCuentas c
    LEFT JOIN HistoricoFiltrado h
        ON h.cuenta = c.cuenta
    GROUP BY
        c.padded_codigo_cliente,
        c.cuenta,
        c.moneda,
        c.fecha_apertura
)
SELECT
    r.padded_codigo_cliente,
    r.cuenta,
    r.moneda,
    r.fecha_apertura,
    r.saldo_maximo_mes,
    r.saldo_promedio_maximo_mes,
    r.dias_con_fondos,
    r.fecha_primer_fondeo,
    r.tuvo_fondos_mes,
    CASE
        WHEN r.fecha_primer_fondeo IS NULL THEN NULL
        ELSE DATEDIFF(DAY, r.fecha_apertura, r.fecha_primer_fondeo)
    END AS dias_a_primer_fondeo,
    CASE
        WHEN DAY(r.fecha_apertura) <= 7 THEN 'Semana 1'
        WHEN DAY(r.fecha_apertura) <= 14 THEN 'Semana 2'
        WHEN DAY(r.fecha_apertura) <= 21 THEN 'Semana 3'
        ELSE 'Semana 4'
    END AS semana_apertura
FROM ResumenCuenta r
ORDER BY
    r.tuvo_fondos_mes DESC,
    r.dias_con_fondos DESC,
    r.cuenta;
