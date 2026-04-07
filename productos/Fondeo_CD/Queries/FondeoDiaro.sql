WITH CohorteCuentas AS (
    SELECT
        d.DW_CUENTA_CORPORATIVA AS cuenta
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= :fecha_inicio
      AND d.dw_feha_apertura < DATEADD(DAY, 1, CAST(:fecha_fin AS DATE))
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
    GROUP BY d.DW_CUENTA_CORPORATIVA
),
HistoricoDiarioCuenta AS (
    SELECT
        CAST(h.dw_fecha_informacion AS DATE) AS fecha_informacion,
        h.DW_CUENTA_CORPORATIVA AS cuenta,
        MAX(CASE
            WHEN COALESCE(h.ctt001, 0) > 0 OR COALESCE(h.dw_saldo_promedio, 0) > 0
            THEN 1 ELSE 0
        END) AS tuvo_fondos_dia
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN CohorteCuentas c
        ON c.cuenta = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_fecha_informacion >= :fecha_inicio
      AND h.dw_fecha_informacion < DATEADD(DAY, 1, CAST(:fecha_fin AS DATE))
      AND h.dw_producto = 'CUENTA DIGITAL'
      AND h.PRCODP = 1
      AND h.PRSUBP = 51
    GROUP BY CAST(h.dw_fecha_informacion AS DATE), h.DW_CUENTA_CORPORATIVA
),
Dias AS (
    SELECT CAST(:fecha_inicio AS DATE) AS fecha_informacion
    UNION ALL
    SELECT DATEADD(DAY, 1, fecha_informacion)
    FROM Dias
    WHERE fecha_informacion < CAST(:fecha_fin AS DATE)
),
ResumenDia AS (
    SELECT
        h.fecha_informacion,
        COUNT(*) AS cuentas_reportadas_dia,
        SUM(h.tuvo_fondos_dia) AS cuentas_con_fondos_dia
    FROM HistoricoDiarioCuenta h
    GROUP BY h.fecha_informacion
),
PrimerFondeo AS (
    SELECT
        cuenta,
        MIN(fecha_informacion) AS fecha_primer_fondeo
    FROM HistoricoDiarioCuenta
    WHERE tuvo_fondos_dia = 1
    GROUP BY cuenta
),
PrimerFondeoDia AS (
    SELECT
        p.fecha_primer_fondeo AS fecha_informacion,
        COUNT(*) AS cuentas_con_primer_fondeo_dia
    FROM PrimerFondeo p
    GROUP BY p.fecha_primer_fondeo
),
BaseDiaria AS (
    SELECT
        d.fecha_informacion,
        (SELECT COUNT(*) FROM CohorteCuentas) AS cuentas_creadas_periodo,
        COALESCE(r.cuentas_reportadas_dia, 0) AS cuentas_reportadas_dia,
        COALESCE(r.cuentas_con_fondos_dia, 0) AS cuentas_con_fondos_dia,
        COALESCE(p.cuentas_con_primer_fondeo_dia, 0) AS cuentas_con_primer_fondeo_dia
    FROM Dias d
    LEFT JOIN ResumenDia r
        ON r.fecha_informacion = d.fecha_informacion
    LEFT JOIN PrimerFondeoDia p
        ON p.fecha_informacion = d.fecha_informacion
)
SELECT
    b.fecha_informacion,
    b.cuentas_creadas_periodo,
    b.cuentas_reportadas_dia,
    b.cuentas_con_fondos_dia,
    b.cuentas_con_primer_fondeo_dia,
    SUM(b.cuentas_con_primer_fondeo_dia) OVER (
        ORDER BY b.fecha_informacion
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cuentas_acumuladas_con_fondos
FROM BaseDiaria b
ORDER BY b.fecha_informacion
OPTION (MAXRECURSION 400);
