WITH CohorteCuentas AS (
    SELECT
        d.DW_CUENTA_CORPORATIVA AS cuenta
    FROM dw_dep_depositos d
    WHERE CAST(d.dw_feha_apertura AS DATE) BETWEEN :fecha_inicio AND :fecha_fin
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
    WHERE CAST(h.dw_fecha_informacion AS DATE) BETWEEN :fecha_inicio AND :fecha_fin
    GROUP BY CAST(h.dw_fecha_informacion AS DATE), h.DW_CUENTA_CORPORATIVA
),
PrimerFondeo AS (
    SELECT
        cuenta,
        MIN(fecha_informacion) AS fecha_primer_fondeo
    FROM HistoricoDiarioCuenta
    WHERE tuvo_fondos_dia = 1
    GROUP BY cuenta
),
Dias AS (
    SELECT CAST(:fecha_inicio AS DATE) AS fecha_informacion
    UNION ALL
    SELECT DATEADD(DAY, 1, fecha_informacion)
    FROM Dias
    WHERE fecha_informacion < CAST(:fecha_fin AS DATE)
)
SELECT
    d.fecha_informacion,
    (SELECT COUNT(*) FROM CohorteCuentas) AS cuentas_creadas_periodo,
    COUNT(DISTINCT h.cuenta) AS cuentas_reportadas_dia,
    COUNT(DISTINCT CASE WHEN h.tuvo_fondos_dia = 1 THEN h.cuenta END) AS cuentas_con_fondos_dia,
    COUNT(DISTINCT CASE WHEN pf.fecha_primer_fondeo = d.fecha_informacion THEN pf.cuenta END) AS cuentas_con_primer_fondeo_dia,
    COUNT(DISTINCT CASE WHEN pf.fecha_primer_fondeo <= d.fecha_informacion THEN pf.cuenta END) AS cuentas_acumuladas_con_fondos
FROM Dias d
LEFT JOIN HistoricoDiarioCuenta h
    ON h.fecha_informacion = d.fecha_informacion
LEFT JOIN PrimerFondeo pf
    ON pf.cuenta = h.cuenta
GROUP BY d.fecha_informacion
ORDER BY d.fecha_informacion
OPTION (MAXRECURSION 400);
