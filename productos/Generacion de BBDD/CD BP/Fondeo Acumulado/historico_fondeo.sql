WITH cuentas_creadas_periodo AS (
    SELECT DISTINCT
        depositos.DW_CUENTA_CORPORATIVA AS cuenta
    FROM HIS_DEP_DEPOSITOS_VIEW depositos
    WHERE depositos.PRCODP = 1
      AND depositos.PRSUBP = 51
      AND depositos.dw_producto = 'CUENTA DIGITAL'
      AND CAST(depositos.dw_feha_apertura AS DATE) BETWEEN :fecha_inicio AND :fecha_fin
),
base AS (
    SELECT
        CAST(depositos.dw_fecha_informacion AS DATE) AS fecha_informacion,
        depositos.DW_CUENTA_CORPORATIVA AS cuenta,
        CASE
            WHEN COALESCE(depositos.ctt001, 0) > 0
              OR COALESCE(depositos.dw_saldo_promedio, 0) > 0
            THEN 1 ELSE 0
        END AS tuvo_fondos
    FROM HIS_DEP_DEPOSITOS_VIEW depositos
    INNER JOIN cuentas_creadas_periodo creadas
        ON creadas.cuenta = depositos.DW_CUENTA_CORPORATIVA
    WHERE depositos.PRCODP = 1
      AND depositos.PRSUBP = 51
      AND depositos.dw_producto = 'CUENTA DIGITAL'
      AND CAST(depositos.dw_fecha_informacion AS DATE) BETWEEN :fecha_inicio AND :fecha_fin
),
por_cuenta_dia AS (
    SELECT
        fecha_informacion,
        cuenta,
        MAX(tuvo_fondos) AS tuvo_fondos_dia
    FROM base
    GROUP BY fecha_informacion, cuenta
),
primer_fondeo AS (
    SELECT
        cuenta,
        MIN(fecha_informacion) AS fecha_primer_fondeo
    FROM por_cuenta_dia
    WHERE tuvo_fondos_dia = 1
    GROUP BY cuenta
),
dias AS (
    SELECT CAST(:fecha_inicio AS DATE) AS fecha_informacion
    UNION ALL
    SELECT DATEADD(DAY, 1, fecha_informacion)
    FROM dias
    WHERE fecha_informacion < CAST(:fecha_fin AS DATE)
),
cuentas_reportadas_dia AS (
    SELECT
        fecha_informacion,
        COUNT(DISTINCT cuenta) AS cuentas_reportadas_dia
    FROM por_cuenta_dia
    GROUP BY fecha_informacion
),
cuentas_con_fondos_dia AS (
    SELECT
        fecha_informacion,
        COUNT(DISTINCT cuenta) AS cuentas_con_fondos_dia
    FROM por_cuenta_dia
    WHERE tuvo_fondos_dia = 1
    GROUP BY fecha_informacion
),
acumulado AS (
    SELECT
        d.fecha_informacion,
        COUNT(DISTINCT pf.cuenta) AS cuentas_acumuladas_con_fondos
    FROM dias d
    LEFT JOIN primer_fondeo pf
        ON pf.fecha_primer_fondeo <= d.fecha_informacion
    GROUP BY d.fecha_informacion
),
resumen_creadas AS (
    SELECT COUNT(DISTINCT cuenta) AS cuentas_creadas_periodo
    FROM cuentas_creadas_periodo
)
SELECT
    d.fecha_informacion,
    COALESCE(rc.cuentas_creadas_periodo, 0) AS cuentas_creadas_periodo,
    COALESCE(cr.cuentas_reportadas_dia, 0) AS cuentas_reportadas_dia,
    COALESCE(cfd.cuentas_con_fondos_dia, 0) AS cuentas_con_fondos_dia,
    COALESCE(a.cuentas_acumuladas_con_fondos, 0) AS cuentas_acumuladas_con_fondos
FROM dias d
CROSS JOIN resumen_creadas rc
LEFT JOIN cuentas_reportadas_dia cr
    ON cr.fecha_informacion = d.fecha_informacion
LEFT JOIN cuentas_con_fondos_dia cfd
    ON cfd.fecha_informacion = d.fecha_informacion
LEFT JOIN acumulado a
    ON a.fecha_informacion = d.fecha_informacion
ORDER BY d.fecha_informacion
OPTION (MAXRECURSION 400);
