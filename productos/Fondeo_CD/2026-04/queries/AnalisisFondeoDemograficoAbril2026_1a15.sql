-- Analisis demografico y de fondeo para cuentas de Cuenta Digital
-- creadas en abril 2026 completo.
-- Incluye:
-- - genero
-- - departamento
-- - edad / rango_edad / generacion
-- - bandera de fondeo (saldo > 0 al menos 1 vez en abril)
-- - bandera de movimiento (cant_transacciones > 0 en dw_dep_depositos)

WITH universo AS (
    SELECT DISTINCT
        d.DW_CUENTA_CORPORATIVA,
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente,
        CAST(d.dw_feha_apertura AS DATE) AS fecha_apertura,
        COALESCE(d.ctctrx, 0) AS cant_transacciones
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= '2026-04-01'
      AND d.dw_feha_apertura <  '2026-05-01'
      AND d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
),
fondeo_1a15 AS (
    SELECT
        h.DW_CUENTA_CORPORATIVA,
        MAX(CASE WHEN COALESCE(h.ctt001, 0) > 0 THEN 1 ELSE 0 END) AS fondeada_1_15
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN universo u
        ON u.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
    WHERE h.dw_fecha_informacion >= '2026-04-01'
      AND h.dw_fecha_informacion <  '2026-05-01'
    GROUP BY h.DW_CUENTA_CORPORATIVA
),
clientes_raw AS (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8) AS padded_codigo_cliente,
        c.CLISEX AS genero_raw,
        c.DW_FECHA_NACIMIENTO AS fecha_nacimiento,
        ROW_NUMBER() OVER (
            PARTITION BY RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8)
            ORDER BY c.dw_fecha_informacion DESC
        ) AS rn
    FROM DW_CIF_CLIENTES c
    INNER JOIN (SELECT DISTINCT padded_codigo_cliente FROM universo) u
        ON u.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(c.CLDOC)), 8)
),
direcciones_raw AS (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente,
        d.DW_NIVEL_GEO2 AS depto,
        ROW_NUMBER() OVER (
            PARTITION BY RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
            ORDER BY d.dw_fecha DESC
        ) AS rn
    FROM DW_CIF_DIRECCIONES d
    INNER JOIN (SELECT DISTINCT padded_codigo_cliente FROM universo) u
        ON u.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
    WHERE d.CLDICO = 1
)
SELECT
    u.DW_CUENTA_CORPORATIVA AS numero_cuenta,
    u.padded_codigo_cliente,
    u.fecha_apertura,
    CASE
        WHEN UPPER(LTRIM(RTRIM(c.genero_raw))) IN ('F', 'FEMENINO', 'MUJER') THEN 'MUJER'
        WHEN UPPER(LTRIM(RTRIM(c.genero_raw))) IN ('M', 'H', 'MASCULINO', 'HOMBRE') THEN 'HOMBRE'
        ELSE 'SIN_DATO'
    END AS genero,
    COALESCE(NULLIF(LTRIM(RTRIM(d.depto)), ''), 'SIN DEPTO') AS depto,
    c.fecha_nacimiento,
    CASE
        WHEN c.fecha_nacimiento IS NULL THEN NULL
        ELSE DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30')
             - CASE
                 WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30'), c.fecha_nacimiento) > '2026-04-30'
                 THEN 1 ELSE 0
               END
    END AS edad,
    CASE
        WHEN c.fecha_nacimiento IS NULL THEN 'SIN DATO'
        WHEN (
            DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30')
            - CASE
                WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30'), c.fecha_nacimiento) > '2026-04-30'
                THEN 1 ELSE 0
              END
        ) BETWEEN 18 AND 25 THEN '18-25'
        WHEN (
            DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30')
            - CASE
                WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30'), c.fecha_nacimiento) > '2026-04-30'
                THEN 1 ELSE 0
              END
        ) BETWEEN 26 AND 35 THEN '26-35'
        WHEN (
            DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30')
            - CASE
                WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30'), c.fecha_nacimiento) > '2026-04-30'
                THEN 1 ELSE 0
              END
        ) BETWEEN 36 AND 45 THEN '36-45'
        WHEN (
            DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30')
            - CASE
                WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30'), c.fecha_nacimiento) > '2026-04-30'
                THEN 1 ELSE 0
              END
        ) BETWEEN 46 AND 55 THEN '46-55'
        WHEN (
            DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30')
            - CASE
                WHEN DATEADD(YEAR, DATEDIFF(YEAR, c.fecha_nacimiento, '2026-04-30'), c.fecha_nacimiento) > '2026-04-30'
                THEN 1 ELSE 0
              END
        ) >= 56 THEN '56+'
        ELSE 'SIN DATO'
    END AS rango_edad,
    CASE
        WHEN c.fecha_nacimiento IS NULL THEN 'SIN DATO'
        WHEN YEAR(c.fecha_nacimiento) BETWEEN 1965 AND 1980 THEN 'Generation X (1965-1980)'
        WHEN YEAR(c.fecha_nacimiento) BETWEEN 1981 AND 1996 THEN 'Gen Y - Millennials (1981-1996)'
        WHEN YEAR(c.fecha_nacimiento) BETWEEN 1997 AND 2012 THEN 'Generacion Z (1997-2012)'
        ELSE 'OTRA GENERACION'
    END AS generacion,
    COALESCE(f.fondeada_1_15, 0) AS fondeada_1_15,
    CASE WHEN u.cant_transacciones > 0 THEN 1 ELSE 0 END AS con_movimiento,
    u.cant_transacciones
FROM universo u
LEFT JOIN fondeo_1a15 f
    ON f.DW_CUENTA_CORPORATIVA = u.DW_CUENTA_CORPORATIVA
LEFT JOIN clientes_raw c
    ON c.padded_codigo_cliente = u.padded_codigo_cliente
   AND c.rn = 1
LEFT JOIN direcciones_raw d
    ON d.padded_codigo_cliente = u.padded_codigo_cliente
   AND d.rn = 1;
