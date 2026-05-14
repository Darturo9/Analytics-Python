/*
==============================================================================
clientes_campanas_apertura_generacion.sql
------------------------------------------------------------------------------
Propósito:
  Clientes únicos contactados desde marzo 2026 en las campañas de Cuenta Digital,
  cuántos abrieron su cuenta y agrupación por generación de los que abrieron.

Campañas incluidas (3 grupos):
  Grupo A: 47188, 47190, 47194, 47245, 47246, 47339, 47370, 47446, 47448,
            47494, 47499, 47500, 47668, 47670, 47671, 47750, 47789, 47848,
            47908, 47909, 47911, 48026, 48028, 48029
  Grupo B: 48070, 48071, 48072, 48262, 48263, 48264, 48338, 48379, 48380,
            48457, 48512, 48513, 48569, 48608, 48649, 48735, 48736, 48737,
            48759, 48760, 48812, 48911, 48912
  Grupo C: 48950, 48952, 49027, 49058, 49220

Parámetros: ninguno (hardcoded marzo 2026 en adelante)
==============================================================================
*/

WITH campanas AS (
    SELECT DISTINCT
        RIGHT(
            '00000000' + LEFT(
                RTRIM(LTRIM(h.Codigo_Cliente)),
                LEN(RTRIM(LTRIM(h.Codigo_Cliente))) - 1
            ),
            8
        ) AS padded_codigo_cliente
    FROM DWHBI.dbo.DW_RTM_APP_CAMPAIGN c
    INNER JOIN DWHBI.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO h
        ON h.CampaignID = c.CampaignID
    WHERE c.CampaignID IN (
        -- Grupo A
        47188, 47190, 47194, 47245, 47246, 47339, 47370, 47446, 47448,
        47494, 47499, 47500, 47668, 47670, 47671, 47750, 47789, 47848,
        47908, 47909, 47911, 48026, 48028, 48029,
        -- Grupo B
        48070, 48071, 48072, 48262, 48263, 48264, 48338, 48379, 48380,
        48457, 48512, 48513, 48569, 48608, 48649, 48735, 48736, 48737,
        48759, 48760, 48812, 48911, 48912,
        -- Grupo C
        48950, 48952, 49027, 49058, 49220
    )
),

aperturas AS (
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(d.cldoc)), 8)    AS padded_codigo_cliente,
        d.dw_cuenta_corporativa                          AS numero_cuenta,
        CAST(d.dw_feha_apertura AS DATE)                 AS fecha_apertura,
        CAST(cli.DW_FECHA_NACIMIENTO AS DATE)            AS fecha_nacimiento,
        CASE
            WHEN YEAR(cli.DW_FECHA_NACIMIENTO) BETWEEN 1965 AND 1980
                THEN 'Generation X (1965-1980)'
            WHEN YEAR(cli.DW_FECHA_NACIMIENTO) BETWEEN 1981 AND 1996
                THEN 'Gen Y - Millennials (1981-1996)'
            WHEN YEAR(cli.DW_FECHA_NACIMIENTO) BETWEEN 1997 AND 2012
                THEN 'Generación Z (1997-2012)'
            ELSE 'Otra Generación'
        END AS generacion,
        ROW_NUMBER() OVER (
            PARTITION BY RTRIM(LTRIM(d.cldoc))
            ORDER BY d.dw_feha_apertura ASC
        ) AS rn
    FROM dw_dep_depositos d
    LEFT JOIN dw_cif_clientes cli
        ON RTRIM(LTRIM(d.cldoc)) = RTRIM(LTRIM(cli.cldoc))
    WHERE d.dw_feha_apertura >= '2026-03-01'
      AND d.dw_producto     = 'CUENTA DIGITAL'
      AND d.PRCODP          = 1
      AND d.PRSUBP          = 51
),

aperturas_dedup AS (
    -- Una fila por cliente (primera apertura)
    SELECT
        padded_codigo_cliente,
        numero_cuenta,
        fecha_apertura,
        fecha_nacimiento,
        generacion
    FROM aperturas
    WHERE rn = 1
),

-- ── RESULTADO 1: resumen total ──────────────────────────────────────────────
resumen AS (
    SELECT
        COUNT(DISTINCT cam.padded_codigo_cliente) AS total_clientes_campanas,
        COUNT(DISTINCT ape.numero_cuenta)          AS total_abrieron_cuenta
    FROM campanas cam
    LEFT JOIN aperturas_dedup ape
        ON cam.padded_codigo_cliente = ape.padded_codigo_cliente
),

-- ── RESULTADO 2: aperturas por generación ──────────────────────────────────
por_generacion AS (
    SELECT
        ape.generacion,
        COUNT(DISTINCT ape.numero_cuenta)          AS cuentas_aperturadas,
        COUNT(DISTINCT cam.padded_codigo_cliente)   AS clientes_unicos
    FROM campanas cam
    INNER JOIN aperturas_dedup ape
        ON cam.padded_codigo_cliente = ape.padded_codigo_cliente
    GROUP BY ape.generacion
)

-- Selecciona qué resultado quieres: descomenta uno de los dos bloques.

-- RESUMEN TOTAL
-- SELECT * FROM resumen;

-- POR GENERACIÓN
SELECT
    generacion,
    clientes_unicos,
    cuentas_aperturadas
FROM por_generacion
ORDER BY cuentas_aperturadas DESC;
