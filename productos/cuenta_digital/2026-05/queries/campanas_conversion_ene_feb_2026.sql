/*
==============================================================================
campanas_conversion_ene_feb_2026.sql
------------------------------------------------------------------------------
Propósito:
  Por cada campaña del listado, mostrar clientes únicos contactados y cuántos
  aperturaron Cuenta Digital entre enero y febrero 2026.

Campañas incluidas:
  45538, 45704, 45750, 45841, 45919, 45998,
  46105, 46151, 46280, 46383, 46498, 46707, 46944

Salida: CampaignID | clientes_unicos | con_apertura_ene_feb
==============================================================================
*/

WITH campanas AS (
    SELECT
        c.CampaignID,
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
        45538, 45704, 45750, 45841, 45919, 45998,
        46105, 46151, 46280, 46383, 46498, 46707, 46944
    )
),

aperturas AS (
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(d.cldoc)), 8) AS padded_codigo_cliente
    FROM dw_dep_depositos d
    WHERE d.dw_feha_apertura >= '2026-01-01'
      AND d.dw_feha_apertura <  '2026-03-01'
      AND d.dw_producto      = 'CUENTA DIGITAL'
      AND d.PRCODP           = 1
      AND d.PRSUBP           = 51
)

SELECT
    cam.CampaignID                              AS campana,
    COUNT(DISTINCT cam.padded_codigo_cliente)   AS clientes_unicos,
    COUNT(DISTINCT ape.padded_codigo_cliente)   AS con_apertura_ene_feb
FROM campanas cam
LEFT JOIN aperturas ape
    ON cam.padded_codigo_cliente = ape.padded_codigo_cliente
GROUP BY cam.CampaignID
ORDER BY cam.CampaignID;
