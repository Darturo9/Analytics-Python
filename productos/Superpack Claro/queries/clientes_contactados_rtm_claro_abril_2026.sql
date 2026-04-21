/*
==============================================================================
Descripcion:
Listado de clientes contactados por RTM para campanas Claro (abril 2026+).
==============================================================================
*/

SELECT DISTINCT
    RIGHT(
        '00000000'
        + LEFT(
            RTRIM(LTRIM(hu.Codigo_Cliente)),
            LEN(RTRIM(LTRIM(hu.Codigo_Cliente))) - 1
        ),
        8
    ) AS padded_codigo_cliente
FROM DW_RTM_APP_CAMPAIGN c
INNER JOIN DW_RTM_APP_HISCAMPAIGNUNIVERSO hu
    ON c.CampaignID = hu.CampaignID
WHERE c.Name LIKE '%72049%'
  AND c.Name LIKE '%Claro%'
  AND c.Start_date >= '2026-04-01';
