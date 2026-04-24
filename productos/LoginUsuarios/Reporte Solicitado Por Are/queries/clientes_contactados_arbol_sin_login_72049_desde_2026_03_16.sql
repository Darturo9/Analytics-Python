/*
Clientes contactados - Arbol sin login
Campana 72049 + login desde primera comunicacion (2026-03-16)
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
FROM dwhbi.dbo.DW_RTM_APP_CAMPAIGN c
INNER JOIN dwhbi.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO hu
    ON c.CampaignID = hu.CampaignID
WHERE c.Name LIKE '%72049%'
  AND c.Name LIKE '%login%'
  AND c.Start_date >= '2026-03-16';
