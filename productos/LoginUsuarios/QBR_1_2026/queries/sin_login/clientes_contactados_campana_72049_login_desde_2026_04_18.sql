/*
Listado de clientes contactados por campana RTM (login)
Filtros actuales:
- Name contiene 72049
- Name contiene login
- Start_date >= 2026-04-18
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
  AND c.Name LIKE '%login%'
  AND c.Start_date >= '2026-04-18';
