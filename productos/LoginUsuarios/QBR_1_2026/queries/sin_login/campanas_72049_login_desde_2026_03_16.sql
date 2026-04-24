/*
Campanas RTM (nombre y fecha de envio) para 72049 + login
*/

SELECT DISTINCT
    c.name,
    c.start_date
FROM dwhbi.dbo.DW_RTM_APP_CAMPAIGN c
INNER JOIN dwhbi.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO hu
    ON c.CampaignID = hu.CampaignID
WHERE c.Name LIKE '%72049%'
  AND c.Name LIKE '%login%'
  AND c.Start_date >= '2026-03-16';
