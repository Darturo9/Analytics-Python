SELECT DISTINCT
        RIGHT('00000000' + LEFT(RTRIM(LTRIM(u.codigo_cliente)), LEN(RTRIM(LTRIM(u.codigo_cliente))) - 1), 8) AS padded_codigo_cliente
FROM dwhbi.dbo.DW_RTM_APP_CAMPAIGN c
    INNER JOIN dwhbi.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO u
        ON u.CampaignID = c.CampaignID
    WHERE
        c.CampaignID IN (47194,47190,47188)