/*
Conteo consolidado (sin detalle 1 a 1) para campaña 43106
*/

WITH base_clientes AS (
    SELECT
        u.Codigo_Cliente,
        MIN(cli.bounceDate) AS Fecha_Rebote,
        MIN(o.opendate) AS Fecha_Open,
        MIN(c.clickdate) AS Fecha_Click
    FROM RTM_APP_HisCampaignUniverso u WITH (NOLOCK)
    LEFT JOIN VW_RTM_TBL_Mongo_DB_Bounce cli WITH (NOLOCK)
        ON cli.EmailLogID = u.SmsID
    LEFT JOIN VW_RTM_TBL_Mongo_DB_Opens o WITH (NOLOCK)
        ON o.EmailLogID = u.SmsID
    LEFT JOIN VW_RTM_TBL_Mongo_DB_clicks c WITH (NOLOCK)
        ON c.EmailLogID = u.SmsID
    WHERE u.CampaignID IN (43106)
      AND u.Codigo_Cliente NOT IN ('26642381', '27875271', '40345491', '50142391', '41542421')
    GROUP BY u.Codigo_Cliente
)
SELECT
    COUNT(*) AS total_clientes,
    SUM(CASE WHEN Fecha_Rebote IS NULL THEN 1 ELSE 0 END) AS entregados,
    SUM(CASE WHEN Fecha_Rebote IS NOT NULL THEN 1 ELSE 0 END) AS rebotados,
    SUM(CASE WHEN Fecha_Open IS NOT NULL THEN 1 ELSE 0 END) AS abrieron,
    SUM(CASE WHEN Fecha_Open IS NULL THEN 1 ELSE 0 END) AS no_abrieron,
    SUM(CASE WHEN Fecha_Click IS NOT NULL THEN 1 ELSE 0 END) AS hicieron_click,
    SUM(CASE WHEN Fecha_Click IS NULL THEN 1 ELSE 0 END) AS no_hicieron_click
FROM base_clientes;
