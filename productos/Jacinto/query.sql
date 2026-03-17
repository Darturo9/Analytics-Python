   select u.Codigo_Cliente,Fecha_Rebote,bouncesubtype as Motivo_Rebote, 
    CASE
    when fecha_rebote is null then 'SI'
    WHEN FECHA_REBOTE IS NOT NULL THEN 'NO'
    END as Entregado,
    fecha_open,
    case
    when fecha_open is null then 'NO'
    when fecha_open is not null then 'SI'
    END AS Abrio,
    Fecha_click,
    case
    when fecha_click is null then 'NO'
    when fecha_click is not null then 'SI'
    END AS Hizo_click
    from
    (
    SELECT u.Codigo_Cliente,MIN(cli.bounceDate) Fecha_rebote, cli.bouncetype, cli.bouncesubtype, min(o.opendate) Fecha_Open, min(c.clickdate) as Fecha_click
    FROM
    RTM_APP_HisCampaignUniverso u WITH (NOLOCK) 
    --left join TBL_Mongo_DB_deliveries op WITH (NOLOCK) on op.EmailLogID=u.SmsID 
    left join VW_RTM_TBL_Mongo_DB_Bounce cli WITH (NOLOCK) on cli.EmailLogID=u.SmsID 
    left join VW_RTM_TBL_Mongo_DB_Opens o WITH (NOLOCK) on o.EmailLogID=u.SmsID
    left join VW_RTM_TBL_Mongo_DB_clicks c WITH (NOLOCK) on c.EmailLogID=u.SmsID
    WHERE
    u.CampaignID in (43106)
    and 
    Codigo_Cliente not in ('26642381', '27875271', '40345491', '50142391', '41542421')
    group by 
    u.Codigo_Cliente,cli.bouncetype,cli.bouncesubtype
    )u