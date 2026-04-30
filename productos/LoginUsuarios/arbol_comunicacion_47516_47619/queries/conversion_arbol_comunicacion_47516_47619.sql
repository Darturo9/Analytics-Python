-- ============================================================
-- Árbol de Comunicación Sin Login — Conversión por Cliente
-- Campaña 47516: Oferta Inicial
-- Campaña 47619: Recordatorio 1
--
-- KPI primario : cambio de contraseña (meta_cumplida)
-- KPI secundario: login (tuvo_login)
-- ============================================================

WITH campana AS (

    SELECT DISTINCT
        RIGHT('00000000' + LEFT(RTRIM(LTRIM(h.Codigo_Cliente)),
              LEN(RTRIM(LTRIM(h.Codigo_Cliente))) - 1), 8)  AS padded_codigo_cliente,
        CAST(h.FechaAplica AS DATE)                         AS fecha_comunicacion,
        h.CampaignID,
        CASE h.CampaignID
            WHEN '47516' THEN 'Oferta Inicial'
            WHEN '47619' THEN 'Recordatorio 1'
        END AS tipo_campana
    FROM DW_RTM_APP_CAMPAIGN c
    INNER JOIN DW_RTM_APP_HISCAMPAIGNUNIVERSO h ON c.CampaignID = h.CampaignID
    WHERE c.CampaignID IN ('47516', '47619')

),

logins AS (

    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(b.CLCCLI)), 8) AS padded_codigo_cliente,
        CAST(b.dw_fecha_trx AS DATE)                   AS fecha_login
    FROM dw_bel_IBSTTRA_VIEW b
    WHERE b.SECODE IN ('app-login', 'web-login', 'login')
      AND b.CLCCLI IS NOT NULL

),

cambios_pass AS (

    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(u.CLCCLI)), 8)  AS padded_codigo_cliente,
        CAST(u.dw_fecha_cambio_pass AS DATE)            AS fecha_cambio_pass
    FROM DW_BEL_IBUSER u
    WHERE u.CLCCLI IS NOT NULL
      AND u.dw_fecha_cambio_pass IS NOT NULL

)

SELECT
    c.padded_codigo_cliente,
    c.CampaignID,
    c.tipo_campana,
    c.fecha_comunicacion,

    -- KPI primario: cambio de contraseña
    MIN(p.fecha_cambio_pass)                                        AS fecha_primer_cambio_pass,
    DATEDIFF(DAY, c.fecha_comunicacion, MIN(p.fecha_cambio_pass))   AS dias_cambio_pass,
    CASE WHEN MIN(p.fecha_cambio_pass) IS NOT NULL THEN 1 ELSE 0 END AS meta_cumplida,

    -- KPI secundario: login
    MIN(l.fecha_login)                                              AS fecha_primer_login,
    DATEDIFF(DAY, c.fecha_comunicacion, MIN(l.fecha_login))         AS dias_login,
    CASE WHEN MIN(l.fecha_login) IS NOT NULL THEN 1 ELSE 0 END      AS tuvo_login,

    -- Detalle de qué acción(es) realizó el cliente
    CASE
        WHEN MIN(p.fecha_cambio_pass) IS NOT NULL AND MIN(l.fecha_login) IS NOT NULL THEN 'Cambio Pass + Login'
        WHEN MIN(p.fecha_cambio_pass) IS NOT NULL                                     THEN 'Solo Cambio Password'
        WHEN MIN(l.fecha_login)       IS NOT NULL                                     THEN 'Solo Login'
        ELSE                                                                               'No convirtio'
    END AS tipo_conversion,

    -- Días hasta el primer evento (lo que ocurra primero; 0 = mismo día de comunicación)
    DATEDIFF(DAY, c.fecha_comunicacion,
        CASE
            WHEN MIN(l.fecha_login) IS NULL                                 THEN MIN(p.fecha_cambio_pass)
            WHEN MIN(p.fecha_cambio_pass) IS NULL                           THEN MIN(l.fecha_login)
            WHEN MIN(l.fecha_login) <= MIN(p.fecha_cambio_pass)             THEN MIN(l.fecha_login)
            ELSE                                                                  MIN(p.fecha_cambio_pass)
        END
    ) AS dias_primer_evento

FROM campana c
LEFT JOIN logins l
    ON  c.padded_codigo_cliente = l.padded_codigo_cliente
    AND l.fecha_login       >= c.fecha_comunicacion
LEFT JOIN cambios_pass p
    ON  c.padded_codigo_cliente = p.padded_codigo_cliente
    AND p.fecha_cambio_pass >= c.fecha_comunicacion
GROUP BY
    c.padded_codigo_cliente,
    c.CampaignID,
    c.tipo_campana,
    c.fecha_comunicacion
ORDER BY
    c.tipo_campana,
    c.padded_codigo_cliente
