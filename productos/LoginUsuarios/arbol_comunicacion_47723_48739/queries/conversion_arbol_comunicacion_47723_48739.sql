-- =====================================================================
-- Árbol de Comunicación Sin Login — Conversión por envío (FechaAplica)
-- Campañas:
--   47723 = Recordatorio 2
--   47955 = Recordatorio 3
--   48101 = Recordatorio 4
--   48311 = Recordatorio 5
--   48514 = Recordatorio 6
--   48739 = Recordatorio 7
--
-- Reglas implementadas:
-- 1) Unidad de análisis: cada envío (cliente + campaña + FechaAplica)
-- 2) Si cliente cae en dos campañas el mismo día: prioriza la de ID mayor
-- 3) Ventana de conversión: día 0 al día 7 desde FechaAplica (8 días)
-- 4) Si hay nuevo envío antes de que venza la ventana, se corta en el día previo al nuevo envío
-- 5) KPI de conversión: login (app-login, web-login, login)
--
-- Parámetros esperados (desde Python):
--   fecha_inicio (DATE) obligatorio
--   fecha_fin    (DATE) opcional (NULL = sin tope)
-- =====================================================================

WITH campana_raw AS (

    SELECT DISTINCT
        RIGHT('00000000' + LEFT(RTRIM(LTRIM(h.Codigo_Cliente)),
              LEN(RTRIM(LTRIM(h.Codigo_Cliente))) - 1), 8) AS padded_codigo_cliente,
        CAST(h.FechaAplica AS DATE) AS fecha_comunicacion,
        h.CampaignID,
        CASE h.CampaignID
            WHEN '47723' THEN 'Recordatorio 2'
            WHEN '47955' THEN 'Recordatorio 3'
            WHEN '48101' THEN 'Recordatorio 4'
            WHEN '48311' THEN 'Recordatorio 5'
            WHEN '48514' THEN 'Recordatorio 6'
            WHEN '48739' THEN 'Recordatorio 7'
        END AS tipo_campana
    FROM dwhbi.dbo.DW_RTM_APP_CAMPAIGN c
    INNER JOIN dwhbi.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO h
        ON c.CampaignID = h.CampaignID
    WHERE c.CampaignID IN ('47723', '47955', '48101', '48311', '48514', '48739')
      AND CAST(h.FechaAplica AS DATE) >= CAST(:fecha_inicio AS DATE)
      AND CAST(h.FechaAplica AS DATE) <= COALESCE(CAST(:fecha_fin AS DATE), CAST(h.FechaAplica AS DATE))

),

campana_dedup_mismo_dia AS (

    SELECT
        padded_codigo_cliente,
        fecha_comunicacion,
        CampaignID,
        tipo_campana
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.padded_codigo_cliente, r.fecha_comunicacion
                ORDER BY r.CampaignID DESC
            ) AS rn
        FROM campana_raw r
    ) x
    WHERE x.rn = 1

),

envios AS (

    SELECT
        d.padded_codigo_cliente,
        d.fecha_comunicacion,
        d.CampaignID,
        d.tipo_campana,
        LEAD(d.fecha_comunicacion) OVER (
            PARTITION BY d.padded_codigo_cliente
            ORDER BY d.fecha_comunicacion, d.CampaignID DESC
        ) AS siguiente_envio_cliente
    FROM campana_dedup_mismo_dia d

),

envios_con_ventana AS (

    SELECT
        e.padded_codigo_cliente,
        e.CampaignID,
        e.tipo_campana,
        e.fecha_comunicacion,
        e.siguiente_envio_cliente,
        CASE
            WHEN e.siguiente_envio_cliente IS NULL
                THEN DATEADD(DAY, 7, e.fecha_comunicacion)
            WHEN DATEADD(DAY, -1, e.siguiente_envio_cliente) < DATEADD(DAY, 7, e.fecha_comunicacion)
                THEN DATEADD(DAY, -1, e.siguiente_envio_cliente)
            ELSE DATEADD(DAY, 7, e.fecha_comunicacion)
        END AS fecha_fin_ventana
    FROM envios e

),

logins AS (

    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(st.clccli)), 8) AS padded_codigo_cliente,
        CAST(st.dw_fecha_trx AS DATE) AS fecha_login
    FROM dw_bel_IBSTTRA_VIEW st
    WHERE st.clccli IS NOT NULL
      AND st.secode IN ('app-login', 'web-login', 'login')

),

conversion_por_envio AS (

    SELECT
        w.padded_codigo_cliente,
        w.CampaignID,
        w.tipo_campana,
        w.fecha_comunicacion,
        w.siguiente_envio_cliente,
        w.fecha_fin_ventana,
        MIN(l.fecha_login) AS fecha_conversion_login
    FROM envios_con_ventana w
    LEFT JOIN logins l
        ON w.padded_codigo_cliente = l.padded_codigo_cliente
       AND l.fecha_login >= w.fecha_comunicacion
       AND l.fecha_login <= w.fecha_fin_ventana
    GROUP BY
        w.padded_codigo_cliente,
        w.CampaignID,
        w.tipo_campana,
        w.fecha_comunicacion,
        w.siguiente_envio_cliente,
        w.fecha_fin_ventana

)

SELECT
    c.padded_codigo_cliente,
    c.CampaignID,
    c.tipo_campana,
    c.fecha_comunicacion,
    c.siguiente_envio_cliente,
    c.fecha_fin_ventana,
    c.fecha_conversion_login,
    CASE
        WHEN c.fecha_conversion_login IS NOT NULL
            THEN DATEDIFF(DAY, c.fecha_comunicacion, c.fecha_conversion_login)
        ELSE NULL
    END AS dias_a_conversion,
    CASE
        WHEN c.fecha_conversion_login IS NOT NULL THEN 1
        ELSE 0
    END AS convirtio_login
FROM conversion_por_envio c
ORDER BY
    c.fecha_comunicacion,
    c.CampaignID,
    c.padded_codigo_cliente;
