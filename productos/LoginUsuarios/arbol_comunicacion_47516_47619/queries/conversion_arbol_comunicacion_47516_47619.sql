-- =====================================================================
-- Árbol de Comunicación Sin Login — Conversión por envío (FechaAplica)
-- Campañas:
--   47516 = Oferta Inicial
--   47619 = Recordatorio 1
--
-- Reglas implementadas:
-- 1) Unidad de análisis: cada envío (cliente + campaña + FechaAplica)
-- 2) Si cliente cae en ambas campañas el mismo día: prioriza 47619
-- 3) Ventana de conversión: día 0, 1 y 2 desde FechaAplica
-- 4) Si hay un nuevo envío antes de convertir, se corta la ventana del envío previo
-- 5) KPI de conversión: cambio de contraseña (sin login)
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
            WHEN '47516' THEN 'Oferta Inicial'
            WHEN '47619' THEN 'Recordatorio 1'
        END AS tipo_campana,
        CASE h.CampaignID
            WHEN '47619' THEN 2
            WHEN '47516' THEN 1
            ELSE 0
        END AS prioridad_campana
    FROM dwhbi.dbo.DW_RTM_APP_CAMPAIGN c
    INNER JOIN dwhbi.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO h
        ON c.CampaignID = h.CampaignID
    WHERE c.CampaignID IN ('47516', '47619')
      AND CAST(h.FechaAplica AS DATE) >= CAST(:fecha_inicio AS DATE)
      AND CAST(h.FechaAplica AS DATE) <= COALESCE(CAST(:fecha_fin AS DATE), CAST(h.FechaAplica AS DATE))

),

campana_dedup_mismo_dia AS (

    SELECT
        padded_codigo_cliente,
        fecha_comunicacion,
        CampaignID,
        tipo_campana,
        prioridad_campana
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.padded_codigo_cliente, r.fecha_comunicacion
                ORDER BY r.prioridad_campana DESC, r.CampaignID DESC
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
        d.prioridad_campana,
        LEAD(d.fecha_comunicacion) OVER (
            PARTITION BY d.padded_codigo_cliente
            ORDER BY d.fecha_comunicacion, d.prioridad_campana DESC, d.CampaignID DESC
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
                THEN DATEADD(DAY, 2, e.fecha_comunicacion)
            WHEN DATEADD(DAY, -1, e.siguiente_envio_cliente) < DATEADD(DAY, 2, e.fecha_comunicacion)
                THEN DATEADD(DAY, -1, e.siguiente_envio_cliente)
            ELSE DATEADD(DAY, 2, e.fecha_comunicacion)
        END AS fecha_fin_ventana
    FROM envios e

),

cambios_pass AS (

    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(u.CLCCLI)), 8) AS padded_codigo_cliente,
        CAST(u.dw_fecha_cambio_pass AS DATE) AS fecha_cambio_pass
    FROM DW_BEL_IBUSER u
    WHERE u.CLCCLI IS NOT NULL
      AND u.dw_fecha_cambio_pass IS NOT NULL

),

conversion_por_envio AS (

    SELECT
        w.padded_codigo_cliente,
        w.CampaignID,
        w.tipo_campana,
        w.fecha_comunicacion,
        w.siguiente_envio_cliente,
        w.fecha_fin_ventana,
        MIN(p.fecha_cambio_pass) AS fecha_conversion_password
    FROM envios_con_ventana w
    LEFT JOIN cambios_pass p
        ON w.padded_codigo_cliente = p.padded_codigo_cliente
       AND p.fecha_cambio_pass >= w.fecha_comunicacion
       AND p.fecha_cambio_pass <= w.fecha_fin_ventana
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
    c.fecha_conversion_password,
    CASE
        WHEN c.fecha_conversion_password IS NOT NULL
            THEN DATEDIFF(DAY, c.fecha_comunicacion, c.fecha_conversion_password)
        ELSE NULL
    END AS dias_a_conversion,
    CASE
        WHEN c.fecha_conversion_password IS NOT NULL THEN 1
        ELSE 0
    END AS convirtio_password
FROM conversion_por_envio c
ORDER BY
    c.fecha_comunicacion,
    c.CampaignID,
    c.padded_codigo_cliente;
