WITH universo AS (
    SELECT
        LTRIM(RTRIM(c.cldoc)) AS codigo_cliente_raw,
        RIGHT('00000000' + LTRIM(RTRIM(c.cldoc)), 8) AS padded_codigo_cliente,
        MAX(c.clnomb) AS nombre_cliente
    FROM dw_cif_clientes c
    WHERE c.estatu = 'A'
      AND c.cltipe = 'N'
      AND c.dw_usuarios_bel_cnt > 0
      AND c.cldoc IS NOT NULL
    GROUP BY
        LTRIM(RTRIM(c.cldoc)),
        RIGHT('00000000' + LTRIM(RTRIM(c.cldoc)), 8)
),
pagos_detalle AS (
    -- Fuente BXI con parámetros de PagosdeServicios.sql
    SELECT
        u.codigo_cliente_raw,
        u.padded_codigo_cliente,
        u.nombre_cliente,
        CAST(j.dw_fecha_journal AS DATE) AS fecha_pago,
        s.sedesc AS nombre_pago,
        s.secode AS codigo_pago,
        CAST(j.jovalo AS DECIMAL(18, 2)) AS monto_pago,
        'BXI' AS origen_pago,
        COALESCE(NULLIF(LTRIM(RTRIM(s.inserv)), ''), 'SIN_DATO') AS canal_pago
    FROM universo u
    INNER JOIN dw_bel_ibjour j
        ON LTRIM(RTRIM(j.clccli)) = u.codigo_cliente_raw
    INNER JOIN dw_bel_ibserv s
        ON j.secode = s.secode
       AND s.inserv = 'APP'
       AND s.tiserv = 'O'
       AND s.seuspr = 'S'
       AND s.secode IN (
            'ap-pagclar','app-pagcla','ope-rccl','app-reccla',
            'app-ptigo','pag-tigo','app-rectig','ope-rctg',
            'app-paenee','pag-enee','app-asps','pag-asps',
            'app-achtrf','app-trach','app-transf','app-transh','app-transt',
            'app-tcpago','app-pagotc','app-paptmo','pago-ptmos','ope-psarah'
       )
    WHERE (:fecha_inicio IS NULL OR j.dw_fecha_journal >= :fecha_inicio)
      AND j.jostat = 1
      AND j.josecu = 1
      AND j.jovalo > 0

    UNION ALL

    -- Fuente Multipago con parámetros de PagosdeServicios.sql
    SELECT
        u.codigo_cliente_raw,
        u.padded_codigo_cliente,
        u.nombre_cliente,
        CAST(p.dw_fecha_operacion_sp AS DATE) AS fecha_pago,
        m.spnomc AS nombre_pago,
        p.spcodc AS codigo_pago,
        CAST(p.sppava AS DECIMAL(18, 2)) AS monto_pago,
        'MULTIPAGO' AS origen_pago,
        COALESCE(NULLIF(LTRIM(RTRIM(p.spcpde)), ''), 'SIN_DATO') AS canal_pago
    FROM dw_mul_sppadat p
    INNER JOIN dw_mul_spmaco m
        ON p.spcodc = m.spcodc
    CROSS APPLY (
        SELECT CASE
            WHEN p.spinus IS NULL THEN NULL
            WHEN PATINDEX('%[A-Za-z]%', p.spinus) > 1 THEN LEFT(p.spinus, PATINDEX('%[A-Za-z]%', p.spinus) - 1)
            WHEN PATINDEX('%[A-Za-z]%', p.spinus) = 0 THEN p.spinus
            ELSE NULL
        END AS cif_raw
    ) x
    INNER JOIN universo u
        ON LTRIM(RTRIM(x.cif_raw)) = u.codigo_cliente_raw
    WHERE (:fecha_inicio IS NULL OR p.dw_fecha_operacion_sp >= :fecha_inicio)
      AND p.spcpde = 'App'
      AND p.sppafr = 'N'
      AND p.spcodc IN (
            '866','882','130','143','184','227','237','238','309','368','371',
            '446','459','478','507','512','526','571','574','643','680','687',
            '734','755','885','888','481','907','693','572','573','732','498',
            '524','513','868','869','408','784'
      )
      AND x.cif_raw IS NOT NULL
),
resumen_cliente AS (
    SELECT
        padded_codigo_cliente,
        COUNT(*) AS cantidad_pagos_detectados,
        SUM(monto_pago) AS monto_total_pagos,
        MAX(fecha_pago) AS fecha_ultimo_pago
    FROM pagos_detalle
    GROUP BY padded_codigo_cliente
),
primer_pago AS (
    SELECT
        pd.*,
        ROW_NUMBER() OVER (
            PARTITION BY pd.padded_codigo_cliente
            ORDER BY pd.fecha_pago ASC, pd.origen_pago ASC, pd.codigo_pago ASC
        ) AS rn
    FROM pagos_detalle pd
)
SELECT
    pp.codigo_cliente_raw AS codigo_cliente,
    pp.padded_codigo_cliente,
    pp.nombre_cliente,
    pp.fecha_pago AS fecha_primer_pago,
    pp.nombre_pago AS nombre_primer_pago,
    pp.codigo_pago AS codigo_primer_pago,
    pp.monto_pago AS monto_primer_pago,
    pp.origen_pago AS origen_primer_pago,
    pp.canal_pago AS canal_primer_pago,
    rc.cantidad_pagos_detectados,
    rc.monto_total_pagos,
    rc.fecha_ultimo_pago
FROM primer_pago pp
INNER JOIN resumen_cliente rc
    ON pp.padded_codigo_cliente = rc.padded_codigo_cliente
WHERE pp.rn = 1
ORDER BY pp.padded_codigo_cliente;
