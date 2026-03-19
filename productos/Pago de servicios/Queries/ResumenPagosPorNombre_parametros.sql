WITH universo AS (
    SELECT DISTINCT
        LTRIM(RTRIM(c.cldoc)) AS codigo_cliente_raw,
        RIGHT('00000000' + LTRIM(RTRIM(c.cldoc)), 8) AS padded_codigo_cliente
    FROM dw_cif_clientes c
    WHERE c.estatu = 'A'
      AND c.cltipe = 'N'
      AND c.dw_usuarios_bel_cnt > 0
      AND c.cldoc IS NOT NULL
),
pagos_detalle AS (
    -- BXI
    SELECT
        u.padded_codigo_cliente,
        CAST(j.dw_fecha_journal AS DATE) AS fecha_pago,
        s.sedesc AS nombre_pago,
        s.secode AS codigo_pago,
        CAST(j.jovalo AS DECIMAL(18, 2)) AS monto_pago,
        'BXI' AS origen_pago,
        'APP' AS canal_pago
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

    -- MULTIPAGO
    SELECT
        u.padded_codigo_cliente,
        CAST(p.dw_fecha_operacion_sp AS DATE) AS fecha_pago,
        m.spnomc AS nombre_pago,
        p.spcodc AS codigo_pago,
        CAST(p.sppava AS DECIMAL(18, 2)) AS monto_pago,
        'MULTIPAGO' AS origen_pago,
        'APP' AS canal_pago
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
)
SELECT
    nombre_pago,
    origen_pago,
    canal_pago,
    COUNT(*) AS total_transacciones,
    COUNT(DISTINCT padded_codigo_cliente) AS clientes_unicos,
    CAST(SUM(monto_pago) AS DECIMAL(18, 2)) AS monto_total
FROM pagos_detalle
GROUP BY
    nombre_pago,
    origen_pago,
    canal_pago
ORDER BY
    total_transacciones DESC,
    clientes_unicos DESC,
    nombre_pago;
