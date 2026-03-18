WITH universo AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM(c.cldoc)), 8) AS padded_codigo_cliente
    FROM dw_cif_clientes c
    WHERE c.estatu = 'A'
      AND c.cltipe = 'N'
      AND c.dw_usuarios_bel_cnt > 0
      AND c.cldoc IS NOT NULL
),
pagos_bxi_todos AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM(j.clccli)), 8) AS padded_codigo_cliente,
        COALESCE(NULLIF(LTRIM(RTRIM(s.inserv)), ''), 'SIN_DATO') AS canal_pago
    FROM dw_bel_ibjour j
    INNER JOIN dw_bel_ibserv s
        ON j.secode = s.secode
       AND s.tiserv = 'O'
       AND s.seuspr = 'S'
       AND s.secode IN (
            'ap-pagclar','app-pagcla','ope-rccl','app-reccla',
            'app-ptigo','pag-tigo','app-rectig','ope-rctg',
            'app-paenee','pag-enee','app-asps','pag-asps',
            'app-achtrf','app-trach','app-transf','app-transh','app-transt',
            'app-tcpago','app-pagotc','app-paptmo','pago-ptmos','ope-psarah'
       )
    WHERE j.jostat = 1
      AND j.josecu = 1
      AND j.jovalo > 0
      AND j.clccli IS NOT NULL
),
pagos_multi_todos AS (
    SELECT DISTINCT
        RIGHT('00000000' + LTRIM(RTRIM(x.cif_raw)), 8) AS padded_codigo_cliente,
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
    WHERE p.sppafr = 'N'
      AND p.spcodc IN (
            '866','882','130','143','184','227','237','238','309','368','371',
            '446','459','478','507','512','526','571','574','643','680','687',
            '734','755','885','888','481','907','693','572','573','732','498',
            '524','513','868','869','408','784'
      )
      AND x.cif_raw IS NOT NULL
),
pagadores_todos_canales AS (
    SELECT DISTINCT padded_codigo_cliente
    FROM (
        SELECT padded_codigo_cliente FROM pagos_bxi_todos
        UNION ALL
        SELECT padded_codigo_cliente FROM pagos_multi_todos
    ) t
),
pagadores_app AS (
    SELECT DISTINCT padded_codigo_cliente
    FROM (
        SELECT padded_codigo_cliente
        FROM pagos_bxi_todos
        WHERE UPPER(canal_pago) = 'APP'
        UNION ALL
        SELECT padded_codigo_cliente
        FROM pagos_multi_todos
        WHERE UPPER(canal_pago) = 'APP'
    ) t
),
universo_marcado AS (
    SELECT
        u.padded_codigo_cliente,
        CASE WHEN pt.padded_codigo_cliente IS NOT NULL THEN 1 ELSE 0 END AS tiene_pago_todos_canales,
        CASE WHEN pa.padded_codigo_cliente IS NOT NULL THEN 1 ELSE 0 END AS tiene_pago_app
    FROM universo u
    LEFT JOIN pagadores_todos_canales pt
        ON pt.padded_codigo_cliente = u.padded_codigo_cliente
    LEFT JOIN pagadores_app pa
        ON pa.padded_codigo_cliente = u.padded_codigo_cliente
)
SELECT
    COUNT(*) AS clientes_universo,
    SUM(tiene_pago_todos_canales) AS clientes_con_pago_todos_canales,
    COUNT(*) - SUM(tiene_pago_todos_canales) AS clientes_sin_pago_todos_canales,
    SUM(tiene_pago_app) AS clientes_con_pago_app,
    COUNT(*) - SUM(tiene_pago_app) AS clientes_sin_pago_app
FROM universo_marcado;
