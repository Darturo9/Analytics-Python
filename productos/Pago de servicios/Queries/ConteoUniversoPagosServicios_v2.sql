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
pagos_bxi AS (
    SELECT DISTINCT
        u.padded_codigo_cliente,
        UPPER(COALESCE(NULLIF(LTRIM(RTRIM(s.inserv)), ''), 'SIN_DATO')) AS canal_pago
    FROM universo u
    INNER JOIN dw_bel_ibjour j
        ON LTRIM(RTRIM(j.clccli)) = u.codigo_cliente_raw
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
),
pagos_multi AS (
    SELECT DISTINCT
        u.padded_codigo_cliente,
        UPPER(COALESCE(NULLIF(LTRIM(RTRIM(p.spcpde)), ''), 'SIN_DATO')) AS canal_pago
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
    WHERE p.sppafr = 'N'
      AND p.spcodc IN (
            '866','882','130','143','184','227','237','238','309','368','371',
            '446','459','478','507','512','526','571','574','643','680','687',
            '734','755','885','888','481','907','693','572','573','732','498',
            '524','513','868','869','408','784'
      )
      AND x.cif_raw IS NOT NULL
),
pagadores_todos AS (
    SELECT DISTINCT padded_codigo_cliente
    FROM (
        SELECT padded_codigo_cliente FROM pagos_bxi
        UNION ALL
        SELECT padded_codigo_cliente FROM pagos_multi
    ) t
),
pagadores_app AS (
    SELECT DISTINCT padded_codigo_cliente
    FROM (
        SELECT padded_codigo_cliente FROM pagos_bxi WHERE canal_pago = 'APP'
        UNION ALL
        SELECT padded_codigo_cliente FROM pagos_multi WHERE canal_pago = 'APP'
    ) t
)
SELECT
    (SELECT COUNT(*) FROM universo) AS clientes_universo,
    (SELECT COUNT(*) FROM pagadores_todos) AS clientes_con_pago_todos_canales,
    (SELECT COUNT(*) FROM universo) - (SELECT COUNT(*) FROM pagadores_todos) AS clientes_sin_pago_todos_canales,
    (SELECT COUNT(*) FROM pagadores_app) AS clientes_con_pago_app,
    (SELECT COUNT(*) FROM universo) - (SELECT COUNT(*) FROM pagadores_app) AS clientes_sin_pago_app;
