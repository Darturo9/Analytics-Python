IF OBJECT_ID('tempdb..#universo') IS NOT NULL DROP TABLE #universo;
IF OBJECT_ID('tempdb..#pagadores_todos') IS NOT NULL DROP TABLE #pagadores_todos;
IF OBJECT_ID('tempdb..#pagadores_app') IS NOT NULL DROP TABLE #pagadores_app;

SELECT DISTINCT
    RIGHT('00000000' + LTRIM(RTRIM(c.cldoc)), 8) AS padded_codigo_cliente
INTO #universo
FROM dw_cif_clientes c
WHERE c.estatu = 'A'
  AND c.cltipe = 'N'
  AND c.dw_usuarios_bel_cnt > 0
  AND c.cldoc IS NOT NULL;

CREATE UNIQUE CLUSTERED INDEX IX_universo_padded
ON #universo (padded_codigo_cliente);

SELECT DISTINCT
    t.padded_codigo_cliente
INTO #pagadores_todos
FROM (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(j.clccli)), 8) AS padded_codigo_cliente
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

    UNION

    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(x.cif_raw)), 8) AS padded_codigo_cliente
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
) t
INNER JOIN #universo u
    ON u.padded_codigo_cliente = t.padded_codigo_cliente;

CREATE UNIQUE CLUSTERED INDEX IX_pagadores_todos_padded
ON #pagadores_todos (padded_codigo_cliente);

SELECT DISTINCT
    t.padded_codigo_cliente
INTO #pagadores_app
FROM (
    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(j.clccli)), 8) AS padded_codigo_cliente
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
      AND UPPER(COALESCE(NULLIF(LTRIM(RTRIM(s.inserv)), ''), 'SIN_DATO')) = 'APP'

    UNION

    SELECT
        RIGHT('00000000' + LTRIM(RTRIM(x.cif_raw)), 8) AS padded_codigo_cliente
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
      AND UPPER(COALESCE(NULLIF(LTRIM(RTRIM(p.spcpde)), ''), 'SIN_DATO')) = 'APP'
) t
INNER JOIN #universo u
    ON u.padded_codigo_cliente = t.padded_codigo_cliente;

CREATE UNIQUE CLUSTERED INDEX IX_pagadores_app_padded
ON #pagadores_app (padded_codigo_cliente);

SELECT
    (SELECT COUNT(*) FROM #universo) AS clientes_universo,
    (SELECT COUNT(*) FROM #pagadores_todos) AS clientes_con_pago_todos_canales,
    (SELECT COUNT(*) FROM #universo) - (SELECT COUNT(*) FROM #pagadores_todos) AS clientes_sin_pago_todos_canales,
    (SELECT COUNT(*) FROM #pagadores_app) AS clientes_con_pago_app,
    (SELECT COUNT(*) FROM #universo) - (SELECT COUNT(*) FROM #pagadores_app) AS clientes_sin_pago_app;
