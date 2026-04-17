/*
Operaciones post-login abril 2026 usando 4 fuentes:
- BEL (dw_bel_IBSTTRA_VIEW)
- Journal (dw_BEL_IBJOUR)
- ACH (DW_DEP_DPMOVM_VIEW)
- Multipagos (DW_MUL_SPPADAT)

Se filtra por clientes base usando el placeholder {{CLIENTES_BASE_VALUES}}.
*/

WITH clientes_base AS (
    SELECT v.padded_codigo_cliente
    FROM (VALUES
            {{CLIENTES_BASE_VALUES}}
    ) v(padded_codigo_cliente)
),
eventos_bel AS (
    SELECT
        st.dw_fecha_trx AS fecha_evento,
        LTRIM(RTRIM(st.CLCCLI)) AS codigo_usuario,
        RIGHT('00000000' + LTRIM(RTRIM(st.CLCCLI)), 8) AS padded_codigo_usuario,
        CASE
            WHEN st.SECODE IN ('login', 'web-login') THEN 'Web'
            WHEN st.SECODE = 'app-login' THEN 'App'
            ELSE COALESCE(st.CACODE, 'Otro')
        END AS canal,
        'BEL' AS fuente,
        st.SECODE AS secode,
        st.SECODE AS modulo,
        COALESCE(srv.SEDESC + ' - ' + st.SECODE, st.SECODE) AS operacion,
        CAST(0 AS DECIMAL(18, 2)) AS valor,
        CAST(0 AS DECIMAL(18, 2)) AS valorlempirizado
    FROM dw_bel_IBSTTRA_VIEW st
    INNER JOIN clientes_base cb
        ON cb.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(st.CLCCLI)), 8)
    LEFT JOIN DW_BEL_IBSERV srv
        ON srv.SECODE = st.SECODE
    WHERE st.dw_fecha_trx >= '2026-04-01'
      AND st.dw_fecha_trx <  '2026-04-18'
      AND st.CLCCLI IS NOT NULL
      AND st.SECODE IN (
            'pym-desemb', 'app-edocta', 'res-edoct2', 'con-sal2',
            'app-hisptm', 'app-extcns', 'seg-cnhtrx', 'mpg-ccnspg',
            'app-cnsdiv', 'estado-cta', 'con-sal', 'res-ctacor',
            'mpg-ccnhp', 'adq-edocta', 'dei-cnpag', 'dei-cnsapg',
            'adq-cnstrx', 'ptr-cnsptm', 'pym-cnsinf', 'ptr-cnssal',
            'chp-cnschq', 'adq-cnsrec', 'pym-cnsmov', 'ptr-cnspar',
            'adq-voltrx', 'cns-chqtrc', 'cpr-lincre', 'app-divinf',
            'adq-cncrad', 'adq-estcra', 'ach-cnsope', 'adq-cnspos',
            'pla-consul', 'prf-receml', 'prf-recosm', 'TRANSF-INT', 'APP-TRAINT'
      )
),
eventos_journal AS (
    SELECT
        j.dw_fecha_journal AS fecha_evento,
        LTRIM(RTRIM(j.CLCCLI)) AS codigo_usuario,
        RIGHT('00000000' + LTRIM(RTRIM(j.CLCCLI)), 8) AS padded_codigo_usuario,
        CASE
            WHEN j.CACODE = 'AP' THEN 'App'
            WHEN j.CACODE = 'IB' THEN 'Web'
            ELSE COALESCE(j.CACODE, 'Otro')
        END AS canal,
        'JOURNAL' AS fuente,
        CAST(j.SECODE AS VARCHAR(60)) AS secode,
        'Trx Journal' AS modulo,
        COALESCE(srv.SEDESC + ' - ' + j.SECODE, j.SECODE) AS operacion,
        CAST(j.JOVAOR AS DECIMAL(18, 2)) AS valor,
        CAST(j.JOVAOR AS DECIMAL(18, 2)) AS valorlempirizado
    FROM dw_BEL_IBJOUR j
    INNER JOIN clientes_base cb
        ON cb.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(j.CLCCLI)), 8)
    LEFT JOIN DW_BEL_IBSERV srv
        ON srv.SECODE = j.SECODE
    WHERE j.dw_fecha_journal >= '2026-04-01'
      AND j.dw_fecha_journal <  '2026-04-18'
      AND j.CLCCLI IS NOT NULL
      AND j.JOSTAT = 1
      AND j.JOSECU = CASE
                         WHEN j.SECODE = 'ope-cmpdiv' AND j.dw_fecha_journal > '2023-06-17' THEN 2
                         ELSE 1
                     END
      AND j.CACODE IN ('IB', 'AP')
      AND j.SECODE NOT IN (
            'mpg-cpago', 'cns-cdv', 'cns-pcit', 'cns-vdv',
            'app-cpago', 'app-cnspci', 'app-debtar', 'app-cnscdv'
      )
),
eventos_ach AS (
    SELECT
        mv.dw_fecha_operacion AS fecha_evento,
        LTRIM(RTRIM(dep.CLDOC)) AS codigo_usuario,
        RIGHT('00000000' + LTRIM(RTRIM(dep.CLDOC)), 8) AS padded_codigo_usuario,
        'Web y App' AS canal,
        'ACH' AS fuente,
        CAST(mv.TRXCOD AS VARCHAR(60)) AS secode,
        CASE
            WHEN mv.TRXCOD IN (4210, 4212, 4232, 4234) THEN 'ACH QR'
            ELSE 'ACH'
        END AS modulo,
        COALESCE(mv.dw_transaccion, 'ACH') AS operacion,
        CAST(mv.MVVAL AS DECIMAL(18, 2)) AS valor,
        CAST(mv.MVVAL AS DECIMAL(18, 2)) AS valorlempirizado
    FROM DW_DEP_DPMOVM_VIEW mv
    INNER JOIN DW_DEP_DEPOSITOS dep
        ON dep.DW_CUENTA_CORPORATIVA = mv.dw_cuenta_corporativa
    INNER JOIN clientes_base cb
        ON cb.padded_codigo_cliente = RIGHT('00000000' + LTRIM(RTRIM(dep.CLDOC)), 8)
    WHERE mv.dw_fecha_operacion >= '2026-04-01'
      AND mv.dw_fecha_operacion <  '2026-04-18'
      AND mv.MVSTAT <> 'R'
      AND mv.TRXCOD IN (4823, 4825, 4824, 4822, 4652, 4654, 4651, 4653, 4210, 4212, 4232, 4234)
      AND dep.CLDOC IS NOT NULL
),
eventos_multipagos AS (
    SELECT
        p.DW_FECHA_OPERACION_SP AS fecha_evento,
        cod.Codigo_Cliente AS codigo_usuario,
        cod.padded_codigo_cliente AS padded_codigo_usuario,
        COALESCE(p.SPCPDE, 'App') AS canal,
        'MULTIPAGOS' AS fuente,
        CAST(p.SPCODC AS VARCHAR(60)) AS secode,
        'Multipagos' AS modulo,
        COALESCE(m.SPNOMC, 'Multipagos') AS operacion,
        CAST(p.SPPAVA AS DECIMAL(18, 2)) AS valor,
        CAST(p.SPPAVA AS DECIMAL(18, 2)) AS valorlempirizado
    FROM DW_MUL_SPPADAT p
    INNER JOIN DW_MUL_SPMACO m
        ON m.SPCODC = p.SPCODC
    CROSS APPLY (
        VALUES (
            LTRIM(RTRIM(
                CASE
                    WHEN p.SPINUS IS NULL THEN NULL
                    WHEN PATINDEX('%[A-Za-z]%', p.SPINUS) > 1 THEN LEFT(p.SPINUS, PATINDEX('%[A-Za-z]%', p.SPINUS) - 1)
                    WHEN PATINDEX('%[A-Za-z]%', p.SPINUS) = 1 THEN NULL
                    ELSE p.SPINUS
                END
            ))
        )
    ) src(Codigo_Cliente)
    CROSS APPLY (
        VALUES (
            src.Codigo_Cliente,
            CASE
                WHEN src.Codigo_Cliente IS NULL OR src.Codigo_Cliente = '' THEN NULL
                ELSE RIGHT('00000000' + src.Codigo_Cliente, 8)
            END
        )
    ) cod(Codigo_Cliente, padded_codigo_cliente)
    INNER JOIN clientes_base cb
        ON cb.padded_codigo_cliente = cod.padded_codigo_cliente
    WHERE p.DW_FECHA_OPERACION_SP >= '2026-04-01'
      AND p.DW_FECHA_OPERACION_SP <  '2026-04-18'
      AND p.SPCPCO IN (1, 7)
      AND p.SPPAFR = 'N'
      AND cod.padded_codigo_cliente IS NOT NULL
)
SELECT *
FROM (
    SELECT * FROM eventos_bel
    UNION ALL
    SELECT * FROM eventos_journal
    UNION ALL
    SELECT * FROM eventos_ach
    UNION ALL
    SELECT * FROM eventos_multipagos
) e
WHERE e.padded_codigo_usuario IS NOT NULL;
