/*
Post-login Q1 2026 (general, no empresarial)
Fuente adaptada de la lógica operativa de query1.sql.

Columnas de salida esperadas por dashboard:
- Fecha
- Codigo_Cliente
- padded_codigo_cliente
- Canal
- Modulo
- Operación
- Valor
- ValorLempirizado

Nota:
- El dashboard reemplaza {{CLIENTES_BASE_VALUES}} con los códigos del Excel
  para filtrar en SQL desde el origen y mejorar tiempos.
*/

WITH clientes_base AS (
    SELECT v.padded_codigo_cliente
    FROM (VALUES
            {{CLIENTES_BASE_VALUES}}
    ) v(padded_codigo_cliente)
),
eventos_bel AS (
    SELECT
        st.dw_fecha_trx AS Fecha,
        LTRIM(RTRIM(st.CLCCLI)) AS Codigo_Cliente,
        cod.padded_codigo_cliente,
        CASE
            WHEN st.SECODE IN ('login', 'web-login') THEN 'Web'
            WHEN st.SECODE = 'app-login' THEN 'App'
            ELSE COALESCE(st.CACODE, 'Otro')
        END AS Canal,
        CASE
            WHEN st.SECODE = 'app-login' THEN 'Login'
            ELSE st.SECODE
        END AS Modulo,
        CASE
            WHEN st.SECODE IN ('login', 'web-login') THEN 'Login Web'
            ELSE COALESCE(srv.SEDESC + ' - ' + st.SECODE, st.SECODE)
        END AS [Operación],
        CAST(0 AS DECIMAL(18, 2)) AS Valor,
        CAST(0 AS DECIMAL(18, 2)) AS ValorLempirizado
    FROM dw_bel_IBSTTRA_VIEW st
    CROSS APPLY (
        VALUES (RIGHT('00000000' + LTRIM(RTRIM(st.CLCCLI)), 8))
    ) cod(padded_codigo_cliente)
    INNER JOIN clientes_base cb
        ON cb.padded_codigo_cliente = cod.padded_codigo_cliente
    LEFT JOIN DW_BEL_IBSERV srv
        ON srv.SECODE = st.SECODE
    WHERE st.dw_fecha_trx >= '2026-01-01'
      AND st.dw_fecha_trx < '2026-04-01'
      AND st.CLCCLI IS NOT NULL
      AND st.SECODE IN (
            'login', 'web-login', 'app-login',
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
        j.dw_fecha_journal AS Fecha,
        LTRIM(RTRIM(j.CLCCLI)) AS Codigo_Cliente,
        cod.padded_codigo_cliente,
        CASE
            WHEN j.CACODE = 'AP' THEN 'App'
            WHEN j.CACODE = 'IB' THEN 'Web'
            ELSE COALESCE(j.CACODE, 'Otro')
        END AS Canal,
        'Trx Journal' AS Modulo,
        COALESCE(srv.SEDESC + ' - ' + j.SECODE, j.SECODE) AS [Operación],
        CAST(j.JOVAOR AS DECIMAL(18, 2)) AS Valor,
        CAST(j.JOVAOR AS DECIMAL(18, 2)) AS ValorLempirizado
    FROM dw_BEL_IBJOUR j
    CROSS APPLY (
        VALUES (RIGHT('00000000' + LTRIM(RTRIM(j.CLCCLI)), 8))
    ) cod(padded_codigo_cliente)
    INNER JOIN clientes_base cb
        ON cb.padded_codigo_cliente = cod.padded_codigo_cliente
    LEFT JOIN DW_BEL_IBSERV srv
        ON srv.SECODE = j.SECODE
    WHERE j.dw_fecha_journal >= '2026-01-01'
      AND j.dw_fecha_journal < '2026-04-01'
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
        mv.dw_fecha_operacion AS Fecha,
        LTRIM(RTRIM(dep.CLDOC)) AS Codigo_Cliente,
        cod.padded_codigo_cliente,
        'Web y App' AS Canal,
        CASE
            WHEN mv.TRXCOD IN (4210, 4212, 4232, 4234) THEN 'ACH QR'
            ELSE 'ACH'
        END AS Modulo,
        COALESCE(mv.dw_transaccion, 'ACH') AS [Operación],
        CAST(mv.MVVAL AS DECIMAL(18, 2)) AS Valor,
        CAST(mv.MVVAL AS DECIMAL(18, 2)) AS ValorLempirizado
    FROM DW_DEP_DPMOVM_VIEW mv
    INNER JOIN DW_DEP_DEPOSITOS dep
        ON dep.DW_CUENTA_CORPORATIVA = mv.dw_cuenta_corporativa
    CROSS APPLY (
        VALUES (RIGHT('00000000' + LTRIM(RTRIM(dep.CLDOC)), 8))
    ) cod(padded_codigo_cliente)
    INNER JOIN clientes_base cb
        ON cb.padded_codigo_cliente = cod.padded_codigo_cliente
    WHERE mv.dw_fecha_operacion >= '2026-01-01'
      AND mv.dw_fecha_operacion < '2026-04-01'
      AND mv.MVSTAT <> 'R'
      AND mv.TRXCOD IN (4823, 4825, 4824, 4822, 4652, 4654, 4651, 4653, 4210, 4212, 4232, 4234)
      AND dep.CLDOC IS NOT NULL
),
eventos_multipagos AS (
    SELECT
        p.DW_FECHA_OPERACION_SP AS Fecha,
        cod.Codigo_Cliente,
        cod.padded_codigo_cliente,
        COALESCE(p.SPCPDE, 'App') AS Canal,
        'Multipagos' AS Modulo,
        COALESCE(m.SPNOMC, 'Multipagos') AS [Operación],
        CAST(p.SPPAVA AS DECIMAL(18, 2)) AS Valor,
        CAST(p.SPPAVA AS DECIMAL(18, 2)) AS ValorLempirizado
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
    WHERE p.DW_FECHA_OPERACION_SP >= '2026-01-01'
      AND p.DW_FECHA_OPERACION_SP < '2026-04-01'
      AND p.SPCPCO IN (1, 7)
      AND p.SPPAFR = 'N'
      AND cod.padded_codigo_cliente IS NOT NULL
)
SELECT
    e.Fecha,
    e.Codigo_Cliente,
    e.padded_codigo_cliente,
    e.Canal,
    e.Modulo,
    e.[Operación],
    e.Valor,
    e.ValorLempirizado
FROM (
    SELECT * FROM eventos_bel
    UNION ALL
    SELECT * FROM eventos_journal
    UNION ALL
    SELECT * FROM eventos_ach
    UNION ALL
    SELECT * FROM eventos_multipagos
) e
WHERE e.padded_codigo_cliente IS NOT NULL;
