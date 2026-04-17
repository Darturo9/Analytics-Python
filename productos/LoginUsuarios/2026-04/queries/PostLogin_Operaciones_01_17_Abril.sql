/*
Operaciones de clientes (distintas de login) en abril 2026.
Se filtra por clientes base usando el placeholder {{CLIENTES_BASE_VALUES}}.
*/

WITH clientes_base AS (
    SELECT v.padded_codigo_cliente
    FROM (VALUES
            {{CLIENTES_BASE_VALUES}}
    ) v(padded_codigo_cliente)
)
SELECT
    st.dw_fecha_trx AS fecha_evento,
    RTRIM(LTRIM(st.CLCCLI)) AS codigo_usuario,
    RIGHT('00000000' + RTRIM(LTRIM(st.CLCCLI)), 8) AS padded_codigo_usuario,
    st.SECODE AS secode,
    COALESCE(srv.SEDESC + ' - ' + st.SECODE, st.SECODE) AS operacion
FROM dw_bel_IBSTTRA_VIEW st
INNER JOIN clientes_base cb
    ON cb.padded_codigo_cliente = RIGHT('00000000' + RTRIM(LTRIM(st.CLCCLI)), 8)
LEFT JOIN DW_BEL_IBSERV srv
    ON srv.SECODE = st.SECODE
WHERE st.dw_fecha_trx >= '2026-04-01'
  AND st.dw_fecha_trx <  '2026-04-18'
  AND st.CLCCLI IS NOT NULL
  AND st.SECODE IS NOT NULL
  AND LTRIM(RTRIM(st.SECODE)) <> ''
  AND st.SECODE NOT IN ('app-login', 'web-login', 'login');
