SELECT
    RTRIM(LTRIM(clccli)) AS codigo_usuario,
    RIGHT('00000000' + RTRIM(LTRIM(clccli)), 8) AS padded_codigo_usuario,
    uscode AS nombre_usuario,
    secode AS canal_login,
    dw_fecha_trx AS fecha_inicio
FROM dw_bel_IBSTTRA_VIEW
WHERE dw_fecha_trx >= '2026-04-01'
  AND dw_fecha_trx < '2026-04-08'
  AND secode IN ('app-login', 'web-login', 'login')
  AND clccli IS NOT NULL;
