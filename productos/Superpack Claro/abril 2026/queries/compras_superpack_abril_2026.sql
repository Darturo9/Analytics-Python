/*
==============================================================================
Descripcion:
Compras de Superpack en abril 2026 (detalle transaccional).
==============================================================================
*/

WITH trx_superpack AS (
    SELECT
        RIGHT(
            '00000000' + LTRIM(RTRIM(
                CASE
                    WHEN p.spinus IS NULL THEN NULL
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) > 1
                        THEN LEFT(p.spinus, PATINDEX('%[A-Za-z]%', p.spinus) - 1)
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) = 1 THEN NULL
                    ELSE p.spinus
                END
            )),
            8
        ) AS padded_codigo_cliente,
        CONVERT(date, p.dw_fecha_operacion_sp) AS fecha_operacion,
        TRY_CONVERT(INT, p.spcodc) AS codigo_superpack,
        COALESCE(NULLIF(LTRIM(RTRIM(CAST(p.spcpde AS VARCHAR(60)))), ''), 'SIN_DATO') AS canal_operacion_raw,
        TRY_CONVERT(INT, p.spcpco) AS canal_operacion_codigo,
        CAST(p.sppava AS DECIMAL(18, 2)) AS monto_operacion
    FROM dw_mul_sppadat p
    INNER JOIN dw_mul_spmaco m
        ON p.spcodc = m.spcodc
    WHERE p.dw_fecha_operacion_sp >= '2026-04-01'
      AND p.dw_fecha_operacion_sp <  '2026-05-01'
      AND p.sppafr = 'N'
      AND TRY_CONVERT(INT, p.spcodc) = 498
)
SELECT
    padded_codigo_cliente,
    fecha_operacion,
    codigo_superpack,
    canal_operacion_raw,
    canal_operacion_codigo,
    monto_operacion
FROM trx_superpack
WHERE padded_codigo_cliente IS NOT NULL
ORDER BY fecha_operacion, padded_codigo_cliente;
