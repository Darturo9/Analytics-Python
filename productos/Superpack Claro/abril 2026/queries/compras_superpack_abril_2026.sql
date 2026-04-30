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
    WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
      AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
      AND p.sppafr = 'N'
      AND TRY_CONVERT(INT, p.spcodc) = 498
)
SELECT
    t.padded_codigo_cliente,
    t.fecha_operacion,
    t.codigo_superpack,
    t.canal_operacion_raw,
    t.canal_operacion_codigo,
    t.monto_operacion
FROM trx_superpack t
LEFT JOIN (
    SELECT
        LTRIM(RTRIM(CLDOC)) AS CLDOC,
        CLTIPE,
        ROW_NUMBER() OVER (
            PARTITION BY LTRIM(RTRIM(CLDOC))
            ORDER BY CASE WHEN CLTIPE = 'N' THEN 1 WHEN CLTIPE IS NULL THEN 2 ELSE 3 END
        ) AS RN
    FROM DW_CIF_CLIENTES
) CIF ON CIF.CLDOC = t.padded_codigo_cliente AND CIF.RN = 1
WHERE t.padded_codigo_cliente IS NOT NULL
  AND (CIF.CLTIPE <> 'J' OR CIF.CLTIPE IS NULL)
ORDER BY t.fecha_operacion, t.padded_codigo_cliente;
