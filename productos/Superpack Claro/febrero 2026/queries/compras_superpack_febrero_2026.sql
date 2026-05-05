/*
==============================================================================
Descripcion:
Compras de Superpack en febrero 2026 (detalle transaccional).
Criterios alineados con dashboard Tableau y query de trx banca electronica:
  - CLCCLI via JOIN a DW_BEL_IBUSER (misma logica que resumen trx)
  - Solo canales banca electronica: SPCPCO IN (1, 7)
  - Solo lempiras: CLMOCO IN ('001', 'L')
  - Incluye reversas (sppafr) para calculo neto en Python
  - Excluye juridicos: CLTIPE <> 'J'
==============================================================================
*/

WITH trx_superpack AS (
    SELECT
        ClientesBel.CLCCLI                                                           AS padded_codigo_cliente,
        CONVERT(date, p.dw_fecha_operacion_sp)                                       AS fecha_operacion,
        TRY_CONVERT(INT, p.spcodc)                                                   AS codigo_superpack,
        COALESCE(NULLIF(LTRIM(RTRIM(CAST(p.spcpde AS VARCHAR(60)))), ''), 'SIN_DATO') AS canal_operacion_raw,
        TRY_CONVERT(INT, p.spcpco)                                                   AS canal_operacion_codigo,
        CAST(p.sppava AS DECIMAL(18, 2))                                             AS monto_operacion,
        p.sppafr                                                                     AS es_reversa
    FROM dw_mul_sppadat p
    LEFT JOIN dw_mul_spmaco m ON m.spcodc = p.spcodc
    LEFT JOIN (
        SELECT
            LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
            LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
        FROM DW_BEL_IBUSER
    ) ClientesBel ON LTRIM(RTRIM(p.SPINUS)) = (ClientesBel.CLCCLI + ClientesBel.USCODE)
    WHERE p.dw_fecha_operacion_sp >= :fecha_inicio
      AND p.dw_fecha_operacion_sp <  :fecha_fin_exclusiva
      AND TRY_CONVERT(INT, p.spcodc) = 498
      AND p.spcpco IN (1, 7)
      AND m.CLMOCO IN ('001', 'L')
)
SELECT
    t.padded_codigo_cliente,
    t.fecha_operacion,
    t.codigo_superpack,
    t.canal_operacion_raw,
    t.canal_operacion_codigo,
    t.monto_operacion,
    t.es_reversa
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
