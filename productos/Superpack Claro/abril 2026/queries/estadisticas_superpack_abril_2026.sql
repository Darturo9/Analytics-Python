-------- Estadisticas de compras Superpack Claro - Abril 2026
-------- Excluye reversas (sppafr = 'N')
-------- Agrupa por monto para identificar el mas comprado y el promedio
WITH trx_efectivas AS (
    SELECT
        CAST(p.sppava AS DECIMAL(18, 2)) AS monto_operacion
    FROM dw_mul_sppadat p
    LEFT JOIN dw_mul_spmaco m
        ON m.spcodc = p.spcodc
    LEFT JOIN (
        SELECT
            LTRIM(RTRIM(DW_BEL_IBUSER.CLCCLI)) CLCCLI,
            LTRIM(RTRIM(DW_BEL_IBUSER.USCODE)) USCODE
        FROM DW_BEL_IBUSER
    ) ClientesBel
        ON LTRIM(RTRIM(p.SPINUS)) = (ClientesBel.CLCCLI + ClientesBel.USCODE)
    LEFT JOIN (
        SELECT LTRIM(RTRIM(CLDOC)) CLDOC, CLTIPE,
            ROW_NUMBER() OVER (
                PARTITION BY LTRIM(RTRIM(CLDOC))
                ORDER BY CASE WHEN CLTIPE = 'N' THEN 1 WHEN CLTIPE IS NULL THEN 2 ELSE 3 END
            ) AS RN
        FROM DW_CIF_CLIENTES
    ) CIF ON CIF.CLDOC = ClientesBel.CLCCLI AND CIF.RN = 1
    WHERE p.DW_FECHA_OPERACION_SP >= '2026-04-01'
      AND p.DW_FECHA_OPERACION_SP <  '2026-05-01'
      AND TRY_CONVERT(INT, p.spcodc) = 498
      AND p.spcpco IN (1, 7)
      AND m.CLMOCO IN ('001', 'L')
      AND p.sppafr = 'N'
      AND ClientesBel.CLCCLI IS NOT NULL
      AND (CIF.CLTIPE <> 'J' OR CIF.CLTIPE IS NULL)
)
SELECT
    monto_operacion,
    COUNT(*)                                                              AS total_trx,
    CAST(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()
        AS DECIMAL(5, 2)
    )                                                                     AS pct_del_total
FROM trx_efectivas
GROUP BY monto_operacion
ORDER BY total_trx DESC;
