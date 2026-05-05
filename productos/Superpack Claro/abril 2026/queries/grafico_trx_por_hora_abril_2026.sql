-------- Transacciones por hora del dia - SUPERPACK-CLARO - Abril 2026 (para grafico)
-------- Campo hora: SPPAHR (campo nativo de DW_MUL_SPPADAT)
SELECT
    CASE
        WHEN TRY_CONVERT(INT, p.SPPAHR) IS NULL     THEN NULL
        WHEN TRY_CONVERT(INT, p.SPPAHR) < 24        THEN TRY_CONVERT(INT, p.SPPAHR)
        WHEN TRY_CONVERT(INT, p.SPPAHR) < 2400      THEN TRY_CONVERT(INT, p.SPPAHR) / 100
        ELSE                                              TRY_CONVERT(INT, p.SPPAHR) / 10000
    END                                     AS hora,
    COUNT(*)                                AS total_transacciones,
    COUNT(DISTINCT ClientesBel.CLCCLI)      AS clientes_unicos
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
  AND (CIF.CLTIPE <> 'J' OR CIF.CLTIPE IS NULL)
GROUP BY
    CASE
        WHEN TRY_CONVERT(INT, p.SPPAHR) IS NULL     THEN NULL
        WHEN TRY_CONVERT(INT, p.SPPAHR) < 24        THEN TRY_CONVERT(INT, p.SPPAHR)
        WHEN TRY_CONVERT(INT, p.SPPAHR) < 2400      THEN TRY_CONVERT(INT, p.SPPAHR) / 100
        ELSE                                              TRY_CONVERT(INT, p.SPPAHR) / 10000
    END
ORDER BY hora;
