SELECT DISTINCT
    RIGHT('00000000' + RTRIM(LTRIM(base.dw_codigo_cliente69)), 8) AS padded_codigo_cliente
FROM [DWHBI].[dbo].[DW_CENTROBI_TABLA_CIF69] base
WHERE base.dw_codigo_cliente69 IS NOT NULL;
