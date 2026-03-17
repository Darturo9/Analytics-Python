SELECT DISTINCT base.padded_codigo_cliente from(
		SELECT dw_codigo_cliente69 AS padded_codigo_cliente
		FROM [DWHBI].[dbo].[DW_CENTROBI_TABLA_CIF69]
)as base