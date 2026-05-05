/*
DiagnosticoSecodesExcluidosAbril2026.sql
-----------------------------------------
Muestra los secodes de BXI que NO estan clasificados en la query principal
para clientes fondeados en abril 2026.

Sirve para identificar si hay tipos de transaccion relevantes que deban
agregarse como categoria en UsoDineroCuentasFondeadasAbril2026.sql.

Salida:
- secode: codigo de operacion en BXI
- total_transacciones
- clientes_unicos
- monto_total
*/

WITH cuentas_abiertas AS (
    SELECT
        d.DW_CUENTA_CORPORATIVA,
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente
    FROM dw_dep_depositos d
    WHERE d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
      AND d.dw_feha_apertura >= '2026-04-01'
      AND d.dw_feha_apertura <  '2026-05-01'
    GROUP BY
        d.DW_CUENTA_CORPORATIVA,
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8)
),
universo_fondeado AS (
    SELECT a.padded_codigo_cliente
    FROM cuentas_abiertas a
    WHERE EXISTS (
        SELECT 1
        FROM HIS_DEP_DEPOSITOS_VIEW h
        WHERE h.DW_CUENTA_CORPORATIVA = a.DW_CUENTA_CORPORATIVA
          AND h.dw_fecha_informacion >= '2026-04-01'
          AND h.dw_fecha_informacion <  '2026-05-01'
          AND h.ctt001 > 0
    )
    GROUP BY a.padded_codigo_cliente
)
SELECT
    j.secode,
    COUNT(*)                                         AS total_transacciones,
    COUNT(DISTINCT n.padded_codigo_cliente)          AS clientes_unicos,
    CAST(SUM(j.jovalo) AS DECIMAL(18, 2))            AS monto_total
FROM dw_bel_ibjour j
CROSS APPLY (
    VALUES (RIGHT('00000000' + LTRIM(RTRIM(j.clccli)), 8))
) AS n(padded_codigo_cliente)
INNER JOIN universo_fondeado u
    ON u.padded_codigo_cliente = n.padded_codigo_cliente
WHERE j.dw_fecha_journal >= '2026-04-01'
  AND j.dw_fecha_journal <  '2026-05-01'
  AND j.jostat = 1
  AND j.josecu = 1
  AND j.jovalo > 0
  AND j.secode NOT IN (
      'ap-pagclar', 'app-pagcla', 'ope-rccl', 'app-reccla',
      'app-ptigo', 'pag-tigo', 'app-rectig', 'ope-rctg',
      'app-paenee', 'pag-enee',
      'app-asps', 'pag-asps',
      'app-achtrf', 'app-trach', 'app-transh',
      'app-transf', 'app-transt', 'app-tcpago', 'app-pagotc'
  )
GROUP BY j.secode
ORDER BY total_transacciones DESC;
