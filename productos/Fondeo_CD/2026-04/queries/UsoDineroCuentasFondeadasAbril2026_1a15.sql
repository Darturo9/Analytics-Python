/*
Uso de dinero para cuentas fondeadas en abril 2026 (1 al 15)

Universo:
- Cuentas de CUENTA DIGITAL abiertas del 1 al 15 de abril 2026.
- De ese universo, solo cuentas con fondeo (ctt001 > 0) al menos un dia
  en el mismo rango (1 al 15 de abril 2026).

Salida:
- Tipo de uso/transaccion
- Total de transacciones
- Clientes unicos
- Monto total y promedio
*/

WITH cuentas_abiertas_1a15 AS (
    SELECT DISTINCT
        d.DW_CUENTA_CORPORATIVA,
        RIGHT('00000000' + LTRIM(RTRIM(d.CLDOC)), 8) AS padded_codigo_cliente
    FROM dw_dep_depositos d
    WHERE d.dw_producto = 'CUENTA DIGITAL'
      AND d.PRCODP = 1
      AND d.PRSUBP = 51
      AND CAST(d.dw_feha_apertura AS DATE) >= '2026-04-01'
      AND CAST(d.dw_feha_apertura AS DATE) <  '2026-04-16'
),
cuentas_fondeadas_1a15 AS (
    SELECT DISTINCT
        h.DW_CUENTA_CORPORATIVA
    FROM HIS_DEP_DEPOSITOS_VIEW h
    INNER JOIN cuentas_abiertas_1a15 a
        ON a.DW_CUENTA_CORPORATIVA = h.DW_CUENTA_CORPORATIVA
    WHERE CAST(h.dw_fecha_informacion AS DATE) >= '2026-04-01'
      AND CAST(h.dw_fecha_informacion AS DATE) <  '2026-04-16'
      AND h.ctt001 > 0
),
universo_fondeado AS (
    SELECT DISTINCT
        a.padded_codigo_cliente
    FROM cuentas_abiertas_1a15 a
    INNER JOIN cuentas_fondeadas_1a15 f
        ON f.DW_CUENTA_CORPORATIVA = a.DW_CUENTA_CORPORATIVA
),
pagos_bxi AS (
    SELECT
        u.padded_codigo_cliente,
        CAST(j.dw_fecha_journal AS DATE) AS fecha_transaccion,
        CASE
            WHEN s.secode IN ('ap-pagclar', 'app-pagcla', 'ope-rccl', 'app-reccla') THEN 'Pago Claro'
            WHEN s.secode IN ('app-ptigo', 'pag-tigo', 'app-rectig', 'ope-rctg') THEN 'Pago Tigo'
            WHEN s.secode IN ('app-paenee', 'pag-enee') THEN 'Pago Electricidad'
            WHEN s.secode IN ('app-asps', 'pag-asps') THEN 'Pago Agua'
            WHEN s.secode IN ('app-achtrf', 'app-trach', 'app-transh') THEN 'Transferencias ACH'
            WHEN s.secode = 'app-transf' THEN 'Transferencias Propias'
            WHEN s.secode = 'app-transt' THEN 'Transferencias a terceros'
            WHEN s.secode = 'app-tcpago' THEN 'Pago TC'
            WHEN s.secode = 'app-pagotc' THEN 'Pago TC terceros'
            ELSE 'OTRAS_TRANSACCIONES_BXI'
        END AS tipo_uso,
        CAST(j.jovalo AS DECIMAL(18, 2)) AS valor,
        'BXI' AS origen
    FROM dw_bel_ibjour j
    LEFT JOIN dw_bel_ibserv s
        ON j.secode = s.secode
    CROSS APPLY (
        VALUES (RIGHT('00000000' + LTRIM(RTRIM(j.clccli)), 8))
    ) AS n(padded_codigo_cliente)
    INNER JOIN universo_fondeado u
        ON u.padded_codigo_cliente = n.padded_codigo_cliente
    WHERE j.dw_fecha_journal >= '2026-04-01'
      AND j.dw_fecha_journal <  '2026-04-16'
      AND j.jostat = 1
      AND j.josecu = 1
      AND j.jovalo > 0
),
pagos_multi_raw AS (
    SELECT
        u.padded_codigo_cliente,
        CAST(p.dw_fecha_operacion_sp AS DATE) AS fecha_transaccion,
        TRY_CONVERT(INT, p.spcodc) AS codigo_int,
        TRY_CONVERT(INT, m.SPCCAT) AS categoria_int,
        CAST(p.sppava AS DECIMAL(18, 2)) AS valor
    FROM dw_mul_sppadat p
    INNER JOIN dw_mul_spmaco m
        ON p.spcodc = m.spcodc
    CROSS APPLY (
        VALUES (
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
            )
        )
    ) AS n(padded_codigo_cliente)
    INNER JOIN universo_fondeado u
        ON u.padded_codigo_cliente = n.padded_codigo_cliente
    WHERE p.dw_fecha_operacion_sp >= '2026-04-01'
      AND p.dw_fecha_operacion_sp <  '2026-04-16'
      AND p.sppafr = 'N'
      AND (
          TRY_CONVERT(INT, p.spcodc) IN (481, 907, 693, 524, 572, 573, 732, 498, 866, 882, 513, 868, 869)
          OR TRY_CONVERT(INT, m.SPCCAT) IN (3, 11)
      )
),
pagos_multi AS (
    SELECT
        padded_codigo_cliente,
        fecha_transaccion,
        CASE
            WHEN categoria_int = 3 THEN 'Pago Educacion'
            WHEN codigo_int IN (481, 907) THEN 'Pago Electricidad'
            WHEN codigo_int = 693 THEN 'Pago Licencia'
            WHEN codigo_int = 524 THEN 'Pago Tigo'
            WHEN codigo_int IN (572, 573, 732) THEN 'Pago Cable'
            WHEN codigo_int = 498 THEN 'Pago Claro'
            WHEN codigo_int IN (866, 882) THEN 'Pago Impuestos'
            WHEN categoria_int = 11 THEN 'Pago Agua'
            WHEN codigo_int IN (513, 868, 869) THEN 'Pago Matricula vehiculos'
            ELSE 'OTRAS_TRANSACCIONES_MULTIPAGO'
        END AS tipo_uso,
        valor,
        'MULTIPAGO' AS origen
    FROM pagos_multi_raw
),
uso_dinero AS (
    SELECT padded_codigo_cliente, fecha_transaccion, tipo_uso, origen, valor
    FROM pagos_bxi
    UNION ALL
    SELECT padded_codigo_cliente, fecha_transaccion, tipo_uso, origen, valor
    FROM pagos_multi
)
SELECT
    tipo_uso,
    COUNT(*) AS total_transacciones,
    COUNT(DISTINCT padded_codigo_cliente) AS clientes_unicos,
    CAST(SUM(valor) AS DECIMAL(18, 2)) AS monto_total,
    CAST(AVG(valor) AS DECIMAL(18, 2)) AS monto_promedio
FROM uso_dinero
GROUP BY tipo_uso
ORDER BY total_transacciones DESC, tipo_uso ASC;
