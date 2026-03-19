WITH pagos_base AS (
    -- BXI
    SELECT
        RIGHT('00000000' + RTRIM(LTRIM(j.clccli)), 8) AS cif_toda_transaccion,
        j.jovalo AS valor,
        s.SEDESC AS descrip,
        CAST(NULL AS VARCHAR(20)) AS codigo,
        CAST(NULL AS VARCHAR(20)) AS maestro_multi_cat,
        j.dw_fecha_journal AS fecha_pago,
        s.secode AS codigo_multi,
        CONCAT(CAST(j.JOLOTE AS VARCHAR(50)), 'J') AS id_transaccion
    FROM dw_bel_ibjour j
    INNER JOIN dw_bel_ibserv s
        ON j.secode = s.secode
       AND s.tiserv = 'O'
       AND s.seuspr = 'S'
    WHERE j.dw_fecha_journal >= '2025-01-01'
      AND j.jostat = 1
      AND j.josecu = 1
      AND j.jovalo > 0

    UNION ALL

    -- Multipago
    SELECT
        RIGHT(
            '00000000' + RTRIM(LTRIM(
                CASE
                    WHEN p.spinus IS NULL THEN NULL
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) > 1
                        THEN LEFT(p.spinus, PATINDEX('%[A-Za-z]%', p.spinus) - 1)
                    WHEN PATINDEX('%[A-Za-z]%', p.spinus) = 0
                        THEN p.spinus
                    ELSE NULL
                END
            )), 8
        ) AS cif_toda_transaccion,
        p.SPPAVA AS valor,
        m.SPNOMC AS descrip,
        CAST(p.spcodc AS VARCHAR(20)) AS codigo,
        CAST(m.SPCCAT AS VARCHAR(20)) AS maestro_multi_cat,
        p.DW_FECHA_OPERACION_SP AS fecha_pago,
        CAST(NULL AS VARCHAR(50)) AS codigo_multi,
        CONCAT(CAST(p.SPNUPA AS VARCHAR(50)), 'M') AS id_transaccion
    FROM dw_mul_sppadat p
    INNER JOIN dw_mul_spmaco m
        ON p.spcodc = m.spcodc
    WHERE p.dw_fecha_operacion_sp >= '2025-01-01'
      AND p.sppafr = 'N'
),
pagos_clasificados AS (
    SELECT
        b.*,
        CASE
            WHEN b.codigo_multi IN ('ap-pagclar','app-pagcla','ope-rccl','app-reccla') THEN 'Claro'
            WHEN b.codigo_multi IN ('app-ptigo','pag-tigo','app-rectig','ope-rctg') THEN 'Tigo'
            WHEN b.codigo_multi IN ('app-paenee','pag-enee') THEN 'Electricidad'
            WHEN b.codigo_multi IN ('app-asps','pag-asps') THEN 'Agua'
            WHEN TRY_CONVERT(INT, b.codigo) IN (572, 573, 732) THEN 'Cable'
            WHEN TRY_CONVERT(INT, b.codigo) = 693 THEN 'Licencia'
            WHEN TRY_CONVERT(INT, b.codigo) IN (513, 868, 869) THEN 'Matrícula de vehículos'
            WHEN b.codigo_multi = 'app-pagotc' THEN 'Pago de Tarjeta de Crédito Terceros'
            ELSE NULL
        END AS tipo_pago
    FROM pagos_base b
)
SELECT
    cif_toda_transaccion,
    fecha_pago,
    valor,
    descrip,
    codigo,
    maestro_multi_cat,
    codigo_multi,
    tipo_pago,
    id_transaccion
FROM pagos_clasificados
WHERE tipo_pago IN (
    'Agua',
    'Cable',
    'Claro',
    'Electricidad',
    'Licencia',
    'Matrícula de vehículos',
    'Pago de Tarjeta de Crédito Terceros',
    'Tigo'
);
