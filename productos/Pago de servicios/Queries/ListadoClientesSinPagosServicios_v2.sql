/*
==============================================================================
ListadoClientesSinPagosServicios_v2.sql
------------------------------------------------------------------------------
Versión optimizada de listado de clientes SIN pagos de servicios.

Objetivo:
  Listar clientes elegibles con datos de contacto que NO realizaron pagos de
  servicios en el rango indicado.

Elegibilidad:
  - Cliente natural
  - Cliente activo
  - No empleado Banpaís
  - Tiene banca (usuarios BEL)
  - Cliente BEL activo
  - Usuario BEL activo
  - Cuenta digital activa

Notas de optimización:
  - Fechas con límite superior exclusivo (@fecha_fin_exclusiva)
  - Normalización de código cliente en CTEs (evita repetir transformaciones)
  - Extracción robusta de código desde SPINUS para multipagos
==============================================================================
*/

DECLARE @fecha_inicio DATE = '2026-03-01';
DECLARE @fecha_fin DATE = '2026-03-31';
DECLARE @fecha_fin_exclusiva DATE = DATEADD(DAY, 1, @fecha_fin);

WITH clientes_cif AS (
    SELECT DISTINCT
        RTRIM(LTRIM(cif.CLDOC)) AS codigo_cliente,
        RIGHT('00000000' + RTRIM(LTRIM(cif.CLDOC)), 8) AS padded_codigo_cliente,
        cif.CLNOMB AS nombre_cliente,
        cif.CLTIPE AS tipo_cliente,
        cif.CLCLCO AS clase_cliente,
        cif.ESTATU AS estado_cliente_cif,
        ISNULL(cif.dw_usuarios_bel_cnt, 0) AS banca_cnt,
        YEAR(cif.DW_FECHA_NACIMIENTO) AS anio_nac
    FROM DW_CIF_CLIENTES cif
    WHERE cif.CLTIPE = 'N'                   -- Natural
      AND cif.ESTATU = 'A'                   -- Activo
      AND cif.CLCLCO <> 10                   -- No empleado Banpaís
      AND ISNULL(cif.dw_usuarios_bel_cnt, 0) > 0  -- Tiene banca
),
clientes_bel_activos AS (
    SELECT DISTINCT
        RIGHT('00000000' + RTRIM(LTRIM(bel.CLCCLI)), 8) AS padded_codigo_cliente
    FROM DW_BEL_IBCLIE bel
    WHERE bel.CLSTAT = 'A'
),
usuarios_bel_activos AS (
    SELECT DISTINCT
        RIGHT('00000000' + RTRIM(LTRIM(u.CLCCLI)), 8) AS padded_codigo_cliente
    FROM DW_BEL_IBUSER u
    WHERE u.USSTAT = 'A'
),
cuentas_digitales_activas AS (
    SELECT DISTINCT
        RIGHT('00000000' + RTRIM(LTRIM(dep.CLDOC)), 8) AS padded_codigo_cliente
    FROM DW_DEP_DEPOSITOS dep
    WHERE dep.DW_PRODUCTO LIKE '%DIGITAL%'
      AND dep.PRCODP = 1
      AND dep.PRSUBP = 51
      AND dep.CTSTA = 'A'
),
clientes_elegibles AS (
    SELECT
        cif.codigo_cliente,
        cif.padded_codigo_cliente,
        cif.nombre_cliente,
        cif.tipo_cliente,
        cif.clase_cliente,
        cif.estado_cliente_cif,
        cif.banca_cnt,
        cif.anio_nac
    FROM clientes_cif cif
    INNER JOIN clientes_bel_activos cba
        ON cif.padded_codigo_cliente = cba.padded_codigo_cliente
    INNER JOIN usuarios_bel_activos uba
        ON cif.padded_codigo_cliente = uba.padded_codigo_cliente
    INNER JOIN cuentas_digitales_activas cda
        ON cif.padded_codigo_cliente = cda.padded_codigo_cliente
),
pagos_journal AS (
    SELECT DISTINCT
        RIGHT('00000000' + RTRIM(LTRIM(j.CLCCLI)), 8) AS padded_codigo_cliente
    FROM DW_BEL_IBJOUR j
    INNER JOIN DW_BEL_IBSERV s
        ON j.SECODE = s.SECODE
       AND s.INSERV = 'APP'
       AND s.TISERV = 'O'
       AND s.SEUSPR = 'S'
       AND s.SECODE IN (
            'ap-pagclar','app-pagcla','ope-rccl','app-reccla',
            'app-ptigo','pag-tigo','app-rectig','ope-rctg',
            'app-paenee','pag-enee','app-asps','pag-asps',
            'app-achtrf','app-trach','app-transf','app-transh','app-transt',
            'app-tcpago','app-pagotc','app-paptmo','pago-ptmos','ope-psarah'
       )
    WHERE j.DW_FECHA_JOURNAL >= @fecha_inicio
      AND j.DW_FECHA_JOURNAL < @fecha_fin_exclusiva
      AND j.JOSTAT = 1
      AND j.JOSECU = 1
      AND j.JOVALO > 0
),
pagos_multipagos_raw AS (
    SELECT
        mp.SPINUS,
        mp.SPPAVA,
        mp.SPCODC,
        mp.DW_FECHA_OPERACION_SP,
        mp.SPPAFR
    FROM DW_MUL_SPPADAT mp
    WHERE mp.DW_FECHA_OPERACION_SP >= @fecha_inicio
      AND mp.DW_FECHA_OPERACION_SP < @fecha_fin_exclusiva
      AND mp.SPPAFR = 'N'
      AND ISNULL(mp.SPPAVA, 0) > 0
      AND mp.SPCODC IN (
            '866','882','130','143','184','227','237','238','309','368','371','446',
            '459','478','507','512','526','571','574','643','680','687','734','755',
            '885','888','481','907','693','572','573','732','498','524','513','868',
            '869','408','784'
      )
),
pagos_multipagos AS (
    SELECT DISTINCT
        RIGHT('00000000' + RTRIM(LTRIM(
            CASE
                WHEN PATINDEX('%[A-Za-z]%', RTRIM(LTRIM(r.SPINUS))) > 1
                    THEN LEFT(RTRIM(LTRIM(r.SPINUS)), PATINDEX('%[A-Za-z]%', RTRIM(LTRIM(r.SPINUS))) - 1)
                WHEN PATINDEX('%[A-Za-z]%', RTRIM(LTRIM(r.SPINUS))) = 0
                    THEN RTRIM(LTRIM(r.SPINUS))
                ELSE NULL
            END
        )), 8) AS padded_codigo_cliente
    FROM pagos_multipagos_raw r
    WHERE r.SPINUS IS NOT NULL
),
clientes_con_pago AS (
    SELECT padded_codigo_cliente
    FROM pagos_journal
    WHERE padded_codigo_cliente IS NOT NULL AND padded_codigo_cliente <> ''
    UNION
    SELECT padded_codigo_cliente
    FROM pagos_multipagos
    WHERE padded_codigo_cliente IS NOT NULL AND padded_codigo_cliente <> ''
),
clientes_sin_pago AS (
    SELECT ce.*
    FROM clientes_elegibles ce
    LEFT JOIN clientes_con_pago cp
        ON ce.padded_codigo_cliente = cp.padded_codigo_cliente
    WHERE cp.padded_codigo_cliente IS NULL
),
contacto AS (
    SELECT
        csp.codigo_cliente,
        csp.padded_codigo_cliente,
        csp.nombre_cliente,
        csp.tipo_cliente,
        csp.estado_cliente_cif,
        csp.banca_cnt,
        csp.anio_nac,
        CASE
            WHEN sms.telefono_sms IS NOT NULL THEN sms.telefono_sms
            WHEN COALESCE(LEN(tel.CLTEL1), 0) = 8 AND SUBSTRING(tel.CLTEL1, 1, 1) IN ('3','8','9') THEN tel.CLTEL1
            WHEN COALESCE(LEN(tel.CLTEL2), 0) = 8 AND SUBSTRING(tel.CLTEL2, 1, 1) IN ('3','8','9') THEN tel.CLTEL2
            ELSE NULL
        END AS numero_telefono,
        sms.dw_nombre_operador AS nombre_operador,
        LOWER(RTRIM(LTRIM(cor.CLDIRE))) AS correo,
        ROW_NUMBER() OVER (
            PARTITION BY csp.padded_codigo_cliente
            ORDER BY sms.telefono_sms, tel.CLTEL1, tel.CLTEL2, cor.CLDIRE
        ) AS rn
    FROM clientes_sin_pago csp
    LEFT JOIN DWHBI.dbo.dw_sms_perfil_usuario sms
        ON csp.padded_codigo_cliente = RTRIM(LTRIM(sms.CIF))
       AND sms.dw_descripcion_status = 'ACTIVO'
       AND sms.dw_nombre_operador IN ('Claro H', 'Hondutel', 'Tigo H')
       AND LEN(sms.telefono_sms) = 8
       AND SUBSTRING(sms.telefono_sms, 1, 1) IN ('3','8','9')
    LEFT JOIN DW_CIF_DIRECCIONES_PRINCIPAL tel
        ON csp.codigo_cliente = RTRIM(LTRIM(tel.CLDOC))
    LEFT JOIN DW_CIF_DIRECCIONES cor
        ON csp.codigo_cliente = RTRIM(LTRIM(cor.CLDOC))
       AND cor.CLDICO = 4
       AND cor.CLDIRE LIKE '%_@_%.__%'
)
SELECT
    base.codigo_cliente,
    base.padded_codigo_cliente,
    base.nombre_cliente,
    base.tipo_cliente,
    base.estado_cliente_cif,
    base.banca_cnt,
    base.anio_nac,
    base.numero_telefono,
    base.nombre_operador,
    base.correo
FROM contacto base
WHERE base.rn = 1
  AND NOT (base.numero_telefono IS NULL AND base.correo IS NULL)
ORDER BY base.padded_codigo_cliente;

