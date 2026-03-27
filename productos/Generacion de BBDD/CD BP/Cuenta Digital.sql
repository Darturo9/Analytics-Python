WITH clientes_excluidos_campania AS (
    -- Consulta en DWHBI: Clientes a excluir por campañas previas
    SELECT DISTINCT
        RIGHT('00000000' + LEFT(RTRIM(LTRIM(his.codigo_cliente)), 
        LEN(RTRIM(LTRIM(his.codigo_cliente))) - 1), 8) AS padded_codigo_cliente
    FROM DWHBI.dbo.DW_RTM_APP_CAMPAIGN cam
    INNER JOIN DWHBI.dbo.DW_RTM_APP_HISCAMPAIGNUNIVERSO his ON (his.CampaignID = cam.CampaignID)
    WHERE cam.Start_date >= '2026-01-01'
     -- AND cam.Name LIKE '%CD HN - 72049 (B)%'
    --  AND cam.Description LIKE '%REG SQVD%'
    --  AND cam.Description LIKE '%BPA015%'
		AND cam.Name LIKE '%CD HN%'
	    AND cam.Name LIKE '%Inicial%'
),
clientes_filtrados AS (
    -- Universo base en DWHBP
    SELECT 
        RTRIM(LTRIM(clientes.cldoc)) AS codigo_cliente,
        RIGHT('00000000' + RTRIM(LTRIM(clientes.cldoc)), 8) AS padded_codigo_cliente
    FROM dw_cif_clientes clientes
    WHERE clientes.clclco != 10        
      AND clientes.estatu = 'A'         
      AND clientes.cltipe = 'N'         
),
usuarios_activos AS (
    SELECT * FROM (
        SELECT 
            cf.codigo_cliente,
            cf.padded_codigo_cliente,
            ROW_NUMBER() OVER(PARTITION BY RTRIM(LTRIM(usuarios.CLCCLI)) ORDER BY usuarios.dw_fecha_creacion DESC) AS cont
        FROM dw_bel_ibuser usuarios
        INNER JOIN clientes_filtrados cf ON RTRIM(LTRIM(cf.codigo_cliente)) = RTRIM(LTRIM(usuarios.CLCCLI))
        WHERE usuarios.usstat = 'A'
    ) AS rn
    WHERE rn.cont = 1
),
set_bases AS (
    SELECT DISTINCT 
        base.codigo_cliente,
        base.padded_codigo_cliente
    FROM usuarios_activos base
    WHERE base.codigo_cliente NOT IN (
        -- Exclusión de quienes ya tienen Cuenta Digital
        SELECT RTRIM(LTRIM(cldoc))
        FROM DW_DEP_DEPOSITOS 
        WHERE DW_PRODUCTO LIKE '%DIGITAL%'
          AND PRCODP = 1
          AND PRSUBP = 51
    )
    AND base.padded_codigo_cliente NOT IN (
        -- Exclusión de campaña externa (DWHBI)
        SELECT padded_codigo_cliente FROM clientes_excluidos_campania
    )
)

-- Generación de Base Final
SELECT
    base.cif,
    base.nombre_completo,
    base.numero_celular,
    base.correo,
    base.segmentacion_generacional
FROM (
    SELECT
        sb.codigo_cliente AS cif,
        clientes.clnomb AS nombre_completo,
        CASE
            WHEN YEAR(clientes.DW_FECHA_NACIMIENTO) BETWEEN 1965 AND 1980 THEN 'Generation X (1965-1980)'
            WHEN YEAR(clientes.DW_FECHA_NACIMIENTO) BETWEEN 1981 AND 1996 THEN 'Gen Y - Millennials (1981-1996)'
            WHEN YEAR(clientes.DW_FECHA_NACIMIENTO) BETWEEN 1997 AND 2012 THEN 'Generación Z (1997-2012)'
            ELSE 'OTRA GENERACION'
        END AS segmentacion_generacional,
        CASE
            WHEN sms.telefono_sms IS NOT NULL THEN sms.telefono_sms
            WHEN COALESCE(LEN(telefonos.cltel1), 0) = 8 AND SUBSTRING(telefonos.cltel1, 1, 1) IN ('3', '8', '9') THEN telefonos.cltel1
            WHEN COALESCE(LEN(telefonos.cltel2), 0) = 8 AND SUBSTRING(telefonos.cltel2, 1, 1) IN ('3', '8', '9') THEN telefonos.cltel2
        END AS numero_celular,
        LOWER(RTRIM(LTRIM(correos.cldire))) AS correo,
        ROW_NUMBER() OVER (PARTITION BY sb.codigo_cliente ORDER BY sms.telefono_sms, telefonos.cltel1, correos.cldire) AS rn
    FROM set_bases sb
    INNER JOIN dw_cif_clientes clientes ON RTRIM(LTRIM(sb.codigo_cliente)) = RTRIM(LTRIM(clientes.cldoc))
    LEFT JOIN DWHBI.dbo.dw_sms_perfil_usuario sms ON sb.padded_codigo_cliente = RTRIM(LTRIM(sms.cif))
        AND sms.dw_descripcion_status = 'ACTIVO'
        AND sms.dw_nombre_operador IN ('Claro H', 'Hondutel', 'Tigo H')
        AND LEN(sms.telefono_sms) = 8
    LEFT JOIN dw_cif_direcciones_principal telefonos ON sb.codigo_cliente = RTRIM(LTRIM(telefonos.cldoc))
    LEFT JOIN dw_cif_direcciones correos ON sb.codigo_cliente = RTRIM(LTRIM(correos.cldoc))
        AND correos.cldico = 4 
) AS base
WHERE base.rn = 1
  -- REQUISITOS DE CONTACTABILIDAD OBLIGATORIOS
  AND base.numero_celular IS NOT NULL -- Teléfono obligatorio
  AND base.correo IS NOT NULL         -- Correo obligatorio
  -- Validaciones de formato de correo
  AND base.correo LIKE '%@%.com' 
  AND LEN(base.correo) - LEN(REPLACE(base.correo, '@', '')) = 1 
  AND base.correo NOT LIKE '%www.%'
  AND CHARINDEX(' ', base.correo) = 0