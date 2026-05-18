# Creacion Usuario SV - Reporte Quincena

Reporte quincenal en consola para `creacion_usuario_sv`, con fechas editables en el código.

## Estructura

```text
reporte_quincena/
├── queries/
│   ├── comunicacionesRTM_quincena.sql
│   └── conversion_quincena.sql
└── programas_py/
    └── reporte_creacion_quincena_consola.py
```

## Reglas de joins respetadas

- `comunicacionesRTM_quincena.sql`:
  - `INNER JOIN` entre `dw_rtm_app_hiscampaignuniverso` y `dw_rtm_app_campaign`.
- `conversion_quincena.sql`:
  - `INNER JOIN` entre `dw_bel_ibuser`, `dw_bel_ibclie` y `DW_CIF_CLIENTES`.
  - `LEFT JOIN` con `dw_cif_direcciones_principal` para direcciones.

## Configuración (desde el .py)

Editar en:

`productos/creacion_usuario_sv/reporte_quincena/programas_py/reporte_creacion_quincena_consola.py`

- `CONFIG_ANIO`
- `CONFIG_MES`
- `CONFIG_DIA_INICIO`
- `CONFIG_DIA_FIN`
- `CONFIG_RTM_FECHA_INICIO` (histórico base para campañas)
- `CONFIG_DB_NAME` (por defecto `DWHSV`)

## Ejecución

```bash
python3 productos/creacion_usuario_sv/reporte_quincena/programas_py/reporte_creacion_quincena_consola.py
```
