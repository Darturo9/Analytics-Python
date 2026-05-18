# Creacion Usuario SV - Reporte Quincena

Reportes quincenales con fechas editables en codigo para `creacion_usuario_sv`.

Incluye:
- Script de consola con creacion, medios, usuarios activos, logins, trx, genero y generaciones.
- Dashboard web con graficos exportables en PNG de alta calidad.

## Estructura

```text
reporte_quincena/
├── dashboard/
│   └── dashboard_creacion_quincena_web.py
├── queries/
│   ├── comunicacionesRTM_quincena.sql
│   ├── conversion_quincena.sql
│   ├── logins_quincena.sql
│   └── trx_quincena.sql
└── programas_py/
    └── reporte_creacion_quincena_consola.py
```

## Reglas de joins respetadas

- `comunicacionesRTM_quincena.sql`:
  - `INNER JOIN` entre `dw_rtm_app_hiscampaignuniverso` y `dw_rtm_app_campaign`.
- `conversion_quincena.sql`:
  - `INNER JOIN` entre `dw_bel_ibuser`, `dw_bel_ibclie` y `DW_CIF_CLIENTES`.
  - `LEFT JOIN` con `dw_cif_direcciones_principal` para direcciones.
- `logins_quincena.sql`:
  - `LEFT JOIN` de `dw_bel_IBSTTRA_VIEW` con `DW_CIF_CLIENTES`.
- `trx_quincena.sql`:
  - `LEFT JOIN` de `dw_BEL_IBJOUR` con `DW_CIF_CLIENTES` y `DW_BEL_IBSERV`.

## Configuracion (desde el .py)

### Consola

Editar en:

`productos/creacion_usuario_sv/reporte_quincena/programas_py/reporte_creacion_quincena_consola.py`

- `CONFIG_ANIO`
- `CONFIG_MES`
- `CONFIG_DIA_INICIO`
- `CONFIG_DIA_FIN`
- `CONFIG_RTM_FECHA_INICIO`
- `CONFIG_DB_NAME` (por defecto `DWHSV`)
- `CONFIG_VENTANA_CAMPANIA_MESES`

### Dashboard

Editar en:

`productos/creacion_usuario_sv/reporte_quincena/dashboard/dashboard_creacion_quincena_web.py`

- `CONFIG_ANIO`
- `CONFIG_MES`
- `CONFIG_DIA_INICIO`
- `CONFIG_DIA_FIN`
- `CONFIG_RTM_FECHA_INICIO`
- `CONFIG_VENTANA_CAMPANIA_MESES`
- `CONFIG_HOST`
- `CONFIG_PORT`

## Ejecucion

### Consola

```bash
python3 productos/creacion_usuario_sv/reporte_quincena/programas_py/reporte_creacion_quincena_consola.py
```

### Dashboard

```bash
python3 productos/creacion_usuario_sv/reporte_quincena/dashboard/dashboard_creacion_quincena_web.py
```
