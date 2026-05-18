# Reporte Quincena - Fondeo CD

Este módulo calcula el fondeo de cuentas de `CUENTA DIGITAL` creadas en una quincena,
usando el **mismo rango de fechas** tanto para creación como para fondeo.

## Regla de negocio

- Universo base: cuentas creadas en el rango configurado.
- Fondeada: cuenta con `ctt001 > 0` al menos una vez dentro del mismo rango configurado.

## Estructura

```text
reporte_quincena/
├── dashboard/
│   └── dashboard_fondeo_quincena_web.py
├── queries/
│   └── cuentas_creadas_vs_fondeadas_quincena.sql
│   └── detalle_fondeo_quincena_dashboard.sql
└── analysis/
    └── reporte_fondeo_quincena_configurable.py
```

## Configuración (en el `.py`)

Editar en:

`productos/Fondeo_CD/reporte_quincena/analysis/reporte_fondeo_quincena_configurable.py`

- `CONFIG_ANIO`
- `CONFIG_MES`
- `CONFIG_DIA_INICIO`
- `CONFIG_DIA_FIN`

Ejemplo:
- Mayo 2026, quincena del 1 al 15:
  - `CONFIG_ANIO = 2026`
  - `CONFIG_MES = 5`
  - `CONFIG_DIA_INICIO = 1`
  - `CONFIG_DIA_FIN = 15`

## Ejecución

```bash
python3 productos/Fondeo_CD/reporte_quincena/analysis/reporte_fondeo_quincena_configurable.py
```

## Dashboard web

Incluye:
- KPIs de cuentas creadas/fondeadas/sin fondear/tasa.
- Genero de clientes fondeados.
- Generacion de clientes fondeados.
- Top 3 departamentos por monto maximo fondeado.
- Boton para descargar captura PNG del dashboard.

```bash
python3 productos/Fondeo_CD/reporte_quincena/dashboard/dashboard_fondeo_quincena_web.py
```
