# Reporte Quincena - Fondeo CD

Este módulo calcula el fondeo de cuentas de `CUENTA DIGITAL` creadas en una quincena,
permitiendo que el fondeo ocurra en **cualquier día del mes analizado**.

## Regla de negocio

- Universo base: cuentas creadas en el rango de quincena configurado.
- Fondeada: cuenta con `ctt001 > 0` al menos una vez dentro del mes analizado.

## Estructura

```text
reporte_quincena/
├── queries/
│   └── cuentas_creadas_vs_fondeadas_quincena.sql
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
