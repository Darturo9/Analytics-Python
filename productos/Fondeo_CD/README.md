# Fondeo_CD

Análisis de fondeo para Cuenta Digital, enfocado en cohortes de apertura por período.

## Estructura

- `Queries/`
  - `FondeoResumenCuentas.sql`: 1 fila por cuenta creada en el período.
  - `FondeoDiaro.sql`: histórico diario + acumulado de fondeo del período.
- `Analysis/`
  - `export_fondeo_cd.py`: ejecuta queries y exporta Excel multi-hoja.
- `Dashboards/`
  - `dashboard_fondeo_cd.py`: dashboard ejecutivo en Dash.
- `exports/`
  - Salidas Excel (no en git).

## Ejemplos

```bash
# Exportar para marzo 2026
python3 "productos/Fondeo_CD/Analysis/export_fondeo_cd.py" --fecha-inicio 2026-03-01 --fecha-fin 2026-03-31

# Levantar dashboard para marzo 2026
python3 "productos/Fondeo_CD/Dashboards/dashboard_fondeo_cd.py" --fecha-inicio 2026-03-01 --fecha-fin 2026-03-31
```
