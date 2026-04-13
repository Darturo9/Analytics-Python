# LoginUsuarios / QBR_1_2026

Dashboard para medir logins de clientes durante el primer trimestre de 2026.

## Universo
- Clientes de `DW_CENTROBI_TABLA_CIF69`.
- Logins entre `2026-01-01` y `2026-03-31`.

## Archivos
- `queries/base.sql`: universo de clientes.
- `queries/Logins.sql`: eventos de login Q1 2026.
- `dashboards/dashboard_qbr1_2026.py`: dashboard principal.

## Ejecucion
```bash
python3 productos/LoginUsuarios/QBR_1_2026/dashboards/dashboard_qbr1_2026.py
```
