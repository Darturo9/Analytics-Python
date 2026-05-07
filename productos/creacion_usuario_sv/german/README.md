# Reporte German - Creacion Usuario SV

Este bloque genera reportes de consola para el producto general (sin campañas)
usando base de usuarios creados en 2025 y 2026.

## Estructura

```text
german/
├── programas_py/
│   └── reporte_genero_generacion_2025_2026.py
├── queries/
│   └── base_usuarios_2025_2026.sql
└── exports/
```

Tambien incluye reporte de transacciones para esa misma cohorte:

```text
german/
├── programas_py/
│   ├── reporte_genero_generacion_2025_2026.py
│   └── reporte_trx_usuarios_2025_2026.py
├── queries/
│   ├── base_usuarios_2025_2026.sql
│   ├── trx_usuarios_2025_2026.sql
│   └── logins_usuarios_2025_2026.sql
└── exports/
```

## Ejecucion

```bash
python3 productos/creacion_usuario_sv/german/programas_py/reporte_genero_generacion_2025_2026.py
```

### Transacciones cohorte 2025-2026

```bash
# Modo anual (DEFAULT, general y por anio)
python3 productos/creacion_usuario_sv/german/programas_py/reporte_trx_usuarios_2025_2026.py
python3 productos/creacion_usuario_sv/german/programas_py/reporte_trx_usuarios_2025_2026.py --modo anual

# Modo mensual por evento (trx + logins, sin detalle cliente a cliente)
python3 productos/creacion_usuario_sv/german/programas_py/reporte_trx_usuarios_2025_2026.py --modo mensual
```

### Export Excel

- Modo anual:
  - `exports/resumen_anual_trx_logins_2025_2026.xlsx`
- Modo mensual:
  - `exports/resumen_mensual_trx_logins_2025_2026.xlsx`

## Fuente

- BD: `DWHSV`
- Periodo: desde `2025-01-01` hasta `< 2027-01-01`
- Sin filtro por campañas RTM
