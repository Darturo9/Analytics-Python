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
│   └── trx_usuarios_2025_2026.sql
└── exports/
```

## Ejecucion

```bash
python3 productos/creacion_usuario_sv/german/programas_py/reporte_genero_generacion_2025_2026.py
```

### Transacciones cohorte 2025-2026

```bash
# Modo resumen (general y por anio)
python3 productos/creacion_usuario_sv/german/programas_py/reporte_trx_usuarios_2025_2026.py --modo resumen

# Modo detalle mensual (imprime resumen mensual y exporta un CSV por mes en exports/)
python3 productos/creacion_usuario_sv/german/programas_py/reporte_trx_usuarios_2025_2026.py --modo detalle
```

## Fuente

- BD: `DWHSV`
- Periodo: desde `2025-01-01` hasta `< 2027-01-01`
- Sin filtro por campañas RTM
