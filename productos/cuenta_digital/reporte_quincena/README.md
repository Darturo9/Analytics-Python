# Reporte Quincena - Cuenta Digital

Este módulo permite calcular el conteo de cuentas digitales creadas en un rango de días dentro de un mes.

Objetivo principal:
- Obtener el conteo del `1 al 15` de cada mes.
- Reutilizar el mismo script para mayo, junio y meses siguientes, cambiando solo parámetros.

## Estructura

```text
reporte_quincena/
├── queries/
│   └── conteo_cuentas_creadas_quincena.sql
├── analysis/
│   └── reporte_cuentas_creadas_quincena.py
└── exports/
    └── .gitkeep
```

## Uso rápido

Desde la raíz del repo:

```bash
python3 productos/cuenta_digital/reporte_quincena/analysis/reporte_cuentas_creadas_quincena.py --anio 2026 --mes 5
```

Esto toma por defecto `--dia-inicio 1` y `--dia-fin 15`.

## Ejemplos

Mayo 2026 (1 al 15):

```bash
python3 productos/cuenta_digital/reporte_quincena/analysis/reporte_cuentas_creadas_quincena.py --anio 2026 --mes 5
```

Junio 2026 (1 al 15):

```bash
python3 productos/cuenta_digital/reporte_quincena/analysis/reporte_cuentas_creadas_quincena.py --anio 2026 --mes 6
```

Rango personalizado dentro del mes:

```bash
python3 productos/cuenta_digital/reporte_quincena/analysis/reporte_cuentas_creadas_quincena.py --anio 2026 --mes 6 --dia-inicio 1 --dia-fin 20
```

## Salida

El script imprime:
- Periodo evaluado.
- Conteo diario de cuentas creadas.
- Conteo total del periodo.
