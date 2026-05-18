# App Empresarial - Reporte Quincena

Reporte quincenal configurable desde codigo para comparar actividad de App Empresarial.

## Estructura

```text
reporte_quincena/
├── queries/
│   ├── query1_quincena.sql
│   └── clientes_rtm_quincena.sql
└── programas_py/
    ├── reporte_quincena_app_empresarial.py
    └── reporte_quincena_app_empresarial_saldos.py
```

## Reglas aplicadas en el script

- Base actividad: `query1_quincena.sql`.
- Universo clientes RTM: `clientes_rtm_quincena.sql`.
- Cruce aplicado para el resultado final:
  - `padded_codigo_cliente` igual.
  - `fecha_q1 >= fecha_q2`.
- Clasificacion de modulo:
  - `Login`, `Consulta`, `Transacción`, `Gestiones`, `Gestiones CRM`.

## Configuracion (desde el .py)

Editar en:

`productos/app_empresarial/reporte_quincena/programas_py/reporte_quincena_app_empresarial.py`

- `CONFIG_ANIO`
- `CONFIG_MES`
- `CONFIG_DIA_INICIO`
- `CONFIG_DIA_FIN`
- `CONFIG_RTM_FECHA_INICIO`
- `CONFIG_EXPORTAR_CLIENTES` (True/False)

Para el reporte de saldos:

`productos/app_empresarial/reporte_quincena/programas_py/reporte_quincena_app_empresarial_saldos.py`

- mismas variables anteriores
- `CONFIG_CHUNK_SIZE_CLIENTES`
- `CONFIG_TOP_DEPTOS`

## Ejecucion

```bash
python3 productos/app_empresarial/reporte_quincena/programas_py/reporte_quincena_app_empresarial.py
```

```bash
python3 productos/app_empresarial/reporte_quincena/programas_py/reporte_quincena_app_empresarial_saldos.py
```

## Salida

- Resumen de volumen en rango (query1).
- Resumen de volumen con match contra RTM (cliente + fecha).
- Tabla de modulos antes y despues del match.
- Export opcional de clientes unicos del resultado final.

Salida adicional del script `reporte_quincena_app_empresarial_saldos.py`:
- Perfil financiero de clientes del match (saldo al cierre, saldo promedio y clientes con saldo).
- Top departamentos (depto) por cantidad de clientes del match.
