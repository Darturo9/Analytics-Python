# Superpack Claro

Este modulo contiene una query para:

1. Obtener clientes unicos que compraron Superpack Claro en abril 2026.
2. Validar una lista de clientes para saber cuantos compraron.

## Archivos principales

- `queries/superpack_claro_abril_2026.sql`
- `queries/clientes_contactados_rtm_claro_abril_2026.sql`
- `validar_superpack_claro.py`
- `resumen_superpack_mensual.py`
- `unificar_clientes_contactados.py`

## Logica usada

- Fuente transaccional: `dw_mul_sppadat`
- Catalogo de codigos: `dw_mul_spmaco`
- Criterio Superpack Claro: `spcodc = 498`
- Periodo: abril 2026 (`2026-04-01` a `2026-04-30`)
- Se excluyen reversos con `sppafr = 'N'`

## Uso rapido (SQL)

1. Ejecuta la query tal cual para generar el universo de compradores.
2. En el bloque `#clientes_lista_raw`, pega tu lista de clientes.
3. Re-ejecuta para ver detalle y resumen de cuantos compraron.

## Uso rapido (Python para listas grandes)

Para archivos grandes (200K+), usa el script Python.

1. Coloca los archivos en:

- `productos/Superpack Claro/inputs/clientes Contactados promo Claro.xlsx`
- `productos/Superpack Claro/inputs/Clientes Contactados RTM.xlsx`

2. Ejecuta sin parametros:

```bash
python3 "productos/Superpack Claro/validar_superpack_claro.py"
```

Salida por defecto:

- `productos/Superpack Claro/exports/clientes_que_compraron_superpack_abril_2026.xlsx`
- `productos/Superpack Claro/exports/clientes_que_compraron_superpack_abril_2026_rtm.xlsx`

Hojas de salida:

- `compradores_superpack`

En consola tambien imprime una tabla diaria con:

- fecha
- clientes unicos compradores
- total de transacciones del dia
- monto mas comun y su frecuencia

Opciones utiles:

- `--input "/ruta/a/tu_lista_clientes.xlsx"` para ejecutar un solo analisis personalizado.
- `--sheet "NombreHoja"` para indicar hoja de Excel.
- `--cliente-column "codigo_cliente"` para fijar columna de cliente.
- `--no-export` para correr solo validacion y ver resumen en consola, sin generar archivo.

## Resumen mensual en consola (enero-abril)

Imprime por mes:

- clientes unicos compradores
- total de transacciones
- suma de montos de transacciones
- monto promedio
- monto mas comun

Uso por defecto (enero a abril 2026, codigo 498):

```bash
python3 "productos/Superpack Claro/resumen_superpack_mensual.py"
```

Con parametros:

```bash
python3 "productos/Superpack Claro/resumen_superpack_mensual.py" --anio 2026 --mes-inicio 1 --mes-fin 4 --codigo-superpack 498
```

## Unificar clientes contactados (RTM prioridad)

Este script une 2 archivos de `inputs`:

- `Clientes Contactados RTM.xlsx`
- `clientes Contactados promo Claro.xlsx`

Reglas:

- agrega columna `origen` con `RTM` o `PAUTA`
- elimina duplicados por cliente
- si un cliente esta en ambos archivos, se conserva con `origen = RTM`

Ejecucion:

```bash
python3 "productos/Superpack Claro/unificar_clientes_contactados.py"
```

Salida:

- `productos/Superpack Claro/exports/clientes_contactados_unificados_prioridad_rtm.xlsx`

## Abril 2026 (validacion por canal)

Carpeta:

- `productos/Superpack Claro/abril 2026`

Archivos:

- `queries/compras_superpack_abril_2026.sql` (solo compras Superpack abril 2026)
- `validar_superpack_abril_canales.py`

Este flujo toma el archivo unificado:

- `productos/Superpack Claro/exports/clientes_contactados_unificados_prioridad_rtm.xlsx`

Y muestra en consola:

- cuantos clientes de la lista compraron superpack
- desglose por canal `RTM` y `PAUTA`

Ejecucion:

```bash
python3 "productos/Superpack Claro/abril 2026/validar_superpack_abril_canales.py"
```

Salida detalle:

- `productos/Superpack Claro/abril 2026/exports/validacion_superpack_abril_canales.xlsx`
