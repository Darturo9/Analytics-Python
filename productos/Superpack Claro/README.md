# Superpack Claro

Este modulo contiene una query para:

1. Obtener clientes unicos que compraron Superpack Claro en abril 2026.
2. Validar una lista de clientes para saber cuantos compraron.

## Archivos principales

- `queries/superpack_claro_abril_2026.sql`
- `validar_superpack_claro.py`

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
