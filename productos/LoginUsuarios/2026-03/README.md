# LoginUsuarios / 2026-03

## Dashboard

Ejecutar desde la raíz del proyecto:

```bash
python3 productos/LoginUsuarios/2026-03/dashboards/dashboard.py
```

## Archivo de Contactados

El dashboard busca automáticamente un archivo en:

- `productos/LoginUsuarios/2026-03/archivoExcel/Contactados.xlsx`
- o `productos/LoginUsuarios/2026-03/archivoExcel/Contactados.csv`

Columnas esperadas:

1. Código cliente (ejemplo: `codigo_cliente`, `codigo_cliente_usuario`, `cldoc`)
2. Canal de contacto (opcional, ejemplo: `canal_contacto` o `canal`)

## Lógica del dashboard

- Solo muestra logins de clientes que estén en el archivo `Contactados`.
- El `Canal` del Excel se usa como segmentación de contacto (por ejemplo: `Pauta` o `RTM`).
- El canal de contacto del Excel se analiza por separado del canal de login (`app-login`, `web-login`, etc.).
