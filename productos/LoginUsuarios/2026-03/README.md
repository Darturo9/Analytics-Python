# LoginUsuarios / 2026-03

## Dashboard

Ejecutar desde la raíz del proyecto:

```bash
python3 productos/LoginUsuarios/2026-03/dashboards/dashboard.py
```

## Archivo de Contactados

El dashboard busca automáticamente un archivo en:

- `productos/LoginUsuarios/2026-03/data/Contactados.xlsx`
- o `productos/LoginUsuarios/2026-03/data/Contactados.csv`

Columnas esperadas:

1. Código cliente (ejemplo: `codigo_cliente`, `codigo_cliente_usuario`, `cldoc`)
2. Canal de contacto (opcional, ejemplo: `canal_contacto` o `canal`)
