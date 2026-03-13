# Analytics Python

Repositorio central de análisis de datos. Organizado por producto y mes.

## Estructura

```
Analytics-Python/
├── core/                  # Módulos compartidos (conexión DB, utilidades)
├── productos/             # Todo el trabajo de análisis
│   ├── app_empresarial/
│   ├── cuenta_digital/
│   └── creacion_usuario_sv/
├── data/                  # Datos temporales (NO se sube a git)
├── .env                   # Credenciales (NO se sube a git)
├── .env.example           # Plantilla de credenciales
└── requirements.txt       # Dependencias Python
```

## Configuración inicial

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Crear archivo de credenciales
cp .env.example .env
# Editar .env con tus credenciales de SQL Server
```

## Módulos core

| Archivo        | Qué hace                                      |
|----------------|-----------------------------------------------|
| `core/db.py`   | Conexión a SQL Server, ejecutar queries       |
| `core/utils.py`| Exportar Excel/CSV, formatear CIF             |
| `core/config.py`| Carga variables de entorno (.env)            |

## Uso rápido

```python
from core.db import run_query, run_query_file
from core.utils import exportar_excel, formatear_cif

# Ejecutar una query
df = run_query("SELECT TOP 10 * FROM DW_CIF_CLIENTES")

# Ejecutar desde archivo .sql
df = run_query_file("productos/cuenta_digital/2026-03/queries/clientes.sql")

# Exportar a Excel
exportar_excel(df, "productos/cuenta_digital/2026-03/exports/clientes.xlsx")

# Formatear CIF a 8 dígitos
df = formatear_cif(df, "CIF_CLIENTE")
```

## Productos

- **app_empresarial** — Análisis de la app empresarial (transacciones, campañas, conversiones)
- **cuenta_digital** — Apertura y seguimiento de cuentas digitales
- **creacion_usuario_sv** — Creación de usuarios SV
