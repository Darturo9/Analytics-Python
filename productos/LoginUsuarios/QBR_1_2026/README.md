# LoginUsuarios / QBR_1_2026

Dashboard para medir logins de clientes durante el primer trimestre de 2026.

## Universo
- Clientes desde `ArchivosExcel/Contactados_Enero_2026` (`.xlsx`, `.xls` o `.csv`).
- Logins entre `2026-01-01` y `2026-03-31`.

## Archivos
- `queries/Logins.sql`: eventos de login Q1 2026.
- `queries/Logins_Marzo.sql`: eventos de login de marzo 2026.
- `queries/sin_login/post_login_q1_2026.sql`: eventos operativos generales Q1 2026 para análisis post-login, filtrados en SQL por los clientes del Excel.
- `dashboards/dashboard_qbr1_2026.py`: dashboard principal.
- `dashboards/dashboard_qbr1_arbol_rtm.py`: dashboard para base `Arbol RTM`.
- `dashboards/dashboard_qbr1_post_login.py`: dashboard de comportamiento post-login Q1 usando `queries/sin_login/post_login_q1_2026.sql`.
- `ArchivosExcel/Contactados_Enero_2026.*`: base de clientes a evaluar.
- `ArchivosExcel/ArbolRTM.*`: base alternativa para evaluación RTM.

## Ejecucion
```bash
python3 productos/LoginUsuarios/QBR_1_2026/dashboards/dashboard_qbr1_2026.py
```
