"""
colors.py
---------
Paleta de colores oficial Banpaís.
Importar en cualquier dashboard para mantener consistencia visual.

Uso:
    from core.colors import COLORES, PALETA
"""

COLORES = {
    "azul_experto":     "#003865",   # Azul oscuro principal
    "aqua_digital":     "#00C1D4",   # Cyan / turquesa
    "amarillo_opt":     "#FFB81C",   # Amarillo optimista
    "amarillo_emp":     "#FDD26E",   # Amarillo empático (suave)
    "azul_financiero":  "#2D8C9E",   # Azul teal
    "blanco":           "#FFFFFF",
    "gris_fondo":       "#FFFFFF",   # Fondo general dashboards
    "gris_texto":       "#666666",
}

# Lista ordenada para gráficos con múltiples series
PALETA = [
    "#00C1D4",  # aqua digital
    "#FFB81C",  # amarillo optimista
    "#003865",  # azul experto
    "#2D8C9E",  # azul financiero
    "#FDD26E",  # amarillo empático
]
