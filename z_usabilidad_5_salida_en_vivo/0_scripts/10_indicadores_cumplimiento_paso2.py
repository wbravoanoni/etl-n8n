import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

fecha_actualizacion = datetime.now()


# ======================================================
# 1. RUTAS
# ======================================================
BASE_DIR = Path(__file__).resolve().parent.parent
RESULTADOS_DIR = BASE_DIR / "3_resultados"

RUTA_BASE   = RESULTADOS_DIR / "10_indicadores_cumplimiento_paso1.xlsx"
RUTA_EVO    = RESULTADOS_DIR / "6_evoluciones_pro.xlsx"
RUTA_SALIDA = RESULTADOS_DIR / "10_indicadores_cumplimiento_paso2.xlsx"

# ======================================================
# 2. CARGA
# ======================================================
base = pd.read_excel(RUTA_BASE)
evo  = pd.read_excel(RUTA_EVO)

base.columns = base.columns.str.lower()
evo.columns  = evo.columns.str.lower()

# ======================================================
# 3. DATETIME BASE (SERVICIO)
# ======================================================
base["inicio_servicio_dt"] = pd.to_datetime(
    base["fecha_inicio_servicio"].astype(str) + " " +
    base["hora_inicio_servicio"].astype(str),
    errors="coerce"
)

base["termino_servicio_dt"] = pd.to_datetime(
    base["fecha_termino_servicio"].astype(str) + " " +
    base["hora_termino_servicio"].astype(str),
    errors="coerce"
)

# ======================================================
# 4. DATETIME EVOLUCIONES (FORMATO MIXTO)
# ======================================================
evo["fecha_creacion_dt"] = pd.to_datetime(
    evo["fecha_creacion"].astype(str) + " " +
    evo["hora_creacion"].astype(str),
    dayfirst=True,
    format="mixed",
    errors="coerce"
)

# ======================================================
# 5. NORMALIZAR WARD
# ======================================================
base["ward_locationdr"] = base["ward_locationdr"].astype(str)
evo["ward_locationdr"]  = evo["ward_locationdr"].astype(str)

# ======================================================
# 6. LEFT JOIN BASE ↔ EVOLUCIONES
# ======================================================
cruce = base.merge(
    evo,
    on=["episodio", "ward_locationdr"],
    how="left",
    suffixes=("", "_evo")
)

# ======================================================
# 7. FILTRAR EVOLUCIONES EN RANGO
# ======================================================
cruce = cruce[
    (cruce["fecha_creacion_dt"] >= cruce["inicio_servicio_dt"]) &
    (cruce["fecha_creacion_dt"] <= cruce["termino_servicio_dt"])
].copy()

# ======================================================
# 8. AGREGAR EVOLUCIONES
# ======================================================
resultado = (
    cruce
    .groupby(
        [
            "episodio",
            "servicio",
            "fecha_inicio_servicio",
            "hora_inicio_servicio",
            "fecha_termino_servicio",
            "hora_termino_servicio",
            "tipo_profesional",
        ]
    )
    .agg(
        cantidad_evoluciones=("fecha_creacion_dt", "count"),
        dias_con_evo=("fecha_creacion_dt", lambda x: x.dt.date.nunique())
    )
    .reset_index()
)

# ======================================================
# 8.1 RENOMBRAR COLUMNA
# ======================================================
resultado = resultado.rename(columns={
    "tipo_profesional": "estamento"
})

# ======================================================
# 9. CÁLCULO DIAS_TOTALES_EN_EL_SERVICIO
# ======================================================
resultado["inicio_servicio_dt"] = pd.to_datetime(
    resultado["fecha_inicio_servicio"].astype(str) + " " +
    resultado["hora_inicio_servicio"].astype(str),
    errors="coerce"
)

resultado["termino_servicio_dt"] = pd.to_datetime(
    resultado["fecha_termino_servicio"].astype(str) + " " +
    resultado["hora_termino_servicio"].astype(str),
    errors="coerce"
)

resultado["minutos_totales_en_el_servicio"] = (
    resultado["termino_servicio_dt"] - resultado["inicio_servicio_dt"]
).dt.total_seconds() / 60

resultado["dias_totales_en_el_servicio"] = np.ceil(
    resultado["minutos_totales_en_el_servicio"] / 1440
).astype("Int64")

# ======================================================
# 10. DIAS SIN EVOLUCIÓN
# ======================================================
resultado["dias_sin_evo"] = (
    resultado["dias_totales_en_el_servicio"] - resultado["dias_con_evo"]
).clip(lower=0)

# ======================================================
# 11. LIMPIEZA Y ORDEN FINAL
# ======================================================
resultado = resultado.drop(
    columns=[
        "inicio_servicio_dt",
        "termino_servicio_dt",
        "minutos_totales_en_el_servicio",
    ]
)

resultado["fecha_actualizacion"] = fecha_actualizacion

resultado = resultado[
    [
        "episodio",
        "servicio",
        "estamento",
        "fecha_inicio_servicio",
        "hora_inicio_servicio",
        "fecha_termino_servicio",
        "hora_termino_servicio",
        "dias_totales_en_el_servicio",
        "cantidad_evoluciones",
        "dias_con_evo",
        "dias_sin_evo",
        "fecha_actualizacion",
    ]
]

# ======================================================
# 12. EXPORTAR
# ======================================================

resultado.to_excel(RUTA_SALIDA, index=False)

print("PASO 2 + métricas finales generado correctamente")
print(f"Archivo: {RUTA_SALIDA}")
print(f"Filas: {len(resultado)}")
