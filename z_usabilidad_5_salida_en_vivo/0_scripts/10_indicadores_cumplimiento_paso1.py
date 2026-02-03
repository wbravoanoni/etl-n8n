import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# ======================================================
# 1. RUTAS
# ======================================================
BASE_DIR = Path(__file__).resolve().parent.parent
RESULTADOS_DIR = BASE_DIR / "3_resultados"

RUTA_ENTRADA = RESULTADOS_DIR / "9_hospitalizados_dias_paso2_reglas_clinicas.xlsx"
RUTA_SALIDA  = RESULTADOS_DIR / "10_indicadores_cumplimiento_paso1.xlsx"

# ======================================================
# 2. CARGA
# ======================================================
df = pd.read_excel(RUTA_ENTRADA)
df.columns = df.columns.str.lower()

# ======================================================
# 3. FECHA Y HORA REAL DE EJECUCIÓN
# ======================================================
now = datetime.now()
fecha_ejecucion = now.date()
hora_ejecucion  = now.strftime("%H:%M:%S")

# ======================================================
# 4. CIERRE DE HOSPITALIZACIONES ABIERTAS
# ======================================================
df["fecha_termino_servicio"] = df["fecha_termino_servicio"].fillna(fecha_ejecucion)
df["hora_termino_servicio"]  = df["hora_termino_servicio"].fillna(hora_ejecucion)

# ======================================================
# 5. CREAR DATETIME INICIO / TÉRMINO
# ======================================================
df["inicio_servicio"] = pd.to_datetime(
    df["fecha_inicio_servicio"].astype(str) + " " +
    df["hora_inicio_servicio"].astype(str),
    errors="coerce"
)

df["termino_servicio"] = pd.to_datetime(
    df["fecha_termino_servicio"].astype(str) + " " +
    df["hora_termino_servicio"].astype(str),
    errors="coerce"
)

# ======================================================
# 6. ELIMINAR REGISTROS SIN FECHAS/HORAS VÁLIDAS
# ======================================================
df = df[
    df["inicio_servicio"].notna() &
    df["termino_servicio"].notna()
].copy()

# ======================================================
# 7. ELIMINAR REGISTROS SIN ESTADÍA REAL (0 MINUTOS)
# ======================================================
df = df[df["inicio_servicio"] != df["termino_servicio"]].copy()

# ======================================================
# 8. RECÁLCULO ESTADÍA POR SERVICIO
# ======================================================
df["minutos_estadia_servicio"] = (
    df["termino_servicio"] - df["inicio_servicio"]
).dt.total_seconds() / 60

# Eliminar minutos negativos o cero (errores de registro)
df = df[df["minutos_estadia_servicio"] > 0].copy()

df["dias_estadia_servicio"] = np.ceil(
    df["minutos_estadia_servicio"] / 1440
).astype("Int64")

# ======================================================
# 9. RECÁLCULO ESTADÍA POR EPISODIO
# ======================================================
df["minutos_estadia_episodio"] = (
    df.groupby("episodio")["minutos_estadia_servicio"]
      .transform("sum")
)

df["dias_estadia_episodio"] = np.ceil(
    df["minutos_estadia_episodio"] / 1440
).astype("Int64")

# ======================================================
# 10. LIMPIEZA COLUMNAS AUXILIARES
# ======================================================
df.drop(columns=["inicio_servicio", "termino_servicio"], inplace=True)

# ======================================================
# 11. EXPORTAR RESULTADO FINAL
# ======================================================
df.to_excel(RUTA_SALIDA, index=False)

print("✔ Archivo generado correctamente")
print(f"✔ Ruta: {RUTA_SALIDA}")
print(f"✔ Fecha/hora de ejecución usada como término: {fecha_ejecucion} {hora_ejecucion}")
print(f"✔ Registros finales: {len(df)}")
