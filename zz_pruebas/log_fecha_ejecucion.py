#!/usr/bin/env python3
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

# Carpeta del script
BASE_DIR = Path(__file__).resolve().parent
LOG = BASE_DIR / "logs/log_fecha_ejecucion.log"
STATE = BASE_DIR / "logs/log_fecha_ejecucion.state"

tz = ZoneInfo("America/Santiago")

# MISMA lógica que el ETL
fecha_ejecucion_real = datetime.now(tz)
fecha_sin_tz = fecha_ejecucion_real.replace(tzinfo=None)
epoch_actual = fecha_ejecucion_real.timestamp()

# Leer epoch anterior si existe
epoch_anterior = None
if STATE.exists():
    try:
        epoch_anterior = float(STATE.read_text().strip())
    except Exception:
        epoch_anterior = None

# Calcular delta
delta = None
if epoch_anterior is not None:
    delta = epoch_actual - epoch_anterior

# Guardar epoch actual para la próxima ejecución
STATE.write_text(str(epoch_actual))

# Escribir log
with LOG.open("a") as f:
    f.write("--------------------------------------------------\n")
    f.write(f"Fecha ejecucion real (tz Chile): {fecha_ejecucion_real.isoformat()}\n")
    f.write(f"Fecha sin tz (como BD)        : {fecha_sin_tz}\n")
    f.write(f"Epoch actual                 : {epoch_actual}\n")

    if delta is not None:
        f.write(f"Delta epoch (segundos)       : {delta}\n")

        # Alertas visuales
        if delta < 0:
            f.write("⚠️ ALERTA: el tiempo RETROCEDIÓ\n")
        elif delta > 600:
            f.write("⚠️ ALERTA: salto grande de tiempo\n")
    else:
        f.write("Delta epoch                  : N/A (primera ejecución)\n")

    f.write("--------------------------------------------------\n")
