import subprocess
import sys
import time
import os
from datetime import datetime

# =====================================================
# CONFIGURACIÓN GENERAL
# =====================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LOG_FILE = "z_usabilidad_5_salida_en_vivo/logs/9_hospitalizados_pipeline.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

FECHA_EJECUCION = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# =====================================================
# SCRIPTS A EJECUTAR (EN ORDEN)
# =====================================================
scripts = [
    os.path.join(BASE_DIR, "9_hospitalizados_dias_paso1_descarga.py"),
    os.path.join(BASE_DIR, "9_hospitalizados_dias_paso2_reglas_clinicas.py"),
    os.path.join(BASE_DIR, "9_hospitalizados_dias_paso3_consolidar.py"),
    os.path.join(BASE_DIR, "9_hospitalizados_dias_paso4_resumen_fechas.py"),
    os.path.join(BASE_DIR, "9_hospitalizados_dias_paso5_ajuste_comparacion.py"),
]

# =====================================================
# FUNCIÓN EJECUCIÓN
# =====================================================
def ejecutar_script(script_path):
    print(f"\n Ejecutando: {os.path.basename(script_path)}")
    inicio = datetime.now()

    try:
        resultado = subprocess.run(
            [sys.executable, script_path],
            check=True,
            text=True,
            capture_output=True
        )

        duracion = (datetime.now() - inicio).total_seconds()

        print(f"✔ Finalizado en {duracion:.2f} segundos")
        if resultado.stdout:
            print("— STDOUT —")
            print(resultado.stdout)

        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(
                f"[{FECHA_EJECUCION}] OK - {script_path} "
                f"({duracion:.2f}s)\n"
            )

        return True

    except subprocess.CalledProcessError as e:
        print(" ERROR en ejecución")
        print("— STDERR —")
        print(e.stderr)

        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(
                f"[{FECHA_EJECUCION}] ERROR - {script_path}\n"
                f"{e.stderr}\n"
            )

        return False

    except Exception as ex:
        print("✖ ERROR inesperado")
        print(str(ex))

        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(
                f"[{FECHA_EJECUCION}] ERROR INESPERADO - {script_path}\n"
                f"{str(ex)}\n"
            )

        return False


# =====================================================
# EJECUCIÓN PIPELINE
# =====================================================
print("==============================================")
print(" PIPELINE HOSPITALIZADOS – INICIO")
print(f" FECHA: {FECHA_EJECUCION}")
print("==============================================")

with open(LOG_FILE, "a", encoding="utf-8") as log:
    log.write(f"\n=== EJECUCIÓN {FECHA_EJECUCION} ===\n")

for script in scripts:
    ok = ejecutar_script(script)
    if not ok:
        print(f"\n Pipeline detenido por error en:")
        print(script)
        break

    time.sleep(2)

print("\n==============================================")
print(" PIPELINE FINALIZADO")
print("==============================================")
