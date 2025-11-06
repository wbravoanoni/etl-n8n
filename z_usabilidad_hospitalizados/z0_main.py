import subprocess
import sys
import os

BASE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE)

scripts = [
    "z_usabilidad_hospitalizados_epicrisis.py",
    "z_usabilidad_hospitalizados_evoluciones.py",
    "z_usabilidad_hospitalizados_ingresos.py"
]

if __name__ == "__main__":
    for script in scripts:
        script_path = os.path.join(BASE, script)
        print(f"\n=== Ejecutando: {script} ===")
        subprocess.run([sys.executable, script_path], cwd=PROJECT_ROOT)  # ✅ Corre en la carpeta donde está .env y el .jar
