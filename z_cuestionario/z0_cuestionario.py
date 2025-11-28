import subprocess
import sys
import os

BASE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE)

scripts = [
    "z_cuestionario_braden.py",
    "z_cuestionario_cudyr_riesgo_dependencia.py",
    "z_cuestionario_cudyr_salud_mental.py",
    "z_cuestionario_downtown.py"
]

if __name__ == "__main__":
    for script in scripts:
        script_path = os.path.join(BASE, script)
        print(f"\n=== Ejecutando: {script} ===")
        subprocess.run([sys.executable, script_path], cwd=PROJECT_ROOT)
