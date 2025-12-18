import subprocess
import sys
import os

BASE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE) 

def fail_and_exit(msg):
    print(f"[ERROR] {msg}")
    sys.exit(1)

scripts = [
    "z_usabilidad_dialisis_evoluciones.py",
    "z_usabilidad_dialisis_imagenes.py"
]

if __name__ == "__main__":
    try:
        for script in scripts:
            script_path = os.path.join(BASE, script)
            print(f"\n=== Ejecutando: {script} ===")

            result = subprocess.run(
                [sys.executable, script_path],
                cwd=PROJECT_ROOT
            )

            if result.returncode != 0:
                fail_and_exit(f"El script {script} terminó con error (exit code {result.returncode}).")

        print("\nTodos los scripts finalizados correctamente.")
        sys.exit(0)

    except Exception as e:
        fail_and_exit(f"Error inesperado al ejecutar scripts: {e}")
