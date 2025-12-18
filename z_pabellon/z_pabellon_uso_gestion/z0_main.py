import subprocess
import sys
import os

# BASE ya es z_pabellon_uso_gestion
BASE = os.path.dirname(os.path.abspath(__file__))

def fail_and_exit(msg):
    print(f"[ERROR] {msg}")
    sys.exit(1)

scripts = [
    "z_pabellon_uso_gestion_pabellones_estado_agendamiento.py",
    "z_pabellon_uso_gestion_tiempo_transcurrido.py"
]

if __name__ == "__main__":
    try:
        for script in scripts:
            script_path = os.path.abspath(os.path.join(BASE, script))

            print(f"\n=== Ejecutando: {script} ===")
            print(f"Ruta: {script_path}")

            result = subprocess.run(
                [sys.executable, script_path],
                check=False
            )

            if result.returncode != 0:
                fail_and_exit(
                    f"El script {script} terminó con error (exit code {result.returncode})."
                )

        print("\nTodos los scripts finalizados correctamente.")
        sys.exit(0)

    except Exception as e:
        fail_and_exit(f"Error inesperado al ejecutar scripts: {e}")
