import subprocess
import sys
import os

BASE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE)

scripts = [
    "z_usabilidad_monitoreo_enfermeria_braden_UPC_UHI.py",
    "z_usabilidad_monitoreo_enfermeria_downtown_UPC_UHI.py",
    "z_usabilidad_monitoreo_enfermeria_riesgo_dependencia_UPC_UHI.py",
    "z_usabilidad_monitoreo_enfermeria_kits_UPC_UHI.py",
    "z_usabilidad_monitoreo_enfermeria_examen_segmentario.py",
    "z_usabilidad_monitoreo_enfermeria_ucp_uhi.py"
]

if __name__ == "__main__":
    for script in scripts:
        script_path = os.path.join(BASE, script)
        print(f"\n=== Ejecutando: {script} ===")

        result = subprocess.run(
            [sys.executable, script_path],
            cwd=PROJECT_ROOT
        )

        if result.returncode != 0:
            print(f"❌ ERROR ejecutando {script}")
            print(f"Return code: {result.returncode}")
            sys.exit(1)

    sys.exit(0)
