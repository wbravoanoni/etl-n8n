import subprocess
import sys
import time
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

EMAIL_USER = os.getenv("SMTP_USER")
EMAIL_PASS = os.getenv("SMTP_PASSWORD")
EMAIL_TO = os.getenv("RECIPIENT_EMAILS_TEAMCODER")
EMAIL_ALERT = os.getenv("RECIPIENT_EMAILS_TEAMCODER_ERROR")
EMAIL_HOST = os.getenv("SMTP_HOST")
EMAIL_PORT = int(os.getenv("SMTP_PORT"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 

scripts = [
    os.path.join(BASE_DIR, "z1_teamcoder_descargaReporte.py"),
    os.path.join(BASE_DIR, "z2_teamcoder_homologacion.py"),
    os.path.join(BASE_DIR, "z3_teamcoder_enviarCorreo.py"),
]

FECHA_EJECUCION = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
LOG_FILE = "z_teamcoder/logs/pipeline.log"

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def enviar_alerta(script_fallido, error_mensaje):
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_USER
        msg["To"] = EMAIL_ALERT
        msg["Subject"] = f" Error en pipeline Teamcoder - {script_fallido}"

        cuerpo = f"""
                    Hola,

                    Se produjo un error durante la ejecución del pipeline Teamcoder.

                    Fecha: {FECHA_EJECUCION}
                    Script con error: {script_fallido}
                    Detalle del error:
                    {error_mensaje}

                    Por favor revisa el servidor o los logs en:
                    {LOG_FILE}

                    Saludos,  
                    Automatización Teamcoder
                """
        
        msg.attach(MIMEText(cuerpo, "plain"))

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)

        print(f"Correo de alerta enviado a {EMAIL_ALERT}")
    except Exception as e:
        print(f"No se pudo enviar correo de alerta: {e}")

def ejecutar_script(nombre_script):
    print(f"\nEjecutando: {nombre_script}")
    inicio = datetime.now()

    try:
        resultado = subprocess.run(
            [sys.executable, nombre_script],
            check=True,
            text=True,
            capture_output=True
        )
        duracion = (datetime.now() - inicio).total_seconds()
        print(f" {nombre_script} finalizado en {duracion:.2f} segundos.")
        print(f" Salida:\n{resultado.stdout}")

        # Escribir log
        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(f"\n[{FECHA_EJECUCION}] {nombre_script} OK ({duracion:.2f}s)\n")
        return True

    except subprocess.CalledProcessError as e:
        error_msg = f"Error en {nombre_script}:\n{e.stderr}"
        print(f" {error_msg} ")
        enviar_alerta(nombre_script, error_msg)

        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(f"\n[{FECHA_EJECUCION}] ERROR en {nombre_script}\n{e.stderr}\n")
        return False

    except Exception as ex:
        error_msg = f"Error inesperado en {nombre_script}: {ex}"
        print(f" {error_msg}")
        enviar_alerta(nombre_script, error_msg)

        with open(LOG_FILE, "a", encoding="utf-8") as log:
            log.write(f"\n[{FECHA_EJECUCION}] ERROR inesperado en {nombre_script}\n{ex}\n")
        return False


print("======================================")
print(" Iniciando pipeline Teamcoder")
print("======================================")

with open(LOG_FILE, "a", encoding="utf-8") as log:
    log.write(f"\n\n=== EJECUCIÓN {FECHA_EJECUCION} ===\n")

for script in scripts:
    exito = ejecutar_script(script)
    if not exito:
        print(f"Pipeline detenido por fallo en {script}")
        break
    time.sleep(2) 

print("\n Pipeline finalizado.")
