import os
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from dotenv import load_dotenv
from datetime import datetime, timedelta

# ========================
# CARGA VARIABLES ENTORNO
# ========================
load_dotenv()

EMAIL_USER = os.getenv("SMTP_USER")
EMAIL_PASS = os.getenv("SMTP_PASSWORD")
EMAIL_TO = os.getenv("RECIPIENT_EMAILS_OFICINAGES")
EMAIL_ALERT = os.getenv("RECIPIENT_EMAILS_OFICINAGES_ERROR", EMAIL_USER)
EMAIL_HOST = os.getenv("SMTP_HOST")
EMAIL_PORT = int(os.getenv("SMTP_PORT"))

# ========================
# CONFIGURACIÓN ARCHIVO REPORTE
# ========================
ARCHIVO_REPORTE = "z_reporte_semanal_oficina_ges/resultados/reporte_semanal_oficina_ges.xlsx"
FECHA_REPORTE = datetime.now().strftime("%Y-%m-%d")
NOMBRE_ADJUNTO = f"reporte_semanal_oficina_ges_{FECHA_REPORTE}.xlsx"

# ========================
# CONFIGURACIÓN LOG
# ========================
LOG_FILE_PATH = "z_reporte_semanal_oficina_ges/logs/reporte_semanal_oficina_ges.log"
MAX_LOG_LINES = 200  # cuántas líneas mandar en el correo de error (últimas)


def leer_ultimas_lineas_log(path_log, max_lineas=200):
    """
    Devuelve las últimas `max_lineas` líneas del archivo de log como string.
    Si el archivo no existe o hay problema leyéndolo, devuelve un mensajito.
    """
    if not os.path.exists(path_log):
        return f"(No se encontró el archivo de log en {path_log})"

    try:
        with open(path_log, "r", encoding="utf-8", errors="replace") as f:
            lineas = f.readlines()
        # nos quedamos con el final
        tail = lineas[-max_lineas:]
        # limpiamos trailing spaces
        tail = [l.rstrip("\n") for l in tail]
        return "\n".join(tail) if tail else "(Log vacío)"
    except Exception as e:
        return f"(No se pudo leer el log: {e})"


# ========================
# FUNCIÓN DE NOTIFICACIÓN DE FALLO
# ========================
def notificar_fallo(error_mensaje):
    try:
        print(f"Enviando alerta de fallo a {EMAIL_ALERT}...")

        # Preparamos snippet de log
        log_excerpt = leer_ultimas_lineas_log(LOG_FILE_PATH, MAX_LOG_LINES)

        msg = MIMEMultipart()
        msg["From"] = EMAIL_USER
        msg["To"] = EMAIL_ALERT
        msg["Subject"] = f"ERROR en envío de Reporte Oficina GES - {FECHA_REPORTE}"

        cuerpo = f"""Hola,
            Se produjo un problema al enviar el Reporte Oficina GES .

            Fecha del intento: {FECHA_REPORTE}

            Detalle del error:
            {error_mensaje}

            Últimas líneas del log ({LOG_FILE_PATH}):
            ----------------------------------------
            {log_excerpt}
            ----------------------------------------

            Por favor revisar el servidor, el archivo generado y/o las credenciales SMTP.

            Saludos,
            Automatización Reporte Oficina GES
            """
        msg.attach(MIMEText(cuerpo, "plain"))

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.send_message(msg)

        print(f"Alerta enviada correctamente a {EMAIL_ALERT}")
    except Exception as e:
        print(f"No se pudo enviar la alerta de error: {e}")


# ========================
# FUNCIÓN PRINCIPAL DE ENVÍO
# ========================
def enviar_correo(archivo, intentos=5, espera=10):
    # Validar existencia del archivo ANTES de intentar enviar
    if not os.path.exists(archivo):
        msg_error = f"No se encontró el archivo para enviar: {archivo}"
        print(f"{msg_error}")
        notificar_fallo(msg_error)
        return False

    for intento in range(1, intentos + 1):
        try:
            print(f"Enviando correo... (intento {intento}/{intentos})")

            msg = MIMEMultipart()
            msg["From"] = EMAIL_USER
            msg["To"] = EMAIL_TO
            msg["Subject"] = f"Reporte Oficina GES - {FECHA_REPORTE}"

            cuerpo = f"""
            Estimados, se adjunta Reporte Oficina GES generado automáticamente por el sistema.
            Fecha de Reporte: {FECHA_REPORTE}
            Archivo: {NOMBRE_ADJUNTO}

            Saludos,
            Automatización Reporte Oficina GES
            """
            
            msg.attach(MIMEText(cuerpo, "plain"))

            # Adjuntar archivo con nombre con fecha
            with open(archivo, "rb") as f:
                part = MIMEApplication(f.read(), Name=NOMBRE_ADJUNTO)
                part["Content-Disposition"] = f'attachment; filename="{NOMBRE_ADJUNTO}"'
                msg.attach(part)

            # Enviar
            with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                server.starttls()
                server.login(EMAIL_USER, EMAIL_PASS)
                server.send_message(msg)

            print(f"Correo enviado correctamente a {EMAIL_TO}")
            return True

        except Exception as e:
            print(f"Error al enviar el correo (intento {intento}): {e}")
            if intento < intentos:
                print(f"Reintentando en {espera} segundos...")
                time.sleep(espera)
            else:
                print("Todos los intentos de envío fallaron.")
                notificar_fallo(str(e))
                return False
            
# ========================
# EJECUCIÓN
# ========================
if __name__ == "__main__":
    enviar_correo(ARCHIVO_REPORTE)
