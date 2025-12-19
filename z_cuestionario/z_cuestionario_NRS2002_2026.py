import jaydebeapi
import jpype
import openpyxl
from openpyxl import Workbook
from dotenv import load_dotenv
import os
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta
import time

# Configurar logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("logs/z_cuestionario_NRS2002.log"),
                              logging.StreamHandler()])

# Cargar variables de entorno
load_dotenv(override=True)

# Variables de entorno
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# Configuración de correo
smtp_host = os.getenv('SMTP_HOST')
smtp_port = os.getenv('SMTP_PORT')
smtp_user = os.getenv('SMTP_USER')
smtp_password = os.getenv('SMTP_PASSWORD')

# Arrays para destinatarios y copias
recipients = os.getenv('RECIPIENT_EMAILS').split(',')  # Lista de destinatarios principales
cc_recipients = os.getenv('CC_EMAILS').split(',') if os.getenv('CC_EMAILS') else []  # Lista de destinatarios en copia

# Variables de fechas
fecha_hoy = datetime.now().strftime('%Y-%m-%d')
fecha_menos_7 = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

def enviar_correo(archivo, fecha_menos_7, fecha_hoy, intentos=5, espera=10):
    for intento in range(1, intentos + 1):
        try:
            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = ", ".join(recipients)  # Destinatarios principales
            msg['Cc'] = ", ".join(cc_recipients)  # Destinatarios en copia
            msg['Subject'] = f"Reporte de Cuestionario NRS2002: fecha {fecha_menos_7} al {fecha_hoy} Hospital del salvador"

            # Adjuntar archivo
            with open(archivo, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(archivo)}"')
                msg.attach(part)

            # Combinar destinatarios y copias
            to_addrs = recipients + cc_recipients

            # Enviar correo
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.sendmail(smtp_user, to_addrs, msg.as_string())

            logging.info("Correo enviado exitosamente.")
            return  # Salir de la función si el correo se envía correctamente

        except Exception as e:
            logging.error(f"Intento {intento} - Error al enviar el correo: {e}")
            if intento < intentos:
                time.sleep(espera)  # Esperar antes del siguiente intento
            else:
                logging.error("Todos los intentos de envío fallaron.")

try:
    # Validar variables de entorno
    if not jdbc_driver_name or not jdbc_driver_loc:
        raise ValueError("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
    if not iris_connection_string or not iris_user or not iris_password:
        raise ValueError("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")

    # Iniciar JVM si no está ya iniciada
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # Crear conexión con InterSystems IRIS
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )

    # Consulta SQL
    query = f''' 
         SELECT
 c.PAADM_ADMNo AS "episodio",
PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_NAME2  || ' ' ||
PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_NAME || ' ' ||
PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_NAME3 AS "paciente",
QUESCreateDate AS "fecha creacion",
QUESCreateTime AS "hora creacion",
b.SSUSR_Name AS "usuario creador",
Q27 as "Fecha Sesión",
Q28  as "Hora Sesión",
Q02 as "IMC_menor_20_5",
Q03 as "El paciente ha perdido peso en los últimos 3 meses",
Q04 as "El paciente ha disminuido su ingesta en la última semana",
Q05 as "El paciente está gravemente herido",
Q08 as "La respuesta fue SI a alguna de las preguntas anteriores",
Q10 as "Estado Nutricional",
Q11 as "Severidad de la Enfermedad",
Q12 as "Edad",
QUESScore AS "puntaje",
CASE
    WHEN QUESScore >= 3 THEN
        'El paciente está en riesgo de malnutrición y es necesario iniciar soporte nutricional'
    WHEN QUESScore = 0 THEN
        ''
    WHEN QUESScore < 3 THEN
        'Es necesario reevaluar semanalmente. Si el paciente va a ser sometido a cirugía mayor, iniciar soporte nutricional perioperatorio'
END AS "descripcion puntaje"
FROM questionnaire.QTCNRS2002 AS a
INNER JOIN SS_User b ON a.QUESCreateUserDR = b.SSUSR_RowId
LEFT JOIN PA_Adm c ON a.QUESPAAdmDR = c.PAADM_RowID
WHERE QUESCreateDate BETWEEN '{fecha_menos_7}' AND '{fecha_hoy}'
AND PAADM_TYPE = 'I' AND SSUSR_Hospital_DR->HOSP_Code = 112100;
    '''

    # Ejecutar consulta
    cursor_iris = conn_iris.cursor()
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # Crear archivo Excel
    archivo_excel = "reporte_cuestionario.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "Reporte"

    # Escribir encabezados
    encabezados = [
        "episodio",
        "paciente",
        "fecha creacion",
        "hora creacion",
        "usuario creador",
        "Fecha Sesión",
        "Hora Sesión",
        "IMC_menor_20_5",
        "El paciente ha perdido peso en los últimos 3 meses",
        "El paciente ha disminuido su ingesta en la última semana",
        "El paciente está gravemente herido",
        "La respuesta fue SI a alguna de las preguntas anteriores",
        "Estado Nutricional",
        "Severidad de la Enfermedad",
        "Edad",
        "puntaje",
        "descripcion puntaje"
        ]
    ws.append(encabezados)

    # Escribir datos
    for row in rows:
        ws.append([str(cell) if cell is not None else "" for cell in row])

    # Guardar archivo
    wb.save(archivo_excel)
    logging.info(f"Archivo Excel generado: {archivo_excel}")

    # Enviar archivo por correo con reintentos
    enviar_correo(archivo_excel, fecha_menos_7, fecha_hoy)

except jaydebeapi.DatabaseError as e:
    logging.error(f"Error en InterSystems IRIS: {e}")
except Exception as e:
    logging.error(f"Error: {e}")
finally:
    if 'cursor_iris' in locals():
        cursor_iris.close()
    if 'conn_iris' in locals():
        conn_iris.close()
    if jpype.isJVMStarted():
        jpype.shutdownJVM()