import jaydebeapi
import jpype
import openpyxl
from openpyxl import Workbook
from dotenv import load_dotenv
import os
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime, timedelta
import time
import textwrap

# Configurar logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("logs/z_cuestionario_NRS2002_ayer.log"),
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
recipients = os.getenv('RECIPIENT_EMAILS_NRS2002_AYER').split(',')  # Lista de destinatarios principales
cc_recipients = os.getenv('CC_EMAIL_NRS2002_AYER').split(',') if os.getenv('CC_EMAIL_NRS2002_AYER') else []  # Lista de destinatarios en copia

# Variables de fechas
fecha_hoy = datetime.now().strftime('%Y-%m-%d')
fecha_menos_1 = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
fecha_menos_1_nor = (datetime.now() - timedelta(days=1)).strftime('%d-%m-%Y')

FECHA_EJECUCION = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
LOG_FILE = "z_reporte_semanal_oficina_ges/logs/pipeline.log"

def enviar_correo(archivo, fecha_menos_1, fecha_hoy, intentos=5, espera=10):
    for intento in range(1, intentos + 1):
        try:
            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = ", ".join(recipients)  # Destinatarios principales
            msg['Cc'] = ", ".join(cc_recipients)  # Destinatarios en copia
            msg['Subject'] = f"Reporte de Cuestionario NRS2002: Fecha {fecha_menos_1_nor} Hospital del salvador"

            cuerpo = textwrap.dedent(f"""
                Muy buenos días, se adjunta registro de pacientes por Reporte de Cuestionario NRS2002 en TrakCare generado automáticamente por el sistema.
                Fecha Ejecución: {FECHA_EJECUCION}

                Saludos,
                Automatización NRS2002
            """).strip()

            # Adjuntar archivo
            with open(archivo, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(archivo)}"')
                msg.attach(MIMEText(cuerpo, "plain"))
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
            c.PAADM_ADMNo AS "Número de episodio",
            CASE 
                WHEN c.PAADM_VISITSTATUS = 'A' THEN 'Actual'
                WHEN c.PAADM_VISITSTATUS = 'C' THEN 'Suspendido'
                WHEN c.PAADM_VISITSTATUS = 'D' THEN 'Egreso'
                WHEN c.PAADM_VISITSTATUS = 'P' THEN 'Pre Admisión'
                WHEN c.PAADM_VISITSTATUS = 'R' THEN 'Liberado'
                WHEN c.PAADM_VISITSTATUS = 'N' THEN 'No Atendido'
                ELSE NULL
            END AS "Estado de episodio",
            PAADM_PAPMI_DR->PAPMI_ID AS "RUT paciente",
            PAADM_ADMDATE AS "Fecha admisión",
            PAADM_ADMTIME AS "Hora admisión",
            PAADM_DischgDate AS "Fecha alta adm",
            PAADM_DischgTime AS "Hora alta adm",
            QUESCreateDate AS "Fecha creación",
            QUESCreateTime AS "Hora creación",
            QUESScore AS "Puntaje NRS2002",
            b.SSUSR_Name AS "Usuario que realiza NRS2002",
            COALESCE(
                (
                    SELECT W.WARD_Desc
                    FROM PA_AdmTransaction T
                    LEFT JOIN PAC_Ward W
                        ON T.TRANS_Ward_DR = W.WARD_RowID
                    WHERE T.TRANS_ParRef = c.PAADM_RowID
                    AND T.TRANS_RowID = (
                            SELECT MAX(T2.TRANS_RowID)
                            FROM PA_AdmTransaction T2
                            WHERE T2.TRANS_ParRef = c.PAADM_RowID
                    )
                ),
                c.PAADM_DepCode_DR->CTLOC_Desc
            ) AS "Servicio",
            PAADM_CurrentRoom_DR->Room_Desc AS "Sala",
            PAADM_CurrentBed_DR->Bed_Code AS "Cama",
            c.PAADM_DepCode_DR->CTLOC_Desc AS "Unidad responsable"
        FROM questionnaire.QTCNRS2002 a
        INNER JOIN SS_User b ON a.QUESCreateUserDR = b.SSUSR_RowId
        LEFT JOIN PA_Adm c ON a.QUESPAAdmDR = c.PAADM_RowID
        WHERE QUESCreateDate = CURRENT_DATE - 1
        AND PAADM_TYPE = 'I'
        AND SSUSR_Hospital_DR->HOSP_Code = 112100;
    '''

    # Ejecutar consulta
    cursor_iris = conn_iris.cursor()
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # Crear archivo Excel
    archivo_excel = "z_cuestionario/reportes/reporte_cuestionario_ayer.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "Reporte"

    # Escribir encabezados
    encabezados = [
                "Número de episodio","Estado de episodio","RUT paciente",
                "Fecha admisión","Hora admisión","Fecha alta adm","Hora alta adm",
                "Fecha creación","Hora creación","Puntaje NRS2002","Usuario que realiza NRS2002,"
                "Servicio","Sala","Cama","Unidad responsable"]
    ws.append(encabezados)

    # Escribir datos
    for row in rows:
        ws.append([str(cell) if cell is not None else "" for cell in row])

    # Guardar archivo
    wb.save(archivo_excel)
    logging.info(f"Archivo Excel generado: {archivo_excel}")

    # Enviar archivo por correo con reintentos
    enviar_correo(archivo_excel, fecha_menos_1, fecha_hoy)

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