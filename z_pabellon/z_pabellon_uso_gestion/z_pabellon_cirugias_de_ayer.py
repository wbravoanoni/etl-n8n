# ============================================================
# ETL Cirugías del día – Cron 23:00
# Fecha clínica (IRIS) + Fecha ejecución real (Chile)
# ============================================================

import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from pathlib import Path
import pandas as pd

# ============================================================
# CARGA DE ENTORNO Y LOGS
# ============================================================

load_dotenv(override=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/z_pabellon_cirugias.log"),
        logging.StreamHandler()
    ]
)

# ============================================================
# VARIABLES DE ENTORNO
# ============================================================

# IRIS
jdbc_driver_name = os.getenv("JDBC_DRIVER_NAME")
jdbc_driver_loc = os.getenv("JDBC_DRIVER_PATH")
iris_connection_string = os.getenv("CONEXION_STRING")
iris_user = os.getenv("DB_USER")
iris_password = os.getenv("DB_PASSWORD")

# MySQL
mysql_host = os.getenv("DB_MYSQL_HOST")
mysql_port = int(os.getenv("DB_MYSQL_PORT", 3306))
mysql_user = os.getenv("DB_MYSQL_USER")
mysql_password = os.getenv("DB_MYSQL_PASSWORD")
mysql_database = os.getenv("DB_MYSQL_DATABASE")

# Email
EMAIL_USER  = os.getenv("SMTP_USER")
EMAIL_PASS  = os.getenv("SMTP_PASSWORD")
EMAIL_TO    = os.getenv("RECIPIENT_EMAILS_REGISTROPABELLON")
EMAIL_ALERT = os.getenv("RECIPIENT_EMAILS_REGISTROPABELLON_ERROR")
EMAIL_HOST  = os.getenv("SMTP_HOST")
EMAIL_PORT  = int(os.getenv("SMTP_PORT"))

# ============================================================
# FECHAS (BLINDADAS)
# ============================================================

fecha_ejecucion_real = datetime.now(ZoneInfo("America/Santiago"))
fecha_ejecucion_excel = fecha_ejecucion_real.replace(tzinfo=None)

logging.info(f"Fecha ejecución real (Chile): {fecha_ejecucion_real.isoformat()}")

# ============================================================
# FUNCIÓN: ENVÍO DE CORREO
# ============================================================

def enviar_correo(asunto, cuerpo, es_error=False, adjunto_path=None):
    destinatarios = EMAIL_ALERT if es_error else EMAIL_TO
    if not destinatarios:
        logging.warning("No hay destinatarios configurados")
        return

    msg = MIMEMultipart()
    msg["Subject"] = asunto
    msg["From"] = EMAIL_USER
    msg["To"] = destinatarios
    msg.attach(MIMEText(cuerpo, "plain", "utf-8"))

    if adjunto_path and Path(adjunto_path).exists():
        with open(adjunto_path, "rb") as f:
            adj = MIMEApplication(
                f.read(),
                _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            adj.add_header(
                "Content-Disposition",
                "attachment",
                filename=Path(adjunto_path).name
            )
            msg.attach(adj)

    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(
            EMAIL_USER,
            [d.strip() for d in destinatarios.split(",")],
            msg.as_string()
        )

    logging.info("Correo enviado correctamente")

# ============================================================
# FUNCIÓN: CREAR TABLA MYSQL
# ============================================================

def crear_tabla_si_no_existe(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS z_pabellon_cirugias_de_ayer (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            IDTRAK VARCHAR(255),
            Establecimiento VARCHAR(255),
            Episodio VARCHAR(255),
            Tipo_Episodio VARCHAR(255),
            Numero_Registro VARCHAR(255),
            Apellido_Paterno VARCHAR(255),
            Apellido_Materno VARCHAR(255),
            Nombre VARCHAR(255),
            Paciente VARCHAR(255),
            RUT VARCHAR(255),
            Codigo_Cirugia VARCHAR(255),
            Descripcion_Cirugia VARCHAR(255),
            Tipo_Cirugia VARCHAR(255),
            Diagnostico VARCHAR(255),
            Tiempo_Estimado VARCHAR(255),
            Lista_Espera VARCHAR(255),
            Suspension VARCHAR(255),
            Area VARCHAR(255),
            Pabellon VARCHAR(255),
            Fecha VARCHAR(255),
            Hora VARCHAR(255),
            Cirujano VARCHAR(255),
            Especialidad VARCHAR(255),
            Cirugia_Ambulatoria VARCHAR(255),
            Cirugia_Condicional VARCHAR(255),
            GES VARCHAR(255),
            Estado VARCHAR(255),
            fechaActualizacion DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
    """)

# ============================================================
# FUNCIÓN: GENERAR EXCEL
# ============================================================

def generar_excel(datos, columnas, fecha):
    if not datos:
        return None
    df = pd.DataFrame(datos, columns=columnas)
    ruta = Path("logs") / f"cirugias_pabellon_{fecha}.xlsx"
    df.to_excel(ruta, index=False, engine="openpyxl")
    return ruta

# ============================================================
# MAIN
# ============================================================

conn_iris = None
conn_mysql = None
ruta_excel = None

try:
    # JVM
    if not jpype.isJVMStarted():
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            "-Djava.class.path=" + jdbc_driver_loc
        )

    # IRIS
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {"user": iris_user, "password": iris_password},
        jdbc_driver_loc
    )
    cursor_iris = conn_iris.cursor()

    query = """
        SELECT
            String(RB_OperatingRoom.RBOP_RowId),
            RBOP_PAADM_DR->PAADM_Hospital_DR->HOSP_Desc,
            RBOP_PAADM_DR->PAADM_ADMNo,
            CASE RBOP_PAADM_DR->PAADM_Type
                WHEN 'I' THEN 'Hospitalizado'
                WHEN 'O' THEN 'Ambulatorio'
                WHEN 'E' THEN 'Urgencia'
            END,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_No,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name3,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name2,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name2 || ' ' ||
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name3 || ' ' ||
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_ID,
            RBOP_Operation_DR->OPER_Code,
            RBOP_Operation_DR->OPER_Desc,
            CASE RBOP_BookingType
                WHEN 'EL'  THEN 'Cirugía Electiva'
                WHEN 'ENP' THEN 'Cirugía Electiva No Programada'
                WHEN 'EM'  THEN 'Cirugía Urgencia'
            END,
            RBOP_PreopDiagn_DR->MRCID_Desc,
            RBOP_EstimatedTime,
            RBOP_WaitLIST_DR->WL_NO,
            RBOP_ReasonSuspend_DR->SUSP_Desc,
            RBOP_Resource_DR->RES_CTLOC_DR->CTLOC_Desc,
            RBOP_Resource_DR->RES_Desc,
            CONVERT(VARCHAR, RBOP_DateOper, 105),
            %EXTERNAL(RBOP_TimeOper),
            RBOP_Surgeon_DR->CTPCP_Desc,
            RBOP_OperDepartment_DR->CTLOC_Desc,
            RBOP_DaySurgery,
            RBOP_PreopTestDone,
            RBOP_YesNo3,
            CASE RBOP_Status
                WHEN 'B' THEN 'AGENDADO'
                WHEN 'N' THEN 'NO LISTO'
                WHEN 'P' THEN 'POSTERGADO'
                WHEN 'D' THEN 'REALIZADO'
                WHEN 'A' THEN 'RECEPCIONADO'
                WHEN 'R' THEN 'SOLICITADO'
                WHEN 'X' THEN 'SUSPENDIDO'
                WHEN 'DP' THEN 'SALIDA'
                WHEN 'CL' THEN 'CERRADO'
            END
        FROM RB_OperatingRoom
        WHERE
            RBOP_PAADM_DR->PAADM_Hospital_DR = '10448'
            AND RBOP_DateOper >= CURRENT_DATE
            AND RBOP_DateOper < DATEADD('day', 1, CURRENT_DATE)
        ORDER BY
            RBOP_Resource_DR->RES_CTLOC_DR->CTLOC_Desc,
            RBOP_Resource_DR->RES_Desc;
    """

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    formatted_rows = [
        tuple(map(str, row)) + (fecha_ejecucion_excel,)
        for row in rows
    ]

    columnas_excel = [
        "IDTRAK","Establecimiento","Episodio","Tipo_Episodio","Numero_Registro",
        "Apellido_Paterno","Apellido_Materno","Nombre","Paciente","RUT",
        "Codigo_Cirugia","Descripcion_Cirugia","Tipo_Cirugia","Diagnostico",
        "Tiempo_Estimado","Lista_Espera","Suspension","Area","Pabellon",
        "Fecha","Hora","Cirujano","Especialidad",
        "Cirugia_Ambulatoria","Cirugia_Condicional","GES",
        "Estado","fechaActualizacion"
    ]

    ruta_excel = generar_excel(formatted_rows, columnas_excel, fecha_ejecucion_real.date())

    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    crear_tabla_si_no_existe(cursor_mysql)

    insert_sql = """
        INSERT INTO z_pabellon_cirugias_de_ayer (
            IDTRAK, Establecimiento, Episodio, Tipo_Episodio, Numero_Registro,
            Apellido_Paterno, Apellido_Materno, Nombre, Paciente, RUT,
            Codigo_Cirugia, Descripcion_Cirugia, Tipo_Cirugia, Diagnostico,
            Tiempo_Estimado, Lista_Espera, Suspension, Area, Pabellon,
            Fecha, Hora, Cirujano, Especialidad,
            Cirugia_Ambulatoria, Cirugia_Condicional, GES,
            Estado, fechaActualizacion
        ) VALUES (
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,
            %s,%s
        )
    """

    cursor_mysql.executemany(insert_sql, formatted_rows)
    conn_mysql.commit()

    enviar_correo(
        "ETL Pabellón – Proceso OK",
        f"""Proceso ejecutado correctamente.

Fecha ejecución real : {fecha_ejecucion_real}
Registros cargados   : {len(formatted_rows)}
""",
        es_error=False,
        adjunto_path=ruta_excel
    )

except Exception as e:
    logging.error(str(e), exc_info=True)
    enviar_correo("ETL Pabellón – ERROR", str(e), es_error=True, adjunto_path=ruta_excel)

finally:
    if conn_iris:
        conn_iris.close()
    if conn_mysql:
        conn_mysql.close()
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
