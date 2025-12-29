# ============================================================
# Cirugías del día – Cron 23:00
# Fecha clínica (IRIS) + Fecha ejecución real (Sistema)
# ============================================================

import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
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
# FUNCIÓN: ENVÍO DE CORREO (con adjunto)
# ============================================================

def enviar_correo(asunto, cuerpo, es_error=False, adjunto_path=None):
    destinatarios = EMAIL_ALERT if es_error else EMAIL_TO

    if not destinatarios:
        logging.warning("No hay destinatarios configurados para correo")
        return

    try:
        msg = MIMEMultipart()
        msg["Subject"] = asunto
        msg["From"] = EMAIL_USER
        msg["To"] = destinatarios

        msg.attach(MIMEText(cuerpo, "plain", "utf-8"))

        if adjunto_path and Path(adjunto_path).exists():
            with open(adjunto_path, "rb") as f:
                adjunto = MIMEApplication(
                    f.read(),
                    _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                adjunto.add_header(
                    "Content-Disposition",
                    "attachment",
                    filename=Path(adjunto_path).name
                )
                msg.attach(adjunto)

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(
                EMAIL_USER,
                [d.strip() for d in destinatarios.split(",")],
                msg.as_string()
            )

        logging.info("Correo enviado correctamente")

    except Exception as e:
        logging.error(f"Error enviando correo: {e}", exc_info=True)

# ============================================================
# FUNCIÓN: CREAR TABLA
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
            Diagnostico VARCHAR(255),
            Fecha VARCHAR(255),
            Hora VARCHAR(255),
            Cirujano VARCHAR(255),
            Especialidad VARCHAR(255),
            Estado VARCHAR(255),
            fechaActualizacion DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
    """)

# ============================================================
# FUNCIÓN: GENERAR EXCEL
# ============================================================

def generar_excel_registros(datos, columnas, fecha_proceso):
    if not datos:
        return None

    df = pd.DataFrame(datos, columns=columnas)

    nombre_archivo = f"cirugias_pabellon_{fecha_proceso}.xlsx"
    ruta = Path("logs") / nombre_archivo

    df.to_excel(ruta, index=False, engine="openpyxl")
    return ruta

# ============================================================
# MAIN
# ============================================================

conn_iris = None
conn_mysql = None
ruta_excel = None
fecha_proceso = None

# Fecha real de ejecución (UNA sola vez)
fecha_ejecucion_real = datetime.now()

try:
    # --------------------------------------------------------
    # INICIAR JVM
    # --------------------------------------------------------
    if not jpype.isJVMStarted():
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            "-Djava.class.path=" + jdbc_driver_loc
        )

    # --------------------------------------------------------
    # CONEXIÓN IRIS
    # --------------------------------------------------------
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {"user": iris_user, "password": iris_password},
        jdbc_driver_loc
    )
    cursor_iris = conn_iris.cursor()

    # --------------------------------------------------------
    # QUERY IRIS (día clínico)
    # --------------------------------------------------------
    query = """
        SELECT
            String(RBOP_RowId),
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
            RBOP_PreopDiagn_DR->MRCID_Desc,
            CONVERT(VARCHAR, RBOP_DateOper, 105),
            %EXTERNAL(RBOP_TimeOper),
            RBOP_Surgeon_DR->CTPCP_Desc,
            RBOP_OperDepartment_DR->CTLOC_Desc,
            CASE RBOP_Status
                WHEN 'D' THEN 'REALIZADO'
                WHEN 'A' THEN 'RECEPCIONADO'
                WHEN 'B' THEN 'AGENDADO'
                WHEN 'X' THEN 'SUSPENDIDO'
                ELSE 'OTRO'
            END,
            CURRENT_DATE
        FROM RB_OperatingRoom
        WHERE
            RBOP_PAADM_DR->PAADM_Hospital_DR = '10448'
            AND RBOP_DateOper >= CURRENT_DATE
            AND RBOP_DateOper < DATEADD('day', 1, CURRENT_DATE)
    """

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # --------------------------------------------------------
    # FORMATEO
    # --------------------------------------------------------
    formatted_rows = []

    for row in rows:
        *datos, fecha_proceso_raw = row

        fecha_proceso = datetime.strptime(
            str(fecha_proceso_raw), "%Y-%m-%d"
        ).date()

        formatted_rows.append(
            tuple(map(str, datos)) + (fecha_ejecucion_real,)
        )

    # --------------------------------------------------------
    # GENERAR EXCEL
    # --------------------------------------------------------
    columnas_excel = [
        "IDTRAK", "Establecimiento", "Episodio", "Tipo_Episodio",
        "Numero_Registro", "Apellido_Paterno", "Apellido_Materno",
        "Nombre", "Paciente", "RUT",
        "Codigo_Cirugia", "Descripcion_Cirugia", "Diagnostico",
        "Fecha", "Hora", "Cirujano", "Especialidad", "Estado",
        "fechaActualizacion"
    ]

    ruta_excel = generar_excel_registros(
        formatted_rows,
        columnas_excel,
        fecha_proceso
    )

    # --------------------------------------------------------
    # MYSQL
    # --------------------------------------------------------
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    crear_tabla_si_no_existe(cursor_mysql)

    insert_query = """
        INSERT INTO z_pabellon_cirugias_de_ayer (
            IDTRAK, Establecimiento, Episodio, Tipo_Episodio, Numero_Registro,
            Apellido_Paterno, Apellido_Materno, Nombre, Paciente, RUT,
            Codigo_Cirugia, Descripcion_Cirugia, Diagnostico,
            Fecha, Hora, Cirujano, Especialidad, Estado, fechaActualizacion
        ) VALUES (
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,%s,%s,%s
        )
    """

    cursor_mysql.executemany(insert_query, formatted_rows)
    conn_mysql.commit()

    # --------------------------------------------------------
    # CORREO OK
    # --------------------------------------------------------
    enviar_correo(
        "ETL Pabellón – Proceso OK",
        (
            "Proceso ETL de pabellón ejecutado correctamente.\n\n"
            f"Fecha clínica procesada : {fecha_proceso}\n"
            f"Fecha ejecución real    : {fecha_ejecucion_real}\n"
            f"Registros cargados      : {len(formatted_rows)}\n"
        ),
        es_error=False,
        adjunto_path=ruta_excel
    )

    logging.info("Proceso finalizado correctamente")

except Exception as e:
    logging.error(str(e), exc_info=True)

    enviar_correo(
        "ETL Pabellón – ERROR",
        str(e),
        es_error=True,
        adjunto_path=ruta_excel
    )

finally:
    if conn_iris:
        conn_iris.close()
    if conn_mysql:
        conn_mysql.close()
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
