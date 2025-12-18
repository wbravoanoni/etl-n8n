import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
import sys
from datetime import datetime
from cryptography.fernet import Fernet

load_dotenv(override=True)

os.makedirs("logs", exist_ok=True)

# Configurar logging para que también imprima en la consola
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("logs/z_usabilidad_coloproctologia_sol_quirurgica.log"),
                        Logging.StreamHandler()])

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Leer variables de entorno para InterSystems IRIS
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')

iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# Leer variables de entorno para MySQL
mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = os.getenv('DB_MYSQL_PORT')
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

def encrypt_parity_check(message):
    load_dotenv()
    key = os.getenv('ENCRYPTION_KEY').encode()  # Cargar clave desde .env
    fernet = Fernet(key)
    encrypted = fernet.encrypt(message.encode())
    return encrypted

def fail_and_exit(message):
    logging.error(message)
    sys.exit(1)

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # Validar variables de entorno
    if not jdbc_driver_name or not jdbc_driver_loc:
        fail_and_exit("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
    if not iris_connection_string or not iris_user or not iris_password:
        fail_and_exit("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        fail_and_exit("Las variables de entorno de MySQL no están configuradas correctamente.")
    
    # Iniciar JVM si no está ya iniciada
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), classpath=[jdbc_driver_loc])

    # Crear conexión con InterSystems IRIS
    try:
        conn_iris = jaydebeapi.connect(
            jdbc_driver_name,
            iris_connection_string,
            {'user': iris_user, 'password': iris_password},
            jdbc_driver_loc
        )
    except Exception as e:
        fail_and_exit(f"Error conectando a InterSystems IRIS: {e}")

    # Consulta SQL para obtener datos
    query = ''' 
            SELECT
            appt.APPT_Adm_DR->PAADM_ADMNO AS "Nro_Episodio",
            appt.APPT_PAPMI_DR->PAPMI_ID AS "RUN_Paciente",
            appt.APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name2 AS "Nombre",
            appt.APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name AS "Apellido_Paterno",
            appt.APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name3 AS "Apellido_Materno",
            CONVERT(VARCHAR, appt.APPT_DateComp, 105) AS "Fecha_Cita",
            appt.APPT_AS_ParRef->AS_RES_ParRef->RES_Desc AS "Profesional_Agenda",
            wl.WL_NO AS "ID_Solicitud_Qx",
            wl.WL_Date AS "Fecha_Solicitud_Qx",
            wl.WL_Operation_DR->OPER_Desc AS "Cirugia_Principal",
            wl.WL_BodySitePrimaryProc_DR->BODS_Desc AS "Sitio_Operacion",
            wl.WL_Laterality_DR->LATER_Desc AS "Lateridad",
            wl.WL_WaitListStatus_DR->WLS_Desc AS "Estado_Solicitud",
            wl.WL_User_DR->SSUSR_Name AS "Usuario_Creador",
            wl.WL_ICD_DR->MRCID_Desc AS "Diagnostico"
        FROM
            RB_Appointment appt
            LEFT JOIN PA_WaitingList wl 
                ON appt.APPT_PAPMI_DR = wl.WL_PAPMI_DR
                AND ABS(DATEDIFF(DAY, appt.APPT_DateComp, wl.WL_Date)) <= 7
        WHERE
            appt.APPT_DateComp >= '2025-09-01'
            AND appt.APPT_Adm_DR->PAADM_DepCode_DR->CTLOC_HOSPITAL_DR = 10448
            AND appt.APPT_AS_ParRef->AS_RES_ParRef->RES_CTLOC_DR = 2831
            AND appt.APPT_STATUS <> 'X'
            AND wl.WL_WaitListType_DR->WLT_Code = 'PAB'
        ORDER BY
            appt.APPT_DateComp DESC;
            '''

    # Ejecutar consulta en InterSystems IRIS
    cursor_iris = conn_iris.cursor()
    try:
        cursor_iris.execute(query)
        rows = cursor_iris.fetchall()
    except Exception as e:
        fail_and_exit(f"Error ejecutando consulta SQL en IRIS: {e}")

    # Convertir filas a formato adecuado para MySQL
    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))

    # Conectar a MySQL
    try:
        conn_mysql = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
    except Exception as e:
        fail_and_exit(f"Error conectando a MySQL: {e}")

    cursor_mysql = conn_mysql.cursor()

    # Truncar la tabla en MySQL
    try:
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_coloproctologia_sol_quirurgica")
        conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error truncando tabla MySQL: {e}")

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_usabilidad_coloproctologia_sol_quirurgica (
            Nro_Episodio,
            RUN_Paciente,
            Nombre,
            Apellido_Paterno,
            Apellido_Materno,
            Fecha_Cita,
            Profesional_Agenda,
            ID_Solicitud_Qx,
            Fecha_Solicitud_Qx,
            Cirugia_Principal,
            Sitio_Operacion,
            Lateridad,
            Estado_Solicitud,
            Usuario_Creador,
            Diagnostico,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s
        )
    """

    try:
        chunk_size = 1000
        for i in range(0, len(formatted_rows), chunk_size):
            chunk = formatted_rows[i:i + chunk_size]
            cursor_mysql.executemany(insert_query, chunk)
            conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error insertando registros en MySQL: {e}")

    logging.info("Datos transferidos exitosamente.")
    sys.exit(0)

except Exception as e:
    fail_and_exit(f"Error inesperado: {e}")

finally:
    if cursor_iris:
        cursor_iris.close()
    if conn_iris:
        conn_iris.close()
    if cursor_mysql:
        cursor_mysql.close()
    if conn_mysql:
        conn_mysql.close()
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
