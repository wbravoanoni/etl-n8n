import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
import sys
from datetime import datetime
from cryptography.fernet import Fernet

# ============================================================
# CONFIGURACIÓN INICIAL
# ============================================================

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_usabilidad_hospitalizados_evoluciones.log"),
        logging.StreamHandler()
    ]
)

# Variables IRIS
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc  = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# Variables MySQL
mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = os.getenv('DB_MYSQL_PORT')
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

# ============================================================
# FUNCIONES DE CONTROL DE ERRORES
# ============================================================

def fail_and_exit(message):
    logging.error(message)
    sys.exit(1)

# ============================================================
# INICIO DE PROCESO
# ============================================================

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # ------------------------------------------------------------
    # Validación variables de entorno
    # ------------------------------------------------------------
    if not jdbc_driver_name or not jdbc_driver_loc:
        fail_and_exit("JDBC_DRIVER_NAME o JDBC_DRIVER_PATH no configurados.")

    if not iris_connection_string or not iris_user or not iris_password:
        fail_and_exit("Variables de conexión IRIS no configuradas.")

    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        fail_and_exit("Variables de conexión MySQL no configuradas.")

    # ------------------------------------------------------------
    # Iniciar JVM (si no está iniciada)
    # ------------------------------------------------------------
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # ------------------------------------------------------------
    # Conexión IRIS
    # ------------------------------------------------------------
    logging.info("Conectando a InterSystems IRIS...")

    try:
        conn_iris = jaydebeapi.connect(
            jdbc_driver_name,
            iris_connection_string,
            {'user': iris_user, 'password': iris_password},
            jdbc_driver_loc
        )
    except Exception as e:
        fail_and_exit(f"Error conectando a IRIS: {e}")

    cursor_iris = conn_iris.cursor()

    # ------------------------------------------------------------
    # Consulta SQL
    # ------------------------------------------------------------
    query = ''' 
            select %nolock
            NOT_Hospital_DR->HOSP_Code,
            NOT_Hospital_DR->HOSP_Desc,
            NOT_ParRef->MRADM_ADM_DR->PAAdm_AdmNo as NumeroEpisodio,
            NOT_Status_DR->NNS_Desc as Estado_Evolucion,
            NOT_ClinNoteSens_DR->CNS_Desc as Grupo_Evolucion,
            NOT_ClinNotesType_DR->CNT_Desc as Tipo_Evolucion,
            NOT_User_DR->SSUSR_Name as Usuario_Evolucion,
            CONVERT(VARCHAR,NOT_Date ,105) as FechaEvolucion,
            convert( varchar(5), NOT_Time, 108 )as HoraEvolucion,
            NOT_NurseId_DR->CTPCP_Desc as ProfesionalEvolucion,
            NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc as EstamentoProfesional,
            NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_ID as RUNPaciente,
            NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_Name2 as NombresPaciente,
            NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_Name as AppPaternoPaciente,
            NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_Name3 as AppMaternoPaciente,
            NOT_ParRef->MRADM_ADM_DR->PAADM_CurrentWard_DR->WARD_Desc AS local_actual,
            NOT_Hospital_DR,
            convert(varchar, NOT_ParRef->MRADM_ADM_DR->PAADM_EstimDischargeDate, 105),
            convert(varchar, NOT_ParRef->MRADM_ADM_DR->PAADM_EstimDischargeTime, 108),
            convert(varchar, NOT_ParRef->MRADM_ADM_DR->PAADM_DischgDate, 105),
            convert(varchar, NOT_ParRef->MRADM_ADM_DR->PAADM_DischgTime, 108)
            from SQLUser.MR_NursingNotes
            where
                NOT_Date >= '2024-10-09'
                and NOT_Hospital_DR = 10448
                AND NOT_ParRef->MRADM_ADM_DR->PAAdm_Type='I'
        '''

    try:
        cursor_iris.execute(query)
        rows = cursor_iris.fetchall()
    except Exception as e:
        fail_and_exit(f"Error al ejecutar consulta en IRIS: {e}")

    # ------------------------------------------------------------
    # Formateo de filas
    # ------------------------------------------------------------
    formatted_rows = []

    for row in rows:
        formatted_rows.append(tuple(
            '' if r is None else str(r)
            for r in row
        ) + (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))

    # ------------------------------------------------------------
    # Conexión a MySQL
    # ------------------------------------------------------------
    logging.info("Conectando a MySQL...")

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

    # ------------------------------------------------------------
    # TRUNCATE
    # ------------------------------------------------------------
    try:
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_hospitalizados_evoluciones")
        conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error truncando tabla MySQL: {e}")

    # ------------------------------------------------------------
    # INSERT
    # ------------------------------------------------------------
    insert_query = """
        INSERT INTO z_usabilidad_hospitalizados_evoluciones (
            HOSP_Code, HOSP_Desc, NumeroEpisodio, Estado_Evolucion, Grupo_Evolucion,
            Tipo_Evolucion, Usuario_Evolucion, FechaEvolucion, HoraEvolucion,
            ProfesionalEvolucion, EstamentoProfesional, RUNPaciente, NombresPaciente,
            AppPaternoPaciente, AppMaternoPaciente, local_actual, nOT_Hospital_DR,
            fecha_alta_medica, hora_alta_medica, fecha_alta_adm, hora_alta_adm,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """

    try:
        for i in range(0, len(formatted_rows), 1000):
            cursor_mysql.executemany(insert_query, formatted_rows[i:i+1000])
            conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error insertando datos en MySQL: {e}")

    logging.info("✅ Datos transferidos exitosamente.")
    sys.exit(0)

# ============================================================
# EXCEPCIONES GENERALES
# ============================================================
except Exception as e:
    fail_and_exit(f"Error inesperado: {e}")

finally:
    if cursor_iris: cursor_iris.close()
    if conn_iris: conn_iris.close()
    if cursor_mysql: cursor_mysql.close()
    if conn_mysql: conn_mysql.close()
    if jpype.isJVMStarted(): jpype.shutdownJVM()
