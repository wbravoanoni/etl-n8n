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

os.makedirs("logs", exist_ok=True)

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

def fail_and_exit(message):
    logging.error(message)
    sys.exit(1)

def create_table_if_not_exists_evoluciones(cursor, conn):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_name = 'z_usabilidad_hospitalizados_evoluciones';
    """, (mysql_database,))

    exists = cursor.fetchone()[0]

    if exists:
        logging.info("Tabla z_usabilidad_hospitalizados_evoluciones ya existe.")
        return

    logging.info("Creando tabla z_usabilidad_hospitalizados_evoluciones...")

    create_sql = """
    CREATE TABLE `z_usabilidad_hospitalizados_evoluciones` (
      `HOSP_Code` VARCHAR(6),
      `HOSP_Desc` VARCHAR(45),
      `NumeroEpisodio` VARCHAR(11),
      `Estado_Evolucion` VARCHAR(10),
      `Grupo_Evolucion` VARCHAR(40),
      `Tipo_Evolucion` VARCHAR(29),
      `Usuario_Evolucion` VARCHAR(44),
      `FechaEvolucion` VARCHAR(10),
      `HoraEvolucion` VARCHAR(5),
      `ProfesionalEvolucion` VARCHAR(44),
      `EstamentoProfesional` VARCHAR(22),
      `RUNPaciente` VARCHAR(10),
      `NombresPaciente` VARCHAR(39),
      `AppPaternoPaciente` VARCHAR(21),
      `AppMaternoPaciente` VARCHAR(22),
      `local_actual` VARCHAR(45),
      `NOT_Hospital_DR` VARCHAR(5),
      `fecha_alta_medica` VARCHAR(10),
      `hora_alta_medica` VARCHAR(8),
      `fecha_alta_adm` VARCHAR(10),
      `hora_alta_adm` VARCHAR(8),
      `fechaActualizacion` DATETIME
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
    """

    cursor.execute(create_sql)
    conn.commit()
    logging.info("Tabla creada exitosamente.")


# ============================================================
# INICIO DE PROCESO
# ============================================================

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # Validaciones
    if not jdbc_driver_name or not jdbc_driver_loc:
        fail_and_exit("JDBC_DRIVER_NAME o JDBC_DRIVER_PATH no configurados.")

    if not iris_connection_string or not iris_user or not iris_password:
        fail_and_exit("Variables IRIS incompletas.")

    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        fail_and_exit("Variables MySQL incompletas.")

    # Iniciar JVM
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # Conexión IRIS
    logging.info("Conectando a InterSystems IRIS...")
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )
    cursor_iris = conn_iris.cursor()

    # Consulta IRIS
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

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    formatted_rows = [
        tuple('' if r is None else str(r) for r in row)
        + (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
        for row in rows
    ]

    # Conexión MySQL
    logging.info("Conectando a MySQL...")
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    # CREAR TABLA SI NO EXISTE
    create_table_if_not_exists_evoluciones(cursor_mysql, conn_mysql)

    # TRUNCATE
    cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_hospitalizados_evoluciones")
    conn_mysql.commit()

    # INSERT
    insert_query = """
        INSERT INTO z_usabilidad_hospitalizados_evoluciones (
            HOSP_Code, HOSP_Desc, NumeroEpisodio, Estado_Evolucion, Grupo_Evolucion,
            Tipo_Evolucion, Usuario_Evolucion, FechaEvolucion, HoraEvolucion,
            ProfesionalEvolucion, EstamentoProfesional, RUNPaciente, NombresPaciente,
            AppPaternoPaciente, AppMaternoPaciente, local_actual, NOT_Hospital_DR,
            fecha_alta_medica, hora_alta_medica, fecha_alta_adm, hora_alta_adm,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """

    for i in range(0, len(formatted_rows), 1000):
        cursor_mysql.executemany(insert_query, formatted_rows[i:i+1000])
        conn_mysql.commit()

    logging.info(" Datos transferidos exitosamente.")
    sys.exit(0)

except Exception as e:
    fail_and_exit(f"Error inesperado: {e}")

finally:
    if cursor_iris: cursor_iris.close()
    if conn_iris: conn_iris.close()
    if cursor_mysql: cursor_mysql.close()
    if conn_mysql: conn_mysql.close()
    if jpype.isJVMStarted(): jpype.shutdownJVM()
