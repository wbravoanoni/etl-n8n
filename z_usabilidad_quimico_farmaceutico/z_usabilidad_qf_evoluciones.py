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
        logging.FileHandler("logs/z_usabilidad_qf_evoluciones.log"),
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
        AND table_name = 'z_usabilidad_qf_evoluciones';
    """, (mysql_database,))

    exists = cursor.fetchone()[0]

    if exists:
        logging.info("Tabla z_usabilidad_qf_evoluciones ya existe.")
        return

    logging.info("Creando tabla z_usabilidad_qf_evoluciones...")

    create_sql = """
    CREATE TABLE `z_usabilidad_qf_evoluciones` (
        `FechaEvolucion` VARCHAR(45),
        `HoraEvolucion` VARCHAR(45),
        `ProfesionalEvolucion` VARCHAR(45),
        `EstamentoProfesional` VARCHAR(45),
        `Estado_Evolucion` VARCHAR(45),
        `local_actual` VARCHAR(45),
        `RUNPaciente` VARCHAR(45),
        `NombresPaciente` VARCHAR(45),
        `AppPaternoPaciente` VARCHAR(45),
        `AppMaternoPaciente` VARCHAR(45),
        `NumeroEpisodio` VARCHAR(45),
        `local_usuario` VARCHAR(45),
        `tipo_episodio` VARCHAR(45),
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
        SELECT
        CONVERT(VARCHAR,NOT_Date ,105) as FechaEvolucion,
        convert( varchar(5), NOT_Time, 108 )as HoraEvolucion,
        NOT_NurseId_DR->CTPCP_Desc as ProfesionalEvolucion,
        NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc as EstamentoProfesional,
        NOT_Status_DR->NNS_Desc as Estado_Evolucion,
        NOT_ParRef->MRADM_ADM_DR->PAADM_CurrentWard_DR->WARD_Desc AS local_actual,
        NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_ID as RUNPaciente,
        NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_Name2 as NombresPaciente,
        NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_Name as AppPaternoPaciente,
        NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_Name3 as AppMaternoPaciente,
		NOT_ParRef->MRADM_ADM_DR->PAAdm_AdmNo as NumeroEpisodio,
		NOT_UserAuth_DR->SSUSR_DefaultDept_DR->CTLOC_Desc AS "local_usuario",
        NOT_ParRef->MRADM_ADM_DR->PAADM_TYPE AS "tipo_episodio"
        from SQLUser.MR_NursingNotes
        where
            NOT_Date >= '2026-02-26'
            and NOT_Hospital_DR = 10448
            -- Químico Farmacéutico
            AND NOT_ClinNotesType_DR=54;
    '''
    #NOT_Date >= '2024-10-09'
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
    cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_qf_evoluciones")
    conn_mysql.commit()

    # INSERT
    insert_query = """
        INSERT INTO z_usabilidad_qf_evoluciones (
            FechaEvolucion,
            HoraEvolucion,
            ProfesionalEvolucion,
            EstamentoProfesional,
            Estado_Evolucion,
            local_actual,
            RUNPaciente,
            NombresPaciente,
            AppPaternoPaciente,
            AppMaternoPaciente,
            NumeroEpisodio,
            local_usuario,
            tipo_episodio,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
