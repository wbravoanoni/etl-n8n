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
# CONFIGURACIÓN PRINCIPAL + LOGGING
# ============================================================

load_dotenv(override=True)

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_usabilidad_oftalmologia_evoluciones.log"),
        logging.StreamHandler()
    ]
)

# VARIABLES IRIS
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc  = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# VARIABLES MYSQL
mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = os.getenv('DB_MYSQL_PORT')
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

def fail_and_exit(message):
    """ Registrar error y terminar con exit code 1 """
    logging.error(message)
    sys.exit(1)

def encrypt_parity_check(message):
    load_dotenv()
    key = os.getenv('ENCRYPTION_KEY').encode()
    fernet = Fernet(key)
    return fernet.encrypt(message.encode())

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # ============================================================
    # VALIDACIONES VARIABLES ENTORNO
    # ============================================================
    if not jdbc_driver_name or not jdbc_driver_loc:
        fail_and_exit("JDBC_DRIVER_NAME o JDBC_DRIVER_PATH no configurados.")

    if not iris_connection_string or not iris_user or not iris_password:
        fail_and_exit("Variables IRIS no configuradas correctamente.")

    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        fail_and_exit("Variables MySQL no configuradas correctamente.")

    # ============================================================
    # INICIAR JVM
    # ============================================================
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # ============================================================
    # CONEXIÓN A IRIS
    # ============================================================
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

    # ============================================================
    # CONSULTA IRIS
    # ============================================================
    query = '''
        SELECT DISTINCT 
            b.NOT_ParRef->MRADM_ADM_DR->PAAdm_AdmNo AS "NumeroEpisodio",
            b.NOT_Status_DR->NNS_Desc AS "Estado_Evolucion",
            b.NOT_ClinNoteSens_DR->CNS_Desc AS "Grupo_Evolucion",
            b.NOT_ClinNotesType_DR->CNT_Desc AS "Tipo_Evolucion",
            b.NOT_User_DR->SSUSR_Name AS "Usuario_Evolucion",
            CONVERT(VARCHAR, b.NOT_Date ,105) AS "FechaEvolucion",
            CONVERT(VARCHAR(5), b.NOT_Time, 108) AS "HoraEvolucion",
            b.NOT_NurseId_DR->CTPCP_Desc AS "ProfesionalEvolucion",
            b.NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc AS "EstamentoProfesional",
            b.NOT_UserAuth_DR->SSUSR_DefaultDept_DR->CTLOC_Desc AS "local_usuario",
            a.ENC_Loc_DR->CTLOC_Desc AS "Local_Encuentro"
        FROM 
            MR_NursingNotes b
        JOIN 
            MR_Encounter a ON b.NOT_ParRef = a.ENC_MRAdm_DR
        WHERE 
            b.NOT_Date >= '2025-04-23'
            AND b.NOT_Hospital_DR = 10448
            AND b.NOT_ParRef->MRADM_ADM_DR->PAAdm_Type = 'O'
            AND a.ENC_StartDate >= '2025-04-23'
            AND a.ENC_Loc_DR IN (
                2869, 2871, 2872, 2873, 2874, 2875, 2680,
                3437, 3850, 4260, 4254, 4264, 4257, 4261,
                4253, 4259, 4258, 4617, 4531, 4532
            );
    '''

    try:
        cursor_iris.execute(query)
        rows = cursor_iris.fetchall()
    except Exception as e:
        fail_and_exit(f"Error ejecutando consulta IRIS: {e}")

    # ============================================================
    # FORMATEO DE FILAS
    # ============================================================
    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))

    # ============================================================
    # CONEXIÓN A MYSQL
    # ============================================================
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

    # TRUNCATE
    try:
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_oftalmologia_evoluciones")
        conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error al truncar tabla MySQL: {e}")

    # ============================================================
    # INSERT EN CHUNKS
    # ============================================================
    insert_query = """
        INSERT INTO z_usabilidad_oftalmologia_evoluciones (
            NumeroEpisodio,
            Estado_Evolucion,
            Grupo_Evolucion,
            Tipo_Evolucion,
            Usuario_Evolucion,
            FechaEvolucion,
            HoraEvolucion,
            ProfesionalEvolucion,
            EstamentoProfesional,
            local_usuario,
            Local_Encuentro,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    try:
        for i in range(0, len(formatted_rows), 1000):
            chunk = formatted_rows[i:i+1000]
            cursor_mysql.executemany(insert_query, chunk)
            conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error insertando datos en MySQL: {e}")

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
