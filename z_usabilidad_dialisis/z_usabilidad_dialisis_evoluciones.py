import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
from cryptography.fernet import Fernet

load_dotenv(override=True)

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_usabilidad_dialisis_evoluciones.log"),
        logging.StreamHandler()
    ]
)

jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = os.getenv('DB_MYSQL_PORT')
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

def encrypt_parity_check(message):
    load_dotenv()
    key = os.getenv('ENCRYPTION_KEY').encode()
    fernet = Fernet(key)
    encrypted = fernet.encrypt(message.encode())
    return encrypted

# ============================================================
# FUNCIÓN: crear tabla si no existe (ÚNICA ADICIÓN)
# ============================================================
def crear_tabla_z_usabilidad_dialisis_evoluciones(cursor_mysql):
    cursor_mysql.execute("""
    CREATE TABLE IF NOT EXISTS z_usabilidad_dialisis_evoluciones (
        HOSP_Code VARCHAR(20),
        HOSP_Desc VARCHAR(20),
        NumeroEpisodio VARCHAR(11),
        Estado_Evolucion VARCHAR(10),
        Grupo_Evolucion VARCHAR(40),
        Tipo_Evolucion VARCHAR(15),
        Usuario_Evolucion VARCHAR(20),
        FechaEvolucion VARCHAR(10),
        HoraEvolucion VARCHAR(5),
        ProfesionalEvolucion VARCHAR(31),
        EstamentoProfesional VARCHAR(15),
        RUNPaciente VARCHAR(20),
        NombresPaciente VARCHAR(20),
        AppPaternoPaciente VARCHAR(20),
        AppMaternoPaciente VARCHAR(20),
        local_actual VARCHAR(20),
        local_usuario VARCHAR(32),
        tipo VARCHAR(1),
        NOT_Hospital_DR VARCHAR(20),
        fecha_alta_medica VARCHAR(10),
        hora_alta_medica VARCHAR(20),
        fecha_alta_adm VARCHAR(20),
        hora_alta_adm VARCHAR(20),
        RUT_Usuario_Evolucion VARCHAR(10),
        Local_Agendamiento VARCHAR(20),
        fechaActualizacion VARCHAR(19)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    if not jdbc_driver_name or not jdbc_driver_loc:
        logging.error("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
        raise ValueError("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
    if not iris_connection_string or not iris_user or not iris_password:
        logging.error("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
        raise ValueError("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        logging.error("Las variables de entorno de MySQL no están configuradas correctamente.")
        raise ValueError("Las variables de entorno de MySQL no están configuradas correctamente.")

    if not jpype.isJVMStarted():
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            "-Djava.class.path=" + jdbc_driver_loc
        )

    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )

    cursor_iris = conn_iris.cursor()

    query = f'''
        SELECT %nolock
        NOT_ParRef->MRADM_ADM_DR->PAAdm_AdmNo as "NumeroEpisodio",
        NOT_Status_DR->NNS_Desc as "Estado_Evolucion",
        NOT_ClinNoteSens_DR->CNS_Desc as "Grupo_Evolucion",
        NOT_ClinNotesType_DR->CNT_Desc as "Tipo_Evolucion",
        CONVERT(VARCHAR, NOT_Date ,105) as "FechaEvolucion",
        CONVERT(VARCHAR(5), NOT_Time, 108) as "HoraEvolucion",
        NOT_User_DR->SSUSR_Name as "ProfesionalEvolucion",
        NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc as "EstamentoProfesional",
        NOT_UserAuth_DR->SSUSR_DefaultDept_DR->CTLOC_Desc as "local_usuario",
        NOT_ParRef->MRADM_ADM_DR->PAAdm_Type as "tipo",
        CONVERT(VARCHAR, NOT_ParRef->MRADM_ADM_DR->PAADM_EstimDischargeDate, 105) as "fecha_alta_medica",
        NOT_User_DR->SSUSR_Initials as "RUT_Usuario_Evolucion"
        FROM SQLUser.MR_NursingNotes
        WHERE NOT_Date >= '2025-04-23'
        AND NOT_Hospital_DR = 10448
        AND NOT_User_DR->SSUSR_Initials IN (
            '9982733-5','17875948-5','14279744-5','17455908-2','8481701-5',
            '9980347-9','15308230-8','13049986-4','15020310-4','26438372-2',
            '17811321-6','16852669-5','17120240-k','26079313-6'
        )
    '''

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))

    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    # ============================================================
    # LLAMADA A CREAR TABLA (ANTES DEL TRUNCATE)
    # ============================================================
    crear_tabla_z_usabilidad_dialisis_evoluciones(cursor_mysql)
    conn_mysql.commit()

    cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_dialisis_evoluciones")
    conn_mysql.commit()

    insert_query = """
        INSERT INTO z_usabilidad_dialisis_evoluciones (
            NumeroEpisodio, Estado_Evolucion, Grupo_Evolucion, Tipo_Evolucion,
            FechaEvolucion, HoraEvolucion, ProfesionalEvolucion,
            EstamentoProfesional, local_usuario, tipo,
            fecha_alta_medica, RUT_Usuario_Evolucion, fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        )
    """

    chunk_size = 1000
    for i in range(0, len(formatted_rows), chunk_size):
        chunk = formatted_rows[i:i + chunk_size]
        cursor_mysql.executemany(insert_query, chunk)
        conn_mysql.commit()

    logging.info("Datos transferidos exitosamente.")

except Exception as e:
    logging.error(f"Error: {e}")

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
