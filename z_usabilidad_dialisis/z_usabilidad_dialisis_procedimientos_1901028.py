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
        logging.FileHandler("logs/z_usabilidad_dialisis_procedimientos_1901028.log"),
        logging.StreamHandler()
    ]
)

# IRIS
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# MySQL
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
def crear_tabla_z_usabilidad_dialisis_procedimientos_1901028(cursor_mysql):
    cursor_mysql.execute("""
    CREATE TABLE IF NOT EXISTS z_usabilidad_dialisis_procedimientos_1901028 (
        nro_episodio VARCHAR(11),
        nro_registro VARCHAR(7),
        descripcion_grupo_departamento VARCHAR(32),
        descripcion_local_agendamiento VARCHAR(12),
        descripción_recurso VARCHAR(25),
        fecha_cita VARCHAR(10),
        hora_cita VARCHAR(8),
        estado_cita VARCHAR(9),
        fecha_indicacion VARCHAR(10),
        hora_indicacion VARCHAR(8),
        descripcion_profesional_genera_indicacion VARCHAR(30),
        descripcion_categoria VARCHAR(21),
        descripcion_subCategoria VARCHAR(8),
        descripcion_estado_indicacion VARCHAR(10),
        estamentoProfesional VARCHAR(15),
        fechaActualizacion VARCHAR(19)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    if not jdbc_driver_name or not jdbc_driver_loc:
        raise ValueError("Driver JDBC no configurado")
    if not iris_connection_string or not iris_user or not iris_password:
        raise ValueError("Credenciales IRIS incompletas")
    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        raise ValueError("Credenciales MySQL incompletas")

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

    query = ''' 
        SELECT										
        OEORI_APPT_DR->APPT_Adm_DR->PAADM_ADMNO AS "nro_episodio",
        OEORI_APPT_DR->APPT_PAPMI_DR->PAPMI_No AS "nro_registro",																
        OEORI_APPT_DR->APPT_Adm_DR->PAADM_DepCode_DR->CTLOC_DEP_DR->DEP_desc AS "descripcion_grupo_departamento",
        OEORI_APPT_DR->APPT_AS_ParRef->AS_RES_ParRef->RES_CTLOC_DR->CTLOC_desc AS "descripcion_local_agendamiento",
        OEORI_APPT_DR->APPT_AS_ParRef->AS_RES_ParRef->RES_Desc as "descripción_recurso",
        OEORI_APPT_DR->APPT_DateComp as "fecha_cita",
        OEORI_APPT_DR->APPT_TimeComp as "hora_cita",
        (CASE
            WHEN OEORI_APPT_DR->APPT_STATUS = 'P' then 'Agendado'
            WHEN OEORI_APPT_DR->APPT_STATUS = 'D' then 'Atendido'
            WHEN OEORI_APPT_DR->APPT_STATUS = 'X' then 'Cancelado'
            WHEN OEORI_APPT_DR->APPT_STATUS = 'A' then 'Llegó'
            WHEN OEORI_APPT_DR->APPT_STATUS = 'N' then 'No atendido'
            WHEN OEORI_APPT_DR->APPT_STATUS = 'T' then 'Transferido'
            WHEN OEORI_APPT_DR->APPT_STATUS = 'H' then 'En Espera'
            ELSE OEORI_APPT_DR->APPT_STATUS
        end) AS "estado_cita",
        OEORI_SttDat as "fecha_indicacion",
        OEORI_SttTim as "hora_indicacion",
        OEORI_UserAdd->SSUSR_Name AS "descripcion_profesional_genera_indicacion",
        OEORI_Categ_DR->ORCAT_Desc AS "descripcion_categoria",
        OEORI_SubCateg_DR->ARCIC_desc AS "descripcion_subCategoria",
        OEORI_ItemStat_DR->OSTAT_Desc as "descripcion_estado_indicacion",
        OEORI_UserAdd->SSUSR_CareProv_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc AS "estamentoProfesional"
        FROM OE_OrdItem						
        WHERE OEORI_SttDat >= '2025-04-23'
        AND OEORI_APPT_DR->APPT_Adm_DR->PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448 
        AND OEORI_APPT_DR->APPT_RBCServ_DR->SER_ARCIM_DR = '82425||1'
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
    # CREAR TABLA SI NO EXISTE (ANTES DEL TRUNCATE)
    # ============================================================
    crear_tabla_z_usabilidad_dialisis_procedimientos_1901028(cursor_mysql)
    conn_mysql.commit()

    cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_dialisis_procedimientos_1901028")
    conn_mysql.commit()

    insert_query = """
        INSERT INTO z_usabilidad_dialisis_procedimientos_1901028 (
            nro_episodio,
            nro_registro,
            descripcion_grupo_departamento,
            descripcion_local_agendamiento,
            descripción_recurso,
            fecha_cita,
            hora_cita,
            estado_cita,
            fecha_indicacion,
            hora_indicacion,
            descripcion_profesional_genera_indicacion,
            descripcion_categoria,
            descripcion_subCategoria,
            descripcion_estado_indicacion,
            estamentoProfesional,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
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
