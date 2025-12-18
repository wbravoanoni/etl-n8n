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
        logging.FileHandler("logs/z_usabilidad_oftalmologia_imagenes.log"),
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
    """ Registrar error real y terminar con exit code 1 para n8n """
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
    # VALIDACIÓN VARIABLES ENTORNO
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
        SELECT 
            PIC_ParRef->MRADM_ADM_DR->PAADM_ADMNo as "episodio",
            PIC_DocType_DR->Doctype_Desc as "tipo_registro",
            PIC_UserCreated->SSUSR_DefaultDept_DR->CTLOC_Desc as "local",
            PIC_DateCreated AS "fecha_Creacion",
            PIC_UserCreated->SSUSR_Name as "creador",
            PIC_ParRef->MRADM_ADM_DR->PAAdm_Type
        FROM MR_Pictures a
        WHERE PIC_DateCreated >= '2025-04-23' 
        AND PIC_UserCreated->SSUSR_DefaultDept_DR IN (
            2869, 2871, 2872, 2873, 2874, 2875, 2680, 3437, 3850,
            4260, 4254, 4264, 4257, 4261, 4253, 4259, 4258, 4617,
            4531, 4532
        )
    '''

    try:
        cursor_iris.execute(query)
        rows = cursor_iris.fetchall()
    except Exception as e:
        fail_and_exit(f"Error ejecutando consulta IRIS: {e}")

    # ============================================================
    # FORMATEAR FILAS
    # ============================================================
    formatted_rows = []
    for row in rows:
        episodio        = '' if row[0] is None else str(row[0])
        tipo_registro   = '' if row[1] is None else str(row[1])
        local           = '' if row[2] is None else str(row[2])
        fecha_creacion  = '' if row[3] is None else str(row[3])
        creador         = '' if row[4] is None else str(row[4])
        tipo            = '' if row[5] is None else str(row[5])
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        formatted_rows.append((
            episodio,
            tipo_registro,
            local,
            fecha_creacion,
            creador,
            tipo,
            fechaActualizacion
        ))

    # ============================================================
    # CONEXIÓN MYSQL
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
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_oftalmologia_imagenes")
        conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error al truncar MySQL: {e}")

    # ============================================================
    # INSERT EN CHUNKS
    # ============================================================
    insert_query = """
        INSERT INTO z_usabilidad_oftalmologia_imagenes (
            episodio,
            tipo_registro,
            local,
            fecha_creacion,
            creador,
            tipo,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s
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
