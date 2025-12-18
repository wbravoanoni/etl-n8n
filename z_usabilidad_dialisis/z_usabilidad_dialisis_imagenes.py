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

# Configurar logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_usabilidad_dialisis_imagenes.log"),
        logging.StreamHandler()
    ]
)

# Variables IRIS
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# Variables MySQL
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
def crear_tabla_z_usabilidad_dialisis_imagenes(cursor_mysql):
    cursor_mysql.execute("""
    CREATE TABLE IF NOT EXISTS z_usabilidad_dialisis_imagenes (
        episodio VARCHAR(11),
        tipo_registro VARCHAR(25),
        local VARCHAR(12),
        fecha_creacion VARCHAR(10),
        creador VARCHAR(29),
        tipo VARCHAR(1),
        fechaActualizacion VARCHAR(19)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # Validaciones
    if not jdbc_driver_name or not jdbc_driver_loc:
        raise ValueError("Driver JDBC no configurado")
    if not iris_connection_string or not iris_user or not iris_password:
        raise ValueError("Credenciales IRIS incompletas")
    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        raise ValueError("Credenciales MySQL incompletas")

    # Iniciar JVM
    if not jpype.isJVMStarted():
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            "-Djava.class.path=" + jdbc_driver_loc
        )

    # Conexión IRIS
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )
    cursor_iris = conn_iris.cursor()

    query = ''' 
        SELECT
            PIC_ParRef->MRADM_ADM_DR->PAADM_ADMNo as "episodio",
            PIC_DocType_DR->Doctype_Desc as "tipo_registro",
            PIC_UserCreated->SSUSR_DefaultDept_DR->CTLOC_Desc as "local",
            PIC_DateCreated AS "fecha_creacion",
            PIC_UserCreated->SSUSR_Name as "creador",
            PIC_ParRef->MRADM_ADM_DR->PAAdm_Type
        FROM MR_Pictures a
        WHERE PIC_DateCreated >= '2025-04-23' 
        AND PIC_UserCreated->SSUSR_DefaultDept_DR = 3806;
    '''

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    formatted_rows = []
    for row in rows:
        episodio = '' if row[0] is None else str(row[0])
        tipo_registro = '' if row[1] is None else str(row[1])
        local = '' if row[2] is None else str(row[2])
        fecha_creacion = '' if row[3] is None else str(row[3])
        creador = '' if row[4] is None else str(row[4])
        tipo = '' if row[5] is None else str(row[5])
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

    # Conexión MySQL
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
    crear_tabla_z_usabilidad_dialisis_imagenes(cursor_mysql)
    conn_mysql.commit()

    # Truncar tabla
    cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_dialisis_imagenes")
    conn_mysql.commit()
    logging.info("Tabla 'z_usabilidad_dialisis_imagenes' truncada exitosamente.")

    insert_query = """
        INSERT INTO z_usabilidad_dialisis_imagenes (
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
