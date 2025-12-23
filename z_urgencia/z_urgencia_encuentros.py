import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
from cryptography.fernet import Fernet

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("logs/z_urgencia_encuentros.log"),
                        logging.StreamHandler()])

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
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )

    cursor_iris = conn_iris.cursor()

    query = f'''
        SELECT 
        adm.PAADM_ADMNO AS "nro_episodio",
        e.ENTRY_StartDate AS "fecha_encuentro",
        e.ENTRY_StartTime AS "hora_encuentro",
        e.ENTRY_StartUser_DR->SSUSR_CareProv_DR->CTPCP_Desc AS "nombre_profesional",
        e.ENTRY_CareProvType_DR->CTCPT_Desc AS "tipo_profesional"
        FROM MR_EncEntry e
        INNER JOIN MR_Encounter enc ON e.ENTRY_Encounter_DR = enc.ENC_RowId
        INNER JOIN MR_ADM mradm ON enc.ENC_MRAdm_DR = mradm.MRADM_RowId
        INNER JOIN PA_ADM adm ON mradm.MRADM_ADM_DR = adm.PAADM_RowId
        WHERE adm.PAADM_HOSPITAL_DR = 10448
        AND adm.PAADM_ADMDATE >= '2025-01-01'
        and e.ENTRY_StartDate>='2025-01-01'
        AND ENTRY_StartUser_DR->SSUSR_CareProv_DR->CTPCP_CarPrvTp_DR IN (56, 60, 71, 61, 73, 80, 83)
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

    cursor_mysql.execute("TRUNCATE TABLE z_urgencia_encuentros")
    conn_mysql.commit()

    insert_query = """
        INSERT INTO z_urgencia_encuentros (
        nro_episodio,fecha_encuentro,hora_encuentro,nombre_profesional,tipo_profesional,fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s
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
