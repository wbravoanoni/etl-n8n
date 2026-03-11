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

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_usabilidad_qf_procedimientos.log"),
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


def fail_and_exit(message):
    logging.error(message)
    sys.exit(1)


def encrypt_parity_check(message):
    key = os.getenv('ENCRYPTION_KEY').encode()
    fernet = Fernet(key)
    return fernet.encrypt(message.encode())


conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:

    # VALIDACIONES
    if not jdbc_driver_name or not jdbc_driver_loc:
        fail_and_exit("El nombre o la ruta del controlador JDBC no están configurados correctamente.")

    if not iris_connection_string or not iris_user or not iris_password:
        fail_and_exit("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")

    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        fail_and_exit("Las variables de entorno de MySQL no están configuradas correctamente.")

    # Iniciar JVM
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # Conexión IRIS
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

    # QUERY IRIS
    query = '''
    SELECT %nolock
        OEORI_OEORD_ParRef->OEORD_Adm_DR->PAADM_ADMNo as episodio,
        OEORI_Date as fecha_solicitado,
        OEORI_TimeOrd as hora_solicitado,
        OEORI_OEORD_ParRef->OEORD_Adm_DR->PAADM_PAPMI_DR->PAPMI_ID AS run,
        OEORI_OEORD_ParRef->OEORD_Adm_DR->PAADM_PAPMI_DR->PAPMI_Name2 as nombres_paciente,
        OEORI_OEORD_ParRef->OEORD_Adm_DR->PAADM_PAPMI_DR->PAPMI_Name as app_paterno,
        OEORI_OEORD_ParRef->OEORD_Adm_DR->PAADM_PAPMI_DR->PAPMI_Name3 as app_materno,
        OEORI_ItemStat_DR->OSTAT_Desc as estado_indicacion,
        OEORI_ItmMast_DR->ARCIM_Code AS codigo_prestacion,
        OEORI_ItmMast_DR->ARCIM_Desc AS prestacion_indicada,
        OEORI_Doctor_DR->CTPCP_Code as rut_profesional,
        OEORI_Doctor_DR->CTPCP_Desc as profesional,
        OEORI_OrdDept_DR->CTLOC_desc as local_solicitante,
        OEORI_RecDep_DR->CTLOC_desc as local_receptor,
        OEORI_OEORD_ParRef->OEORD_Adm_DR->PAADM_TYPE AS "tipo_episodio"

    FROM OE_OrdItem 

    WHERE  
        OEORI_SttDat >= '2026-02-26'
        AND OEORI_Doctor_DR->CTPCP_CarPrvTp_DR->CTCPT_RowId = 65
        AND OEORI_Doctor_DR->CTPCP_CreatedHosp_DR = 10448
    '''

    try:
        cursor_iris.execute(query)
        rows = cursor_iris.fetchall()
    except Exception as e:
        fail_and_exit(f"Error ejecutando consulta IRIS: {e}")

    # FORMATEO
    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))

    # Conexión MySQL
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
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_qf_procedimientos")
        conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error al truncar MySQL: {e}")

    # INSERT MYSQL
    insert_query = """
        INSERT INTO z_usabilidad_qf_procedimientos (
            episodio,
            fecha_solicitado,
            hora_solicitado,
            run,
            nombres_paciente,
            app_paterno,
            app_materno,
            estado_indicacion,
            codigo_prestacion,
            prestacion_indicada,
            rut_profesional,
            profesional,
            local_solicitante,
            local_receptor,
            tipo_episodio,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    try:
        for i in range(0, len(formatted_rows), 1000):
            chunk = formatted_rows[i:i + 1000]
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