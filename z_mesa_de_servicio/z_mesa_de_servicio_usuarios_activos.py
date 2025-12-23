import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
from cryptography.fernet import Fernet

load_dotenv(override=True)

# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_mesa_de_servicio_usuarios_activos.log"),
        logging.StreamHandler()
    ]
)

# =========================
# Variables IRIS
# =========================
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# =========================
# Variables MySQL
# =========================
mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = int(os.getenv('DB_MYSQL_PORT', 3306))
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

# =========================
# Funciones
# =========================
def encrypt_parity_check(message):
    key = os.getenv('ENCRYPTION_KEY').encode()
    fernet = Fernet(key)
    return fernet.encrypt(message.encode())

def recreate_table_mysql(cursor_mysql):
    cursor_mysql.execute("DROP TABLE IF EXISTS z_mesa_de_servicio_usuarios_activos")
    cursor_mysql.execute("""
        CREATE TABLE z_mesa_de_servicio_usuarios_activos (
            RUT                     VARCHAR(12),
            descripcion             VARCHAR(42),
            nombre                  VARCHAR(28),
            apellido                VARCHAR(25),
            Local                   VARCHAR(53),
            Establecimiento         VARCHAR(45),
            Grupo                   VARCHAR(59),
            Perfil                  VARCHAR(77),
            FechaInicio             VARCHAR(10),
            SSUSR_DateLastLogin     VARCHAR(10),
            SSUSR_Initials          VARCHAR(12),
            SSUSR_DateTo            VARCHAR(10),
            fechaActualizacion      DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

# =========================
# Conexiones
# =========================
conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # =========================
    # Validaciones
    # =========================
    if not jdbc_driver_name or not jdbc_driver_loc:
        raise ValueError("JDBC no configurado")
    if not iris_connection_string or not iris_user or not iris_password:
        raise ValueError("Credenciales IRIS incompletas")
    if not mysql_host or not mysql_user or not mysql_password or not mysql_database:
        raise ValueError("Credenciales MySQL incompletas")

    # =========================
    # JVM
    # =========================
    if not jpype.isJVMStarted():
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            "-Djava.class.path=" + jdbc_driver_loc
        )

    # =========================
    # Conexión IRIS
    # =========================
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )
    cursor_iris = conn_iris.cursor()

    # =========================
    # Query IRIS (sin cambios)
    # =========================
    query = """
        SELECT DISTINCT BY (SSUSR_RowId)
            SSUSR_Initials AS RUT,
            SSUSR_Name AS descripcion,
            SSUSR_GivenName AS nombre,
            SSUSR_Surname AS apellido,
            SSUSR_DefaultDept_DR->CTLOC_Desc AS "Local",
            SSUSR_DefaultDept_DR->CTLOC_Hospital_DR->HOSP_Desc AS Establecimiento,
            SSUSR_Group->SSGRP_Desc AS Grupo,
            SSUSR_Profile->SSP_Desc AS Perfil,
            SSUSR_DateFrom AS FechaInicio,
            SSUSR_DateLastLogin,
            SSUSR_Initials,
            SSUSR_DateTo
        FROM SS_User
        WHERE SSUSR_Active = 'Y'
          AND (SSUSR_StaffType_DR->STAFF_Code <> 'IS' OR SSUSR_StaffType_DR IS NULL)
          AND (SSUSR_DateTo IS NULL OR SSUSR_DateTo >= CURRENT_DATE)
          AND SSUSR_Hospital_DR = 10448;
    """

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()
    logging.info(f"Filas obtenidas desde IRIS: {len(rows)}")

    # =========================
    # Formateo (TODO TEXTO)
    # =========================
    formatted_rows = []
    for row in rows:
        formatted_rows.append((
            str(row[0] or ''),
            str(row[1] or ''),
            str(row[2] or ''),
            str(row[3] or ''),
            str(row[4] or ''),
            str(row[5] or ''),
            str(row[6] or ''),
            str(row[7] or ''),
            str(row[8] or ''),
            str(row[9] or ''),
            str(row[10] or ''),
            str(row[11] or ''),
            datetime.now()
        ))

    # =========================
    # MySQL
    # =========================
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    recreate_table_mysql(cursor_mysql)
    conn_mysql.commit()

    insert_sql = """
        INSERT INTO z_mesa_de_servicio_usuarios_activos (
            RUT, descripcion, nombre, apellido, Local, Establecimiento,
            Grupo, Perfil, FechaInicio,
            SSUSR_DateLastLogin, SSUSR_Initials, SSUSR_DateTo,
            fechaActualizacion
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    chunk_size = 1000
    for i in range(0, len(formatted_rows), chunk_size):
        cursor_mysql.executemany(insert_sql, formatted_rows[i:i + chunk_size])
        conn_mysql.commit()

    logging.info("Carga z_mesa_de_servicio_usuarios_activos finalizada correctamente")

except Exception as e:
    logging.error(f"Error general: {e}", exc_info=True)

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
