import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
from cryptography.fernet import Fernet

# =========================
# Cargar variables entorno
# =========================
load_dotenv(override=True)

# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_mesa_de_servicio_grupos_perfiles.log"),
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

def create_table_if_not_exists(cursor_mysql):
    sql = """
    CREATE TABLE IF NOT EXISTS z_mesa_de_servicio_grupos_perfiles (
        RUT                     VARCHAR(12),
        descripcion             VARCHAR(45),
        nombre                  VARCHAR(32),
        apellido                VARCHAR(32),
        Local                   VARCHAR(53),
        Establecimiento         VARCHAR(45),
        Grupo                   VARCHAR(59),
        Perfil                  VARCHAR(77),
        Tipo                    VARCHAR(9),
        FechaInicio             VARCHAR(10),
        SSUSR_DateLastLogin     VARCHAR(10),
        SSUSR_Initials          VARCHAR(12),
        SSUSR_DateTo            VARCHAR(10),
        fechaActualizacion      DATETIME
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cursor_mysql.execute(sql)

# =========================
# Conexiones
# =========================
conn_iris = None
cursor_iris = None
conn_mysql = None
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
            f"-Djava.class.path={jdbc_driver_loc}"
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
    # Query IRIS (válida)
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
        'Principal' AS Tipo,
        SSUSR_DateFrom AS FechaInicio,
        SSUSR_DateLastLogin,
        SSUSR_Initials,
        SSUSR_DateTo
    FROM SS_User
    WHERE SSUSR_Active = 'Y'
      AND (SSUSR_StaffType_DR->STAFF_Code <> 'IS' OR SSUSR_StaffType_DR IS NULL)
      AND SSUSR_Hospital_DR = 10448

    UNION

    SELECT DISTINCT BY (OTHLL_RowId)
        OTHLL_ParRef->SSUSR_Initials AS RUT,
        OTHLL_ParRef->SSUSR_Name AS descripcion,
        OTHLL_ParRef->SSUSR_GivenName AS nombre,
        OTHLL_ParRef->SSUSR_Surname AS apellido,
        OTHLL_CTLOC_DR->CTLOC_Desc AS "Local",
        OTHLL_Hospital_DR->HOSP_Desc AS Establecimiento,
        OTHLL_UserGroup_DR->SSGRP_Desc AS Grupo,
        OTHLL_Profile_DR->SSP_Desc AS Perfil,
        'Otros' AS Tipo,
        OTHLL_DateFrom AS FechaInicio,
        '',
        '',
        OTHLL_DateTo
    FROM SS_UserOtherLogonLoc
    WHERE OTHLL_ParRef->SSUSR_Active = 'Y'
      AND (OTHLL_ParRef->SSUSR_StaffType_DR->STAFF_Code <> 'IS' OR OTHLL_ParRef->SSUSR_StaffType_DR IS NULL)
      AND OTHLL_Hospital_DR = 10448
    ORDER BY 1;
    """

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()
    logging.info(f"Filas obtenidas desde IRIS: {len(rows)}")

    # =========================
    # Formateo (TODO TEXTO)
    # =========================
    formatted_rows = []
    for r in rows:
        formatted_rows.append((
            str(r[0] or ''),
            str(r[1] or ''),
            str(r[2] or ''),
            str(r[3] or ''),
            str(r[4] or ''),
            str(r[5] or ''),
            str(r[6] or ''),
            str(r[7] or ''),
            str(r[8] or ''),
            str(r[9] or ''),
            str(r[10] or ''),
            str(r[11] or ''),
            str(r[12] or ''),
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

    create_table_if_not_exists(cursor_mysql)
    conn_mysql.commit()

    cursor_mysql.execute("TRUNCATE TABLE z_mesa_de_servicio_grupos_perfiles")
    conn_mysql.commit()

    insert_sql = """
    INSERT INTO z_mesa_de_servicio_grupos_perfiles (
        RUT, descripcion, nombre, apellido, Local, Establecimiento,
        Grupo, Perfil, Tipo, FechaInicio,
        SSUSR_DateLastLogin, SSUSR_Initials, SSUSR_DateTo,
        fechaActualizacion
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    chunk_size = 1000
    for i in range(0, len(formatted_rows), chunk_size):
        cursor_mysql.executemany(insert_sql, formatted_rows[i:i+chunk_size])
        conn_mysql.commit()

    logging.info("Proceso finalizado correctamente")

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
