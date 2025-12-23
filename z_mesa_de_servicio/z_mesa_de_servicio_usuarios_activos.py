import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging

from datetime import datetime
from cryptography.fernet import Fernet

load_dotenv(override=True)

# Configurar logging para que también imprima en la consola
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("logs/z_mesa_de_servicio_usuarios_activos.log"),
                        logging.StreamHandler()])

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Leer variables de entorno para InterSystems IRIS
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')

iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# Leer variables de entorno para MySQL
mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = os.getenv('DB_MYSQL_PORT')
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

def encrypt_parity_check(message):
    load_dotenv()
    key = os.getenv('ENCRYPTION_KEY').encode()  # Cargar clave desde .env
    fernet = Fernet(key)
    encrypted = fernet.encrypt(message.encode())
    return encrypted

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # Validar variables de entorno
    if not jdbc_driver_name or not jdbc_driver_loc:
        logging.error("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
        raise ValueError("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
    if not iris_connection_string or not iris_user or not iris_password:
        logging.error("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
        raise ValueError("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        logging.error("Las variables de entorno de MySQL no están configuradas correctamente.")
        raise ValueError("Las variables de entorno de MySQL no están configuradas correctamente.")
    
    # Iniciar JVM si no está ya iniciada
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # Crear conexión con InterSystems IRIS
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )

    # Consulta SQL para obtener datos
    query = ''' 
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
        FROM 
            SS_User
        WHERE 
            SSUSR_Active = 'Y'
            AND (SSUSR_StaffType_DR->STAFF_Code <> 'IS' OR SSUSR_StaffType_DR IS NULL)
            AND (SSUSR_DateTo IS NULL or SSUSR_DateTo >= CURRENT_DATE)
            AND SSUSR_Hospital_DR = 10448;
            '''
    
    #SSUSR_Active = 'Y' AND SSUSR_DateLastLogin < CURRENT_DATE - 14
    
    # Ejecutar consulta en InterSystems IRIS
    cursor_iris = conn_iris.cursor()
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # Convertir filas a formato adecuado para MySQL
    formatted_rows = []
    for row in rows:
        rUT = '' if row[0] is None else str(row[0])
        descripcion = '' if row[1] is None else str(row[1])
        nombre = '' if row[2] is None else str(row[2])
        apellido = '' if row[3] is None else str(row[3])
        Local = '' if row[4] is None else str(row[4])
        Establecimiento = '' if row[5] is None else str(row[5])
        Grupo = '' if row[6] is None else str(row[6])
        Perfil = '' if row[7] is None else str(row[7])
        fechaInicio = '' if row[8] is None else str(row[8])
        sSUSR_DateLastLogin = '' if row[9] is None else str(row[9])
        sSUSR_Initials = '' if row[10] is None else str(row[10])
        sSUSR_DateTo = '' if row[11] is None else str(row[11])
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        formatted_rows.append(( 
        rUT,
        descripcion,
        nombre,
        apellido,
        Local,
        Establecimiento,
        Grupo,
        Perfil,
        fechaInicio,
        sSUSR_DateLastLogin,
        sSUSR_Initials,
        sSUSR_DateTo,
        fechaActualizacion
        ))

    # Conectar a MySQL
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    # Truncar la tabla en MySQL
    try:
        cursor_mysql.execute("TRUNCATE TABLE z_mesa_de_servicio_usuarios_activos")
        conn_mysql.commit()
        logging.info("Tabla 'z_mesa_de_servicio_usuarios_activos' truncada exitosamente.")
    except mysql.connector.Error as e:
        logging.error(f"Error al truncar la tabla: {e}")
        raise

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_mesa_de_servicio_usuarios_activos (
            RUT,
            descripcion,
            nombre,
            apellido,
            Local,
            Establecimiento,
            Grupo,
            Perfil,
            FechaInicio,
            SSUSR_DateLastLogin,
            SSUSR_Initials,
            SSUSR_DateTo,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    chunk_size = 1000  # Ajusta este tamaño según sea necesario
    for i in range(0, len(formatted_rows), chunk_size):
        chunk = formatted_rows[i:i + chunk_size]
        cursor_mysql.executemany(insert_query, chunk)
        conn_mysql.commit()
    logging.info("Datos transferidos exitosamente.")
except jaydebeapi.DatabaseError as e:
    logging.error(f"Error en InterSystems IRIS: {e}")
except mysql.connector.Error as e:
    logging.error(f"Error en MySQL: {e}")
except ValueError as e:
    logging.error(f"Error en la configuración: {e}")
except Exception as e:
    logging.error(f"Error: {e}")
finally:
    # Cerrar cursores y conexiones
    if cursor_iris:
        cursor_iris.close()
    if conn_iris:
        conn_iris.close()
    if cursor_mysql:
        cursor_mysql.close()
    if conn_mysql:
        conn_mysql.close()
    # Detener la JVM si la iniciamos
    if jpype.isJVMStarted():
        jpype.shutdownJVM()