import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging

from datetime import datetime

load_dotenv(override=True)

# Configurar logging para que también imprima en la consola
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("logs/z_urgencias_espera_camas.log"),
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

    cursor_iris = conn_iris.cursor()

    # Consulta SQL para obtener datos
    query = ''' 
        SELECT
            PAADM.PAADM_ADMNo AS Nro_Episodio,
            PAPMI.PAPMI_No AS Nro_Registro,
            PAPMI.PAPMI_Name AS Apellido_Paterno,
            PAPMI.PAPMI_Name2 AS Nombres,
            PAADM.PAADM_AdmDate AS Fecha_Admision,
            PAADM.PAADM_AdmTime AS Hora_Admision,
            PAADM.PAADM_InpatBedReqDate AS Fecha_Solicitud_Cama,
            PAADM.PAADM_InpatBedReqTime AS Hora_Solicitud_Cama,
            CT_LOC.CTLOC_Desc AS Unidad_Solicitada,
            CT_LOC.CTLOC_Code AS Codigo_Unidad
        FROM
            PA_Adm PAADM
        JOIN
            PA_PatMas PAPMI ON PAADM.PAADM_PAPMI_DR = PAPMI.PAPMI_RowID
        LEFT JOIN
            CT_LOC ON PAADM.PAADM_CurrentWard_DR = CT_LOC.CTLOC_RowID
        WHERE
            PAADM.PAADM_VisitStatus = 'A' -- Episodio activo
            AND PAADM.PAADM_CurrentBed_DR IS NULL -- Sin cama asignada
            AND PAADM.PAADM_InpatBedReqDate IS NOT NULL -- Solicitud de cama
            AND PAADM.PAADM_AdmDate >= CURRENT_DATE - 30 -- Últimos 30 días
            AND PAADM.PAADM_Type = 'E' -- Ingreso por urgencia
            AND PAADM.PAADM_Hospital_DR = 10448
        ORDER BY
            CT_LOC.CTLOC_Desc,
            PAADM.PAADM_InpatBedReqDate ASC;
        '''
    
    #PAADM.PAADM_VisitStatus = 'A' Episodio activo
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        valores.append(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        formatted_rows.append(tuple(valores))

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
        cursor_mysql.execute("TRUNCATE TABLE z_urgencias_espera_camas")
        conn_mysql.commit()
        logging.info("Tabla 'z_urgencias_espera_camas' truncada exitosamente.")
    except mysql.connector.Error as e:
        logging.error(f"Error al truncar la tabla: {e}")
        raise

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_urgencias_espera_camas (
        Nro_Episodio,
        Nro_Registro,
        Apellido_Paterno,
        Nombres,
        Fecha_Admision,
        Hora_Admision,
        Fecha_Solicitud_Cama,
        Hora_Solicitud_Cama,
        Unidad_Solicitada,
        Codigo_Unidad,
        fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s
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
