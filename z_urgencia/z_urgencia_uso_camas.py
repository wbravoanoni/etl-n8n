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
                    handlers=[logging.FileHandler("logs/z_urgencia_uso_camas.log"),
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
            BED_RowID,
            BED.BED_Code AS "codigo_cama",
            BED.BED_BedType_DR->BEDTP_Desc AS "tipo_cama",
            BED.BED_Room_DR->ROOM_Desc AS "sala",
            BED.BED_Room_DR->ROOM_RoomType_DR->ROOMT_Desc AS "tipo_sala",
            BED.BED_Available AS "disponible",
            PAADM.PAADM_ADMNo AS "N° episodio",
            PAADM.PAADM_PAPMI_DR->PAPMI_ID AS "rut_paciente",
            PAADM.PAADM_PAPMI_DR->PAPMI_Name2 AS "nombre",
            PAADM.PAADM_PAPMI_DR->PAPMI_Name AS "apellido_paterno",
            PAADM.PAADM_PAPMI_DR->PAPMI_Name3 AS "apellido_materno",
            PAADM.PAADM_AdmDate AS "fecha_admision",
            PAADM.PAADM_AdmTime AS "hora_admision",
            STAT.STAT_Status_DR AS "codigo_estado_cama",
            STAT.STAT_Date AS "fecha_cambio_estado",
            STAT.STAT_Time AS "hora_cambio_estado",
            STAT.STAT_DateTo AS "fecha_fin_bloqueo",
            STAT.STAT_TimeTo AS "hora_fin_bloqueo",
            CASE 
                WHEN (STAT.STAT_DateTo IS NULL OR 
                    STAT.STAT_DateTo > CURRENT_DATE OR 
                    (STAT.STAT_DateTo = CURRENT_DATE AND STAT.STAT_TimeTo > CURRENT_TIME)) 
                THEN STAT.STAT_ReasonNotAvail_DR->RNAV_Desc
                ELSE NULL
            END AS "motivo_bloqueo",
            CASE 
                WHEN BED.BED_Available = 'N' AND PAADM.PAADM_ADMNo IS NULL THEN 'No disponible'
                WHEN BED.BED_Available = 'N' AND PAADM.PAADM_ADMNo IS NOT NULL THEN 'Ocupada'
                WHEN STAT.STAT_DateTo IS NULL THEN 'Bloqueada (sin fin)'
                WHEN STAT.STAT_DateTo > CURRENT_DATE THEN 'Bloqueada (vigente)'
                WHEN STAT.STAT_DateTo = CURRENT_DATE AND STAT.STAT_TimeTo > CURRENT_TIME THEN 'Bloqueada (vigente)'
                WHEN STAT.STAT_DateTo < CURRENT_DATE THEN 'Disponible'
                WHEN STAT.STAT_DateTo = CURRENT_DATE AND STAT.STAT_TimeTo <= CURRENT_TIME THEN 'Disponible'
                ELSE 'Estado desconocido'
            END AS "Estado Interpretado"
            FROM 
            PAC_BED BED
            LEFT JOIN PA_ADM PAADM 
            ON BED.BED_RowID = PAADM.PAADM_CurrentBed_DR 
            AND PAADM.PAADM_VisitStatus = 'A'
            AND PAADM.PAADM_Hospital_DR = 10448
            AND PAADM.PAADM_AdmDate >= '2025-06-01'
            LEFT JOIN PAC_BedStatusChange STAT 
            ON STAT.STAT_RowId IN (
                SELECT TOP 1 S2.STAT_RowId 
                FROM PAC_BedStatusChange S2
                WHERE S2.STAT_ParRef = BED.BED_RowID 
                    AND S2.STAT_Status_DR = 2
                ORDER BY S2.STAT_Date DESC, S2.STAT_Time DESC
            )
            WHERE 
            BED_WARD_ParRef = 395 
            ORDER BY 
            BED.BED_Code;
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
        cursor_mysql.execute("TRUNCATE TABLE z_urgencia_uso_camas")
        conn_mysql.commit()
        logging.info("Tabla 'z_urgencia_uso_camas' truncada exitosamente.")
    except mysql.connector.Error as e:
        logging.error(f"Error al truncar la tabla: {e}")
        raise

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_urgencia_uso_camas (
        BED_RowID,
        codigo_cama,
        tipo_cama,
        sala,
        tipo_sala,
        disponible,
        n_episodio,
        rut_paciente,
        nombre,
        apellido_paterno,
        apellido_materno,
        fecha_admision,
        hora_admision,
        codigo_estado_cama,
        fecha_cambio_estado,
        hora_cambio_estado,
        fecha_fin_bloqueo,
        hora_fin_bloqueo,
        motivo_bloqueo,
        estado_interpretado,
        fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
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
