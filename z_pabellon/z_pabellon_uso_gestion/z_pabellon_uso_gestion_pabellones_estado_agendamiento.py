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
                    handlers=[logging.FileHandler("logs/z_pabellon_prueba_concepto_tisne.log"),
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
SELECT %NOLOCK 
    RBOP.RBOP_PAADM_DR->PAADM_ADMNO AS episodio,
    TO_CHAR(RBOP.RBOP_DateOper, 'DD-MM-YYYY') AS fecha_cirugia,
    CASE RBOP.RBOP_Status
        WHEN 'B'  THEN 'AGENDADO'
        WHEN 'CL'  THEN 'CERRADO'
        WHEN 'C'  THEN 'CONFIRMADO'
        WHEN 'SF'  THEN 'ENVIADO POR'
        WHEN 'SK'  THEN 'ENVIADO POR RECONOCIDO'
        WHEN 'N'  THEN 'NO LISTO'
        WHEN 'P'  THEN 'POSTERGADO'
        WHEN 'D'  THEN 'REALIZADO'
        WHEN 'A'  THEN 'RECEPCIONADO'
        WHEN 'DP' THEN 'SALIDA'
        WHEN 'R'  THEN 'SOLICITADO'
        WHEN 'X'  THEN 'SUSPENDIDO'
        ELSE 'OTRO'
    END AS estado_cirugia,
    CASE RBOP.RBOP_BookingType
        WHEN 'EL'  THEN 'Cirugía Electiva Programada'
        WHEN 'ENP' THEN 'Cirugía Electiva No Programada'
        WHEN 'EM'  THEN 'Cirugía Urgencia'
        ELSE 'Otro'
    END AS tipo_cirugia,
    CASE 
        WHEN RBOP.RBOP_Status = 'X'
             AND (ANA.ANA_TheatreInDate IS NULL 
                  AND ANA.ANA_CustomDate1 IS NULL)
        THEN 'Suspendida ANTES de iniciar'
        WHEN RBOP.RBOP_Status = 'X'
             AND (ANA.ANA_TheatreInDate IS NOT NULL 
                  OR ANA.ANA_CustomDate1 IS NOT NULL)
        THEN 'Suspendida DESPUÉS de iniciar'
        ELSE NULL
    END AS momento_suspension,
    ANAOP.ANAOP_Depar_Oper_DR->CTLOC_Desc AS area_qx,
    RBOP.RBOP_Resource_DR->RES_Desc AS pabellon,
    RBOP.RBOP_RowId AS id_cirugia,
    RBOP.RBOP_ReasonSuspend_DR->SUSP_Desc AS motivo_suspension,
    CASE WHEN HOL.CTHOL_Code IS NOT NULL THEN 'Sí' ELSE 'No' END AS es_festivo,
    HOL.CTHOL_Desc AS nombre_festivo,
    ANA.ANA_TheatreInDate  AS fecha_ingreso_quirofano,
    ANA.ANA_TheatreInTime  AS hora_ingreso_quirofano,
    ANA.ANA_TheatreOutDate AS fecha_egreso_quirofano,
    ANA.ANA_TheatreOutTime AS hora_egreso_quirofano,
    ANA.ANA_CustomDate1 AS fecha_inicio_cirugia_en_protocolo_anestesico,
    ANA.ANA_CustomTime1 AS hora_inicio_cirugia_protAnest,
    ANA.ANA_CustomDate2 AS fecha_termino_cirugia_en_protocolo_anestesico,
    ANA.ANA_CustomTime2 AS hora_termino_cirugia_en_protocolo_anestesico,
    ANAOP.ANAOP_OpStartDate AS fecha_inicio_cirugia_en_protocolo_operatorio,
    ANAOP.ANAOP_OpStartTime AS hora_inicio_cirugia_en_protocolo_operatorio,
    ANAOP.ANAOP_OpEndDate   AS fecha_termino_cirugia_en_protocolo_operatorio,
    ANAOP.ANAOP_OpEndTime   AS hora_termino_cirugia_en_protocolo_operatorio,
    ANAOP.ANAOP_Type_DR->OPER_Code AS codigo_cirugia_principal_del_protocolo_operatorio,
    ANAOP.ANAOP_Type_DR->OPER_Desc AS descripcion_cirugia_principal_del_protocolo_operatorio,
    RBOP.RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_ID AS RUT_Paciente,
    ANAOP.ANAOP_No AS numero_cirugia
FROM RB_OperatingRoom RBOP
/* Tomamos solo el ÚLTIMO registro de anestesia por episodio */
LEFT JOIN OR_Anaesthesia ANA
  ON RBOP.RBOP_PAADM_DR = ANA.ANA_PAADM_ParRef
 AND ANA.ANA_RowId = (
        SELECT MAX(A2.ANA_RowId)
        FROM OR_Anaesthesia A2
        WHERE A2.ANA_PAADM_ParRef = RBOP.RBOP_PAADM_DR
    )
/* Tomamos solo el ÚLTIMO protocolo operatorio por cirugía */
LEFT JOIN OR_Anaest_Operation ANAOP
  ON ANA.ANA_RowId = ANAOP.ANAOP_Par_Ref
 AND ANAOP.ANAOP_No = (
        SELECT MAX(O2.ANAOP_No)
        FROM OR_Anaest_Operation O2
        WHERE O2.ANAOP_Par_Ref = ANA.ANA_RowId
    )
LEFT JOIN CT_PubHol HOL
  ON RBOP.RBOP_DateOper = HOL.CTHOL_Code
WHERE 
    RBOP.RBOP_DateOper >= '2025-01-01'
    AND RBOP.RBOP_DateOper <= CURRENT_DATE
    -- AND RBOP.RBOP_TimeOper > 0
    AND RBOP.RBOP_RowId > 0
    AND RBOP.RBOP_Resource_DR IS NOT NULL
    AND RBOP.RBOP_PAADM_DR->PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448
ORDER BY fecha_cirugia, pabellon, estado_cirugia;
        '''
        
    # Ejecutar consulta en InterSystems IRIS
    cursor_iris = conn_iris.cursor()
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # Convertir filas a formato adecuado para MySQL
    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))

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
        cursor_mysql.execute("TRUNCATE TABLE z_pabellon_uso_gestion_pabellones_estado_agendamiento")
        conn_mysql.commit()
        logging.info("Tabla 'z_pabellon_uso_gestion_pabellones_estado_agendamiento' truncada exitosamente.")
    except mysql.connector.Error as e:
        logging.error(f"Error al truncar la tabla: {e}")
        raise

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_pabellon_uso_gestion_pabellones_estado_agendamiento (
        episodio,
        fecha_cirugia,
        estado_cirugia,
        tipo_cirugia,
        momento_suspension,
        area_qx,
        pabellon,
        id_cirugia,
        motivo_suspencion,
        es_festivo,
        nombre_festivo,
        fecha_ingreso_quirofano,
        hora_ingreso_quirofano,
        fecha_egreso_quirofano,
        hora_egreso_quirofano,
        fecha_inicio_cirugia_en_protocolo_anestesico,
        hora_inicio_cirugia_protAnest,
        fecha_termino_cirugia_en_protocolo_anestesico,
        hora_termino_cirugia_en_protocolo_anestesico,
        fecha_inicio_cirugia_en_protocolo_operatorio,
        hora_inicio_cirugia_en_protocolo_operatorio,
        fecha_termino_cirugia_en_protocolo_operatorio,
        hora_termino_cirugia_en_protocolo_operatorio,
        codigo_cirugia_principal_del_protocolo_pperatorio,
        descripcion_cirugia_principal_del_protocolo_operatorio,
        RUT_Paciente,
        numero_cirugia,
        fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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