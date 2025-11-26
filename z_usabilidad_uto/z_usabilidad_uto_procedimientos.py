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
                    handlers=[logging.FileHandler("logs/z_usabilidad_uto_procedimientos.log"),
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
            SELECT %nolock DISTINCT 
            OEORI_APPT_DR->APPT_Adm_DR->PAADM_ADMNO "nro_episodio",
            OEORI_APPT_DR->APPT_PAPMI_DR->PAPMI_No AS "nro_registro",
            OEORI_APPT_DR->APPT_Adm_DR->PAADM_DepCode_DR->CTLOC_DEP_DR->DEP_desc "descripcion_grupo_departamento",
            OEORI_APPT_DR->APPT_AS_ParRef->AS_RES_ParRef->RES_CTLOC_DR->CTLOC_desc "descripcion_local_agendamiento",
            OEORI_APPT_DR->APPT_AS_ParRef->AS_RES_ParRef->RES_Desc as "descripcion_recurso",
            OEORI_APPT_DR->APPT_AS_ParRef->AS_RES_ParRef->RES_CTPCP_DR->CTPCP_Spec_DR->CTSPC_Desc as "descripcion_especialidad_recurso",
            OEORI_APPT_DR->APPT_RBCServ_DR->SER_ARCIM_DR->ARCIM_desc as "descripcion_prestacion_agendada",
            convert(varchar,OEORI_APPT_DR->APPT_DateComp,105) as "fecha_cita",
            convert(varchar,OEORI_APPT_DR->APPT_TimeComp,108) as "hora_cita",
            (CASE
                    WHEN OEORI_APPT_DR->APPT_STATUS = 'P' then 'Agendado'
                    WHEN OEORI_APPT_DR->APPT_STATUS = 'D' then 'Atendido'
                    WHEN OEORI_APPT_DR->APPT_STATUS = 'X' then 'Cancelado'
                    WHEN OEORI_APPT_DR->APPT_STATUS = 'A' then 'Llegó'
                    WHEN OEORI_APPT_DR->APPT_STATUS = 'N' then 'No atendido'
                    WHEN OEORI_APPT_DR->APPT_STATUS = 'T' then 'Transferido'
                    WHEN OEORI_APPT_DR->APPT_STATUS = 'H' then 'En Espera'
                    WHEN OEORI_APPT_DR->APPT_STATUS = 'S' then 'Llegó - No atendido'
                    ELSE OEORI_APPT_DR->APPT_STATUS
                end) AS "estado_cita",
            OEORI_OrdDept_DR->CTLOC_desc "descripcion_local_solicitante",
            OEORI_RecDep_DR->CTLOC_desc "descripcion_local_receptor",
            convert(varchar,OEORI_SttDat,105) as "fecha_indicacion",
            convert(varchar,OEORI_SttTim,108) as "hora_indicacion",
            OEORI_UserAdd->SSUSR_Name "descripcion_profesional_genera_indicacion",
            OEORI_Categ_DR->ORCAT_Desc "descripcion_categoria",
            OEORI_SubCateg_DR->ARCIC_desc "descripcion_SubCategoria",
            OEORI_ItmMast_DR->ARCIM_Desc as "descripcion_prestacion",
            OEORI_ItemStat_DR->OSTAT_Desc as "descripcion_estado_indicacion"
            from OE_OrdItem 
            where 
            OEORI_SttDat>='2025-04-23' and
            OEORI_APPT_DR->APPT_AS_ParRef->AS_RES_ParRef->RES_CTLOC_DR in (4070,4994)
            '''
    
# 4070  HDS- UTO    Policlínico de Trauma Ocular HDS
# 4994  HDS-07-854  Procedimientos UTO HDS

    
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
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_uto_procedimientos")
        conn_mysql.commit()
        logging.info("Tabla 'z_usabilidad_uto_procedimientos' truncada exitosamente.")
    except mysql.connector.Error as e:
        logging.error(f"Error al truncar la tabla: {e}")
        raise

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_usabilidad_uto_procedimientos (
            nro_episodio,
            nro_registro,
            descripcion_grupo_departamento,
            descripcion_local_agendamiento,
            descripcion_recurso,
            descripcion_especialidad_recurso,
            descripcion_prestacion_agendada,
            fecha_cita,
            hora_cita,
            estado_cita,
            descripcion_local_solicitante,
            descripcion_local_receptor,
            fecha_indicacion,
            hora_indicacion,
            descripcion_profesional_genera_indicacion,
            descripcion_categoria,
            descripcion_SubCategoria,
            descripcion_prestacion,
            descripcion_estado_indicacion,
            fechaActualizacion

        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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