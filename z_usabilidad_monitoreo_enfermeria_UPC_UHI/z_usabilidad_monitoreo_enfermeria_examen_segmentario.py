import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

# =========================
# Cargar variables de entorno
# =========================
load_dotenv(override=True)

# =========================
# Configurar logging
# =========================
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_usabilidad_monitoreo_enfermeria_examen_segmentario.log"),
        logging.StreamHandler()
    ]
)

# =========================
# Crear tabla MySQL (DROP + CREATE)
# =========================
def crear_tabla(cursor_mysql):
    cursor_mysql.execute("DROP TABLE IF EXISTS z_usabilidad_monitoreo_enfermeria_examen_segmentario")

    create_table_sql = """
    CREATE TABLE z_usabilidad_monitoreo_enfermeria_examen_segmentario (
        episodio VARCHAR(50),
        fecha_registro VARCHAR(255),
        hora_registro VARCHAR(50),
        creador VARCHAR(50),
        local VARCHAR(50),
        CantExamenes VARCHAR(255),
        fechaActualizacion VARCHAR(25),
        INDEX idx_episodio (episodio),
        INDEX idx_Fecha_Orden (fecha_registro)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cursor_mysql.execute(create_table_sql)

# =========================
# Variables entorno
# =========================
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = int(os.getenv('DB_MYSQL_PORT', 3306))
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # =========================
    # Iniciar JVM
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

    query = """
        SELECT 
            o.OBS_ParRef->MRADM_ADM_DR->PAADM_ADMNo AS episodio,
            o.OBS_Date as fecha_registro,
            o.OBS_Time as hora_registro,
            OBS_User_DR->SSUSR_Name as creador,
            OBS_User_DR->SSUSR_DefaultDept_DR->CTLOC_Desc as "local",
            COUNT(DISTINCT o.OBS_Entry_DR) AS CantExamenes
        FROM MR_Observations o
        WHERE OBS_ParRef >= 7700000 and o.OBS_Date >= '2026-01-07'
        and  OBS_ParRef->MRADM_ADM_DR->PAADM_Hospital_DR = 10448
        and OBS_User_DR->SSUSR_DefaultDept_DR in (4087,3815,4113,4696,4060,4091)
        AND o.OBS_Item_DR IN (270,271,272,273,275,276,277,278,279,280,281,282,283,284,286,364,410,467,801,818,876,939,960)
        AND OBS_User_DR->SSUSR_CareProv_DR->CTPCP_CarPrvTp_DR=57
        GROUP BY 
            o.OBS_ParRef->MRADM_ADM_DR->PAADM_ADMNo,OBS_User_DR->SSUSR_Name,o.OBS_Date;
    """

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    logging.info(f"Registros obtenidos desde IRIS: {len(rows)}")

    # =========================
    # Convertir TODO a string
    # =========================
    fecha_actualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    formatted_rows = []

    for row in rows:
        converted_row = []
        for value in row:
            if value is None:
                converted_row.append("")
            else:
                converted_row.append(str(value))
        converted_row.append(fecha_actualizacion)
        formatted_rows.append(tuple(converted_row))

    # =========================
    # Conexión MySQL
    # =========================
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    crear_tabla(cursor_mysql)
    conn_mysql.commit()
    logging.info("Tabla recreada correctamente.")

    # =========================
    # Insert
    # =========================
    insert_query = """
        INSERT INTO z_usabilidad_monitoreo_enfermeria_examen_segmentario (
            episodio,
            fecha_registro,
            hora_registro,
            creador,
            local,
            CantExamenes,
            fechaActualizacion
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    chunk_size = 1000
    for i in range(0, len(formatted_rows), chunk_size):
        cursor_mysql.executemany(
            insert_query,
            formatted_rows[i:i + chunk_size]
        )
        conn_mysql.commit()

    logging.info("Carga finalizada correctamente.")

except Exception as e:
    logging.error(f"Error en ejecución: {e}")

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