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
        logging.FileHandler("logs/z_usabilidad_monitoreo_enfermeria_kits_UPC_UHI.log"),
        logging.StreamHandler()
    ]
)

# =========================
# Crear tabla MySQL (DROP + CREATE)
# =========================
def crear_tabla(cursor_mysql):
    cursor_mysql.execute("DROP TABLE IF EXISTS z_usabilidad_monitoreo_enfermeria_kits_UPC_UHI")

    create_table_sql = """
    CREATE TABLE z_usabilidad_monitoreo_enfermeria_kits_UPC_UHI (
        Nro_Episodio VARCHAR(50),
        Sala VARCHAR(255),
        Fecha_Orden VARCHAR(50),
        Nombre_Kit VARCHAR(255),
        Usuario VARCHAR(255),
        Cant_Items_Kit VARCHAR(20),
        fechaActualizacion VARCHAR(25),
        INDEX idx_episodio (Nro_Episodio),
        INDEX idx_Fecha_Orden (Fecha_Orden)
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
            adm.PAADM_ADMNO,
            w.WARD_Desc,
            oi.OEORI_Date,
            oi.OEORI_ARCOS_DR->ARCOS_Desc,
            oi.OEORI_UserAdd->SSUSR_Name,
            COUNT(*)
        FROM OE_OrdItem oi
        INNER JOIN OE_Order o
            ON oi.OEORI_OEORD_ParRef = o.OEORD_RowId
        INNER JOIN PA_ADM adm
            ON o.OEORD_Adm_DR = adm.PAADM_RowId
        LEFT JOIN PAC_Ward w
            ON adm.PAADM_CurrentWard_DR = w.WARD_RowID
        WHERE
            oi.OEORI_ARCOS_DR IS NOT NULL
            AND oi.OEORI_Date >=  '2025-07-01'
            AND adm.PAADM_CurrentWard_DR IN (416,402,417,509,428,415)
            AND oi.OEORI_ARCOS_DR->ARCOS_Desc LIKE 'KIT%'
        GROUP BY
            adm.PAADM_ADMNO,
            w.WARD_Desc,
            oi.OEORI_Date,
            oi.OEORI_ARCOS_DR,
            oi.OEORI_UserAdd
        ORDER BY
            oi.OEORI_Date DESC
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
        INSERT INTO z_usabilidad_monitoreo_enfermeria_kits_UPC_UHI (
            Nro_Episodio,
            Sala,
            Fecha_Orden,
            Nombre_Kit,
            Usuario,
            Cant_Items_Kit,
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