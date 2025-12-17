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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_carga_consultas_urgencias.log"),
        logging.StreamHandler()
    ]
)

# =========================
# Función: crear tabla MySQL si no existe
# =========================
def crear_tabla_z_cuestionario_braden(cursor_mysql):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS z_cuestionario_braden (
        ID VARCHAR(6),
        fecha_creacion VARCHAR(10),
        hora_creacion VARCHAR(8),
        puntaje VARCHAR(2),
        QUESCreateUserDR VARCHAR(5),
        usuario_creador VARCHAR(40),
        episodio VARCHAR(11),
        local_actual VARCHAR(50),

        Q01 TEXT,
        Q02 VARCHAR(20),
        Q03 VARCHAR(21),
        Q04 VARCHAR(19),
        Q05 VARCHAR(21),
        Q06 VARCHAR(24),
        Q08 TEXT,
        Q09 VARCHAR(2),
        Q10 VARCHAR(10),
        Q11 VARCHAR(7),
        Q12 VARCHAR(18),
        Q13 VARCHAR(41),
        Q14 VARCHAR(15),
        Q15 TEXT,
        Q16 TEXT,
        Q17 TEXT,
        Q20 VARCHAR(50),
        Q21 VARCHAR(10),
        Q22 VARCHAR(8),
        Q30 TEXT,
        Q31 TEXT,
        Q32 TEXT,
        Q33 TEXT,
        Q35 TEXT,
        Q40 TEXT,
        Q41 TEXT,
        Q42 TEXT,
        Q43 TEXT,
        Q44 TEXT,
        Q45 TEXT,
        Q46 TEXT,

        fechaActualizacion VARCHAR(19),

        INDEX idx_episodio (episodio),
        INDEX idx_fecha_creacion (fecha_creacion)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cursor_mysql.execute(create_table_sql)

# =========================
# Leer variables de entorno
# =========================

# InterSystems IRIS
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# MySQL
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
    # Validaciones
    # =========================
    if not jdbc_driver_name or not jdbc_driver_loc:
        raise ValueError("Driver JDBC no configurado")
    if not iris_connection_string or not iris_user or not iris_password:
        raise ValueError("Credenciales IRIS incompletas")
    if not mysql_host or not mysql_user or not mysql_password or not mysql_database:
        raise ValueError("Credenciales MySQL incompletas")

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
            ID,
            QUESCreateDate AS fecha_creacion,
            QUESCreateTime AS hora_creacion,
            QUESScore AS puntaje,
            QUESCreateUserDR,
            b.SSUSR_Name AS usuario_creador,
            c.PAADM_ADMNo AS episodio,
            c.PAADM_CurrentWard_DR->WARD_Desc AS local_actual,
            a.Q01,a.Q02,a.Q03,a.Q04,a.Q05,a.Q06,a.Q08,a.Q09,a.Q10,
            a.Q11,a.Q12,a.Q13,a.Q14,a.Q15,a.Q16,a.Q17,a.Q20,a.Q21,a.Q22,
            a.Q30,a.Q31,a.Q32,a.Q33,a.Q35,
            a.Q40,a.Q41,a.Q42,a.Q43,a.Q44,a.Q45,a.Q46
        FROM questionnaire.QTCEBRADEN a
        INNER JOIN SS_User b ON a.QUESCreateUserDR = b.SSUSR_RowId
        LEFT JOIN PA_Adm c ON a.QUESPAAdmDR = c.PAADM_RowID
        WHERE QUESCreateDate >= DATEADD(MONTH, -6, GETDATE())
          AND PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448;
    """

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # =========================
    # Formatear datos
    # =========================
    formatted_rows = []
    for row in rows:
        formatted_rows.append(tuple(
            '' if v is None else str(v)
            for v in row
        ) + (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))

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

    # Crear tabla si no existe
    crear_tabla_z_cuestionario_braden(cursor_mysql)
    conn_mysql.commit()
    logging.info("Tabla z_cuestionario_braden verificada/creada.")

    # Truncar tabla
    cursor_mysql.execute("TRUNCATE TABLE z_cuestionario_braden")
    conn_mysql.commit()
    logging.info("Tabla truncada.")

    # =========================
    # Insertar datos
    # =========================
    insert_query = """
        INSERT INTO z_cuestionario_braden VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
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
