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
        logging.FileHandler("logs/z_cuestionario_downtown.log"),
        logging.StreamHandler()
    ]
)

# =========================
# Crear tabla si no existe
# =========================
def crear_tabla_z_cuestionario_downtown(cursor_mysql):
    cursor_mysql.execute("""
    CREATE TABLE IF NOT EXISTS z_cuestionario_downtown (
        ID VARCHAR(6),
        fecha_creacion VARCHAR(10),
        hora_creacion VARCHAR(8),
        puntaje VARCHAR(2),
        QUESCreateUserDR VARCHAR(5),
        usuario_creador VARCHAR(40),
        episodio VARCHAR(11),
        local_actual VARCHAR(50),

        Q01 VARCHAR(2),
        Q02 VARCHAR(119),
        Q03 VARCHAR(57),
        Q04 VARCHAR(9),
        Q05 VARCHAR(22),
        Q06 VARCHAR(15),

        Q08 VARCHAR(20),
        Q09 VARCHAR(20),
        Q10 VARCHAR(20),

        Q11 VARCHAR(32),
        Q12 VARCHAR(2),
        Q13 VARCHAR(10),
        Q14 VARCHAR(7),
        Q15 VARCHAR(24),
        Q16 VARCHAR(42),
        Q17 VARCHAR(21),

        Q20 VARCHAR(20),
        Q21 VARCHAR(20),
        Q22 VARCHAR(20),
        Q23 VARCHAR(20),
        Q24 VARCHAR(20),
        Q25 VARCHAR(20),

        Q30 VARCHAR(20),
        Q31 VARCHAR(20),
        Q32 VARCHAR(20),
        Q33 VARCHAR(20),
        Q34 VARCHAR(20),
        Q35 VARCHAR(20),

        Q40 VARCHAR(20),
        Q41 VARCHAR(20),
        Q42 VARCHAR(20),
        Q43 VARCHAR(20),
        Q44 VARCHAR(20),
        Q45 VARCHAR(20),

        Q46 VARCHAR(359),
        fechaActualizacion VARCHAR(19),

        INDEX idx_episodio (episodio),
        INDEX idx_fecha_creacion (fecha_creacion)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

# =========================
# Variables de entorno
# =========================
jdbc_driver_name = os.getenv("JDBC_DRIVER_NAME")
jdbc_driver_loc = os.getenv("JDBC_DRIVER_PATH")
iris_connection_string = os.getenv("CONEXION_STRING")
iris_user = os.getenv("DB_USER")
iris_password = os.getenv("DB_PASSWORD")

mysql_host = os.getenv("DB_MYSQL_HOST")
mysql_port = int(os.getenv("DB_MYSQL_PORT", 3306))
mysql_user = os.getenv("DB_MYSQL_USER")
mysql_password = os.getenv("DB_MYSQL_PASSWORD")
mysql_database = os.getenv("DB_MYSQL_DATABASE")

conn_iris = conn_mysql = None
cursor_iris = cursor_mysql = None

try:
    # =========================
    # Iniciar JVM
    # =========================
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # =========================
    # Conectar IRIS
    # =========================
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {"user": iris_user, "password": iris_password},
        jdbc_driver_loc
    )
    cursor_iris = conn_iris.cursor()

    cursor_iris.execute("""
        SELECT
            ID,
            QUESCreateDate,
            QUESCreateTime,
            QUESScore,
            QUESCreateUserDR,
            b.SSUSR_Name,
            c.PAADM_ADMNo,
            c.PAADM_CurrentWard_DR->WARD_Desc,
            a.Q01,a.Q02,a.Q03,a.Q04,a.Q05,a.Q06,
            a.Q08,a.Q09,a.Q10,
            a.Q11,a.Q12,a.Q13,a.Q14,a.Q15,a.Q16,a.Q17,
            a.Q20,a.Q21,a.Q22,a.Q23,a.Q24,a.Q25,
            a.Q30,a.Q31,a.Q32,a.Q33,a.Q34,a.Q35,
            a.Q40,a.Q41,a.Q42,a.Q43,a.Q44,a.Q45,
            a.Q46
        FROM questionnaire.QTCEERC a
        INNER JOIN SS_User b ON a.QUESCreateUserDR = b.SSUSR_RowId
        LEFT JOIN PA_Adm c ON a.QUESPAAdmDR = c.PAADM_RowID
        WHERE QUESCreateDate >= DATEADD(MONTH, -6, CURRENT_DATE)
          AND PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448
    """)

    rows = cursor_iris.fetchall()

    formatted_rows = [
        tuple("" if v is None else str(v) for v in row) +
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),)
        for row in rows
    ]

    # =========================
    # Conectar MySQL
    # =========================
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    crear_tabla_z_cuestionario_downtown(cursor_mysql)
    cursor_mysql.execute("TRUNCATE TABLE z_cuestionario_downtown")
    conn_mysql.commit()

    # =========================
    # INSERT dinámico (CLAVE)
    # =========================
    insert_columns = [
        "ID","fecha_creacion","hora_creacion","puntaje","QUESCreateUserDR",
        "usuario_creador","episodio","local_actual",
        "Q01","Q02","Q03","Q04","Q05","Q06",
        "Q08","Q09","Q10",
        "Q11","Q12","Q13","Q14","Q15","Q16","Q17",
        "Q20","Q21","Q22","Q23","Q24","Q25",
        "Q30","Q31","Q32","Q33","Q34","Q35",
        "Q40","Q41","Q42","Q43","Q44","Q45",
        "Q46",
        "fechaActualizacion"
    ]

    placeholders = ", ".join(["%s"] * len(insert_columns))
    columns_sql = ", ".join(insert_columns)

    insert_query = f"""
        INSERT INTO z_cuestionario_downtown ({columns_sql})
        VALUES ({placeholders})
    """

    if formatted_rows:
        if len(formatted_rows[0]) != len(insert_columns):
            raise ValueError(
                f"Descalce INSERT: columnas={len(insert_columns)}, "
                f"valores_fila={len(formatted_rows[0])}"
            )

    cursor_mysql.executemany(insert_query, formatted_rows)
    conn_mysql.commit()

    logging.info("Datos transferidos exitosamente.")

except Exception as e:
    logging.error(f"Error en ejecución: {e}")

finally:
    for obj, name in [
        (cursor_iris, "cursor_iris"),
        (conn_iris, "conn_iris"),
        (cursor_mysql, "cursor_mysql"),
        (conn_mysql, "conn_mysql")
    ]:
        try:
            if obj:
                obj.close()
        except Exception as e:
            logging.warning(f"No se pudo cerrar {name}: {e}")

    try:
        if jpype.isJVMStarted():
            jpype.shutdownJVM()
    except Exception:
        pass
