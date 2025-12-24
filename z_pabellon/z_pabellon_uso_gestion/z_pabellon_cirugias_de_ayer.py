# Cirugías del día anterior acumuladas

import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

# ============================================================
# CARGA DE ENTORNO Y LOGS
# ============================================================

load_dotenv(override=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_pabellon_cirugias_de_ayer.log"),
        logging.StreamHandler()
    ]
)

# ============================================================
# VARIABLES DE ENTORNO
# ============================================================

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

# ============================================================
# FUNCIÓN: CREAR TABLA SI NO EXISTE (CON fechaActualizacion)
# ============================================================

def crear_tabla_si_no_existe(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS z_pabellon_cirugias_de_ayer (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,       
            IDTRAK VARCHAR(255),
            Establecimiento VARCHAR(255),
            Episodio VARCHAR(255),
            Tipo_Episodio VARCHAR(255),
            Numero_Registro VARCHAR(255),
            Apellido_Paterno VARCHAR(255),
            Apellido_Materno VARCHAR(255),
            Nombre VARCHAR(255),
            Paciente VARCHAR(255),
            RUT VARCHAR(255),
            Codigo_Cirugia VARCHAR(255),
            Descripcion_Cirugia VARCHAR(255),
            Tipo_Cirugia VARCHAR(255),
            Diagnostico VARCHAR(255),
            Tiempo_Estimado VARCHAR(255),
            Lista_Espera VARCHAR(255),
            Suspension VARCHAR(255),
            Area VARCHAR(255),
            Pabellon VARCHAR(255),
            Fecha VARCHAR(255),
            Hora VARCHAR(255),
            Cirujano VARCHAR(255),
            Especialidad VARCHAR(255),
            Cirugia_Ambulatoria VARCHAR(255),
            Cirugia_Condicional VARCHAR(255),
            GES VARCHAR(255),
            Estado VARCHAR(255),
            fechaActualizacion DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
    """)

# ============================================================
# MAIN
# ============================================================

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # --------------------------------------------------------
    # VALIDACIONES
    # --------------------------------------------------------
    if not jdbc_driver_name or not jdbc_driver_loc:
        raise ValueError("JDBC no configurado")
    if not iris_connection_string or not iris_user or not iris_password:
        raise ValueError("IRIS no configurado")
    if not mysql_host or not mysql_user or not mysql_password or not mysql_database:
        raise ValueError("MySQL no configurado")

    # --------------------------------------------------------
    # INICIAR JVM
    # --------------------------------------------------------
    if not jpype.isJVMStarted():
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            "-Djava.class.path=" + jdbc_driver_loc
        )

    # --------------------------------------------------------
    # CONEXIÓN IRIS
    # --------------------------------------------------------
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )
    cursor_iris = conn_iris.cursor()

    # --------------------------------------------------------
    # QUERY IRIS
    # --------------------------------------------------------
    query = """
        SELECT
            String(RB_OperatingRoom.RBOP_RowId),
            RBOP_PAADM_DR->PAADM_Hospital_DR->HOSP_Desc,
            RBOP_PAADM_DR->PAADM_ADMNo,
            CASE RBOP_PAADM_DR->PAADM_Type
                WHEN 'I' THEN 'Hospitalizado'
                WHEN 'O' THEN 'Ambulatorio'
                WHEN 'E' THEN 'Urgencia'
            END,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_No,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name3,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name2,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name2 || ' ' ||
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name3 || ' ' ||
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name,
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_ID,
            RB_OperatingRoom.RBOP_Operation_DR->OPER_Code,
            RB_OperatingRoom.RBOP_Operation_DR->OPER_Desc,
            CASE RBOP_BookingType
                WHEN 'EL'  THEN 'Cirugía Electiva'
                WHEN 'ENP' THEN 'Cirugía Electiva No Programada'
                WHEN 'EM'  THEN 'Cirugía Urgencia'
            END,
            RBOP_PreopDiagn_DR->MRCID_Desc,
            RBOP_EstimatedTime,
            RBOP_WaitLIST_DR->WL_NO,
            RBOP_ReasonSuspend_DR->SUSP_Desc,
            RBOP_Resource_DR->RES_CTLOC_DR->CTLOC_DESC,
            RBOP_Resource_DR->RES_Desc,
            CONVERT(VARCHAR, RB_OperatingRoom.RBOP_DateOper, 105),
            %EXTERNAL(RB_OperatingRoom.RBOP_TimeOper),
            RBOP_Surgeon_DR->CTPCP_Desc,
            RBOP_OperDepartment_DR->CTLOC_Desc,
            RBOP_DaySurgery,
            RBOP_PreopTestDone,
            RBOP_YesNo3,
            CASE RBOP_Status
                WHEN 'B'  THEN 'AGENDADO'
                WHEN 'N'  THEN 'NO LISTO'
                WHEN 'P'  THEN 'POSTERGADO'
                WHEN 'D'  THEN 'REALIZADO'
                WHEN 'A'  THEN 'RECEPCIONADO'
                WHEN 'R'  THEN 'SOLICITADO'
                WHEN 'X'  THEN 'SUSPENDIDO'
                WHEN 'DP' THEN 'SALIDA'
                WHEN 'CL' THEN 'CERRADO'
                WHEN 'C'  THEN 'CONFIRMADO'
                WHEN 'SF' THEN 'ENVIADO POR'
                WHEN 'SK' THEN 'ENVIADO POR RECONOCIDO'
            END
        FROM RB_OperatingRoom
        WHERE RBOP_PAADM_DR->PAADM_Hospital_DR = '10448'
          AND RB_OperatingRoom.RBOP_DateOper = CURRENT_DATE
        ORDER BY RBOP_Resource_DR->RES_Desc
    """

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # --------------------------------------------------------
    # FORMATEO
    # --------------------------------------------------------
    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        formatted_rows.append(tuple(valores + [datetime.now()]))

    # --------------------------------------------------------
    # MYSQL
    # --------------------------------------------------------
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    crear_tabla_si_no_existe(cursor_mysql)
    conn_mysql.commit()

    insert_query = """
        INSERT INTO z_pabellon_cirugias_de_ayer (
            IDTRAK, Establecimiento, Episodio, Tipo_Episodio, Numero_Registro,
            Apellido_Paterno, Apellido_Materno, Nombre, Paciente, RUT,
            Codigo_Cirugia, Descripcion_Cirugia, Tipo_Cirugia, Diagnostico,
            Tiempo_Estimado, Lista_Espera, Suspension, Area, Pabellon,
            Fecha, Hora, Cirujano, Especialidad, Cirugia_Ambulatoria,
            Cirugia_Condicional, GES, Estado, fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
    """

    for i in range(0, len(formatted_rows), 1000):
        cursor_mysql.executemany(insert_query, formatted_rows[i:i + 1000])
        conn_mysql.commit()

    logging.info("Datos transferidos exitosamente.")

except Exception as e:
    logging.error(f"Error: {e}", exc_info=True)

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
