import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
from cryptography.fernet import Fernet

# ============================================================
# CARGA DE ENTORNO Y LOGS
# ============================================================

load_dotenv(override=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_pabellon_uso_gestion_pabellones_estado_agendamiento.log"),
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
# FUNCIÓN: CREAR TABLA SI NO EXISTE
# ============================================================

def crear_tabla_si_no_existe(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS z_pabellon_uso_gestion_pabellones_estado_agendamiento (
        episodio VARCHAR(11),
        fecha_cirugia VARCHAR(10),
        estado_cirugia VARCHAR(12),
        tipo_cirugia VARCHAR(30),
        momento_suspension VARCHAR(29),
        area_qx VARCHAR(46),
        pabellon VARCHAR(15),
        id_cirugia VARCHAR(6),
        motivo_suspencion VARCHAR(97),
        es_festivo VARCHAR(2),
        nombre_festivo VARCHAR(46),
        fecha_ingreso_quirofano VARCHAR(10),
        hora_ingreso_quirofano VARCHAR(8),
        fecha_egreso_quirofano VARCHAR(10),
        hora_egreso_quirofano VARCHAR(8),
        fecha_inicio_cirugia_en_protocolo_anestesico VARCHAR(10),
        hora_inicio_cirugia_protAnest VARCHAR(8),
        fecha_termino_cirugia_en_protocolo_anestesico VARCHAR(10),
        hora_termino_cirugia_en_protocolo_anestesico VARCHAR(8),
        fecha_inicio_cirugia_en_protocolo_operatorio VARCHAR(10),
        hora_inicio_cirugia_en_protocolo_operatorio VARCHAR(8),
        fecha_termino_cirugia_en_protocolo_operatorio VARCHAR(10),
        hora_termino_cirugia_en_protocolo_operatorio VARCHAR(8),
        codigo_cirugia_principal_del_protocolo_pperatorio VARCHAR(16),
        descripcion_cirugia_principal_del_protocolo_operatorio VARCHAR(214),
        RUT_Paciente VARCHAR(10),
        numero_cirugia VARCHAR(11),
        fechaActualizacion VARCHAR(19),
        INDEX idx_episodio (episodio),
        INDEX idx_fecha_cirugia (fecha_cirugia),
        INDEX idx_id_cirugia (id_cirugia)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
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
    query = '''
    SELECT %NOLOCK 
        RBOP.RBOP_PAADM_DR->PAADM_ADMNO AS episodio,
        TO_CHAR(RBOP.RBOP_DateOper, 'DD-MM-YYYY') AS fecha_cirugia,
        CASE RBOP.RBOP_Status
            WHEN 'B'  THEN 'AGENDADO'
            WHEN 'CL' THEN 'CERRADO'
            WHEN 'C'  THEN 'CONFIRMADO'
            WHEN 'SF' THEN 'ENVIADO POR'
            WHEN 'SK' THEN 'ENVIADO POR RECONOCIDO'
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
             AND (ANA.ANA_TheatreInDate IS NULL AND ANA.ANA_CustomDate1 IS NULL)
            THEN 'Suspendida ANTES de iniciar'
            WHEN RBOP.RBOP_Status = 'X'
             AND (ANA.ANA_TheatreInDate IS NOT NULL OR ANA.ANA_CustomDate1 IS NOT NULL)
            THEN 'Suspendida DESPUÉS de iniciar'
            ELSE NULL
        END AS momento_suspension,
        ANAOP.ANAOP_Depar_Oper_DR->CTLOC_Desc AS area_qx,
        RBOP.RBOP_Resource_DR->RES_Desc AS pabellon,
        RBOP.RBOP_RowId AS id_cirugia,
        RBOP.RBOP_ReasonSuspend_DR->SUSP_Desc AS motivo_suspencion,
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
        ANAOP.ANAOP_Type_DR->OPER_Code AS codigo_cirugia_principal_del_protocolo_pperatorio,
        ANAOP.ANAOP_Type_DR->OPER_Desc AS descripcion_cirugia_principal_del_protocolo_operatorio,
        RBOP.RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_ID AS RUT_Paciente,
        ANAOP.ANAOP_No AS numero_cirugia
    FROM RB_OperatingRoom RBOP
    LEFT JOIN OR_Anaesthesia ANA
      ON RBOP.RBOP_PAADM_DR = ANA.ANA_PAADM_ParRef
     AND ANA.ANA_RowId = (
        SELECT MAX(A2.ANA_RowId)
        FROM OR_Anaesthesia A2
        WHERE A2.ANA_PAADM_ParRef = RBOP.RBOP_PAADM_DR
     )
    LEFT JOIN OR_Anaest_Operation ANAOP
      ON ANA.ANA_RowId = ANAOP.ANAOP_Par_Ref
     AND ANAOP.ANAOP_No = (
        SELECT MAX(O2.ANAOP_No)
        FROM OR_Anaest_Operation O2
        WHERE O2.ANAOP_Par_Ref = ANA.ANA_RowId
     )
    LEFT JOIN CT_PubHol HOL
      ON RBOP.RBOP_DateOper = HOL.CTHOL_Code
    WHERE RBOP.RBOP_DateOper >= '2025-01-01'
      AND RBOP.RBOP_DateOper <= CURRENT_DATE
      AND RBOP.RBOP_RowId > 0
      AND RBOP.RBOP_Resource_DR IS NOT NULL
      AND RBOP.RBOP_PAADM_DR->PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448
    ORDER BY fecha_cirugia, pabellon, estado_cirugia;
    '''

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # --------------------------------------------------------
    # FORMATEO
    # --------------------------------------------------------
    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))

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

    cursor_mysql.execute(
        "TRUNCATE TABLE z_pabellon_uso_gestion_pabellones_estado_agendamiento"
    )
    conn_mysql.commit()

    insert_query = """
    INSERT INTO z_pabellon_uso_gestion_pabellones_estado_agendamiento (
        episodio, fecha_cirugia, estado_cirugia, tipo_cirugia, momento_suspension,
        area_qx, pabellon, id_cirugia, motivo_suspencion, es_festivo, nombre_festivo,
        fecha_ingreso_quirofano, hora_ingreso_quirofano, fecha_egreso_quirofano, hora_egreso_quirofano,
        fecha_inicio_cirugia_en_protocolo_anestesico, hora_inicio_cirugia_protAnest,
        fecha_termino_cirugia_en_protocolo_anestesico, hora_termino_cirugia_en_protocolo_anestesico,
        fecha_inicio_cirugia_en_protocolo_operatorio, hora_inicio_cirugia_en_protocolo_operatorio,
        fecha_termino_cirugia_en_protocolo_operatorio, hora_termino_cirugia_en_protocolo_operatorio,
        codigo_cirugia_principal_del_protocolo_pperatorio,
        descripcion_cirugia_principal_del_protocolo_operatorio,
        RUT_Paciente, numero_cirugia, fechaActualizacion
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    for i in range(0, len(formatted_rows), 1000):
        cursor_mysql.executemany(insert_query, formatted_rows[i:i + 1000])
        conn_mysql.commit()

    logging.info("Datos transferidos exitosamente.")

except Exception as e:
    logging.error(f"Error: {e}")

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
