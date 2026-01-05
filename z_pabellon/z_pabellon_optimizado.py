import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_pabellon_optimizado.log"),
        logging.StreamHandler()
    ]
)

jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc  = os.getenv('JDBC_DRIVER_PATH')
iris_conn_str    = os.getenv('CONEXION_STRING')
iris_user        = os.getenv('DB_USER')
iris_password    = os.getenv('DB_PASSWORD')

mysql_cfg = {
    "host": os.getenv('DB_MYSQL_HOST'),
    "port": int(os.getenv('DB_MYSQL_PORT')),
    "user": os.getenv('DB_MYSQL_USER'),
    "password": os.getenv('DB_MYSQL_PASSWORD'),
    "database": os.getenv('DB_MYSQL_DATABASE')
}

def crear_tabla(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS z_pabellon_optimizado (
        ambulatoria VARCHAR(1),
        tipo_de_agendamiento_de_la_cirugia VARCHAR(16),
        tiempo_programado_minutos VARCHAR(3),
        estado_agendamiento VARCHAR(12),
        diagnostico_prequirurgico_lista_de_espera VARCHAR(132),
        ANAOP_Notes VARCHAR(8091),
        estado_protocolo_operatorio VARCHAR(10),
        categoria_cirugia VARCHAR(13),
        codigo_cirugia_principal_del_protocolo_pperatorio VARCHAR(16),
        descripcion_cirugia_principal_del_protocolo_operatorio VARCHAR(214),
        codigo_equipo_quirurgico VARCHAR(17),
        descripcion_equipo_quirurgico VARCHAR(46),
        fecha_inicio_cirugia_en_protocolo_anestesico VARCHAR(10),
        hora_inicio_cirugia_protAnest VARCHAR(8),
        fecha_termino_cirugia_en_protocolo_anestesico VARCHAR(10),
        hora_termino_cirugia_en_protocolo_anestesico VARCHAR(8),
        fecha_inicio_cirugia_en_protocolo_operatorio VARCHAR(10),
        hora_inicio_cirugia_en_protocolo_operatorio VARCHAR(8),
        fecha_termino_cirugia_en_protocolo_operatorio VARCHAR(10),
        hora_termino_cirugia_en_protocolo_operatorio VARCHAR(8),
        sitio_operacion_principal VARCHAR(35),
        RUT_cirujano_principal VARCHAR(11),
        RUT_Cirujano_2 VARCHAR(12),
        tipo_anestesia VARCHAR(49),
        fecha_agendamiento VARCHAR(10),
        tipo_episodio VARCHAR(1),
        cirujano_principal VARCHAR(42),
        episodio VARCHAR(11),
        fecha_ingreso_quirofano VARCHAR(10),
        hora_ingreso_quirofano VARCHAR(8),
        fecha_egreso_quirofano VARCHAR(10),
        hora_egreso_quirofano VARCHAR(8),
        motivo_suspension VARCHAR(97),
        codigos_cirugia_secundaria VARCHAR(16),
        descripcion_cirugia_secundaria VARCHAR(214),
        tiempo_uso_quirofano_minutos VARCHAR(13),
        paciente_condicional VARCHAR(1),
        RBOP_TimeOper VARCHAR(8),
        RBOP_RowId VARCHAR(6),
        fechaActualizacion VARCHAR(19)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

conn_iris = conn_mysql = None
cursor_iris = cursor_mysql = None

try:
    # JVM
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), f"-Djava.class.path={jdbc_driver_loc}")

    # IRIS
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_conn_str,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )
    cursor_iris = conn_iris.cursor()

    query = """
    SELECT %nolock
        RBOP_DaySurgery,
        RBOP_BookingType,
        RBOP_EstimatedTime,
        RBOP_Status,
        RBOP_PreopDiagn_DR->MRCID_DESC,
        OR_Anaest_Operation.ANAOP_Notes,
        ANAOP_Status,
        ANAOP_OperType,
        ANAOP_Type_DR->OPER_Code,
        ANAOP_Type_DR->OPER_Desc,
        ANAOP_Depar_Oper_DR->CTLOC_Code,
        ANAOP_Depar_Oper_DR->CTLOC_Desc,
        convert(varchar,ANA_CustomDate1,105),
        convert(varchar,ANA_CustomTime1,108),
        convert(varchar,ANA_CustomDate2,105),
        convert(varchar,ANA_CustomTime2,108),
        convert(varchar,ANAOP_OpStartDate,105),
        convert(varchar,ANAOP_OpStartTime,108),
        convert(varchar,ANAOP_OpEndDate,105),
        convert(varchar,ANAOP_OpEndTime,108),
        ANAOP_BodySite_DR->BODS_Desc,
        ANAOP_Surgeon_DR->CTPCP_Code,
        ANAOP_SecondSurgeon_DR->CTPCP_Code,
        ANAOP_Par_Ref->ANA_Method->ANMET_Desc,
        convert(varchar,RBOP_DateOper,105),
        RBOP_PAADM_DR->PAADM_TYPE,
        ANAOP_Surgeon_DR->CTPCP_Desc,
        RBOP_PAADM_DR->PAADM_ADMNO,
        ANA_TheatreInDate,
        ANA_TheatreInTime,
        ANA_TheatreOutDate,
        ANA_TheatreOutTime,
        RBOP_ReasonSuspend_DR->SUSP_Desc,
        SECPR_Operation_DR->OPER_Code,
        SECPR_Operation_DR->OPER_Desc,
        ANA_Anest_Duration,
        RBOP_PreopTestDone,
        RBOP_TimeOper,
        RBOP_RowId
    FROM RB_OperatingRoom
    LEFT JOIN OR_Anaesthesia ON RBOP_PAADM_DR = ANA_PAADM_ParRef
    LEFT JOIN OR_Anaest_Operation ON ANA_RowId = ANAOP_Par_Ref
    LEFT JOIN OR_An_Oper_SecondaryProc ON ANAOP_RowId = SECPR_ParRef
    WHERE ANA_TheatreInDate >= '2025-01-01'
      AND RBOP_PAADM_DR->PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448
    """

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    formatted_rows = [
        tuple('' if v is None else str(v) for v in r) +
        (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
        for r in rows
    ]

    # MySQL
    conn_mysql = mysql.connector.connect(**mysql_cfg)
    cursor_mysql = conn_mysql.cursor()

    crear_tabla(cursor_mysql)
    conn_mysql.commit()
    logging.info("Tabla z_pabellon_optimizado verificada/creada.")

    cursor_mysql.execute("TRUNCATE TABLE z_pabellon_optimizado")
    conn_mysql.commit()

    insert_sql = """
    INSERT INTO z_pabellon_optimizado VALUES (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
    )
    """

    cursor_mysql.executemany(insert_sql, formatted_rows)
    conn_mysql.commit()

    logging.info("Carga finalizada correctamente.")

except Exception as e:
    logging.error(f"Error: {e}")

finally:
    for obj in [cursor_iris, conn_iris, cursor_mysql, conn_mysql]:
        try:
            if obj:
                obj.close()
        except Exception:
            pass
    try:
        if jpype.isJVMStarted():
            jpype.shutdownJVM()
    except Exception:
        pass
