import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
import sys
from datetime import datetime
from cryptography.fernet import Fernet

# ============================================================
# CONFIGURACIÓN PRINCIPAL + LOGGING
# ============================================================

load_dotenv(override=True)

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_usabilidad_oftalmologia_procedimientos.log"),
        logging.StreamHandler()
    ]
)

# VARIABLES IRIS
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc  = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# VARIABLES MYSQL
mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = os.getenv('DB_MYSQL_PORT')
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

def fail_and_exit(message):
    """ Registrar error real y terminar con exit code 1 para n8n """
    logging.error(message)
    sys.exit(1)

def encrypt_parity_check(message):
    load_dotenv()
    key = os.getenv('ENCRYPTION_KEY').encode()
    fernet = Fernet(key)
    return fernet.encrypt(message.encode())

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # ============================================================
    # VALIDACIONES DE VARIABLES DE ENTORNO
    # ============================================================
    if not jdbc_driver_name or not jdbc_driver_loc:
        fail_and_exit("JDBC_DRIVER_NAME o JDBC_DRIVER_PATH no configurados.")

    if not iris_connection_string or not iris_user or not iris_password:
        fail_and_exit("Variables IRIS no configuradas correctamente.")

    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        fail_and_exit("Variables MySQL no configuradas correctamente.")

    # ============================================================
    # INICIAR JVM
    # ============================================================
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # ============================================================
    # CONEXIÓN A IRIS
    # ============================================================
    logging.info("Conectando a InterSystems IRIS...")

    try:
        conn_iris = jaydebeapi.connect(
            jdbc_driver_name,
            iris_connection_string,
            {'user': iris_user, 'password': iris_password},
            jdbc_driver_loc
        )
    except Exception as e:
        fail_and_exit(f"Error conectando a IRIS: {e}")

    cursor_iris = conn_iris.cursor()

    # ============================================================
    # CONSULTA IRIS
    # ============================================================
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
        OEORI_APPT_DR->APPT_AS_ParRef->AS_RES_ParRef->RES_CTLOC_DR in (
            2869,2871,2872,2873,2874,2875,2680,3437,3850,
            4260,4254,4264,4257,4261,4253,4259,4258,4256,
            4617,4531,4532
        )
    '''

    try:
        cursor_iris.execute(query)
        rows = cursor_iris.fetchall()
    except Exception as e:
        fail_and_exit(f"Error ejecutando consulta IRIS: {e}")

    # ============================================================
    # FORMATEAR FILAS
    # ============================================================
    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))

    # ============================================================
    # CONEXIÓN MYSQL
    # ============================================================
    logging.info("Conectando a MySQL...")

    try:
        conn_mysql = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
    except Exception as e:
        fail_and_exit(f"Error conectando a MySQL: {e}")

    cursor_mysql = conn_mysql.cursor()

    # TRUNCATE
    try:
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_oftalmologia_procedimientos")
        conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error al truncar MySQL: {e}")

    # ============================================================
    # INSERTAR EN CHUNKS
    # ============================================================
    insert_query = """
        INSERT INTO z_usabilidad_oftalmologia_procedimientos (
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

    try:
        for i in range(0, len(formatted_rows), 1000):
            chunk = formatted_rows[i:i+1000]
            cursor_mysql.executemany(insert_query, chunk)
            conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error insertando datos en MySQL: {e}")

    logging.info("Datos transferidos exitosamente.")
    sys.exit(0)

except Exception as e:
    fail_and_exit(f"Error inesperado: {e}")

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
