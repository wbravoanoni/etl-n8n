import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
import sys
from datetime import datetime
from cryptography.fernet import Fernet

load_dotenv(override=True)

os.makedirs("logs", exist_ok=True)

# Configurar logging para que también imprima en la consola
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("logs/z_usabilidad_coloproctologia_agendas_diagnosticos.log"),
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

def fail_and_exit(message):
    logging.error(message)
    sys.exit(1)

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # Validar variables de entorno
    if not jdbc_driver_name or not jdbc_driver_loc:
        fail_and_exit("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
    if not iris_connection_string or not iris_user or not iris_password:
        fail_and_exit("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        fail_and_exit("Las variables de entorno de MySQL no están configuradas correctamente.")
    
    # Iniciar JVM si no está ya iniciada
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # Crear conexión con InterSystems IRIS
    try:
        conn_iris = jaydebeapi.connect(
            jdbc_driver_name,
            iris_connection_string,
            {'user': iris_user, 'password': iris_password},
            jdbc_driver_loc
        )
    except Exception as e:
        fail_and_exit(f"Error conectando a IRIS: {e}")

    # Consulta SQL para obtener datos
    query = ''' 
    SELECT
        APPT_Adm_DR->PAADM_ADMNO AS "nro_episodio",
        APPT_PAPMI_DR->PAPMI_No AS "nro_registro",
        APPT_PAPMI_DR->PAPMI_ID AS "run",
        APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name2 AS "nombres",
        APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name AS "apellido_paterno",
        APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name3 AS "apellido_materno",
        APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_AgeYr AS "edad",
        APPT_WaitList_DR->WL_NO AS "nro_le",
        APPT_Adm_DR->PAADM_DepCode_DR->CTLOC_DEP_DR->DEP_Desc AS "descripcion_grupo_departamento",
        APPT_AS_ParRef->AS_RES_ParRef->RES_CTLOC_DR->CTLOC_Desc AS "descripcion_local_agendamiento",
        APPT_AS_ParRef->AS_RES_ParRef->RES_Desc AS "descripcion_recurso",
        APPT_RBCServ_DR->SER_ARCIM_DR->ARCIM_Code AS "codigo_prestacion_agendada",
        APPT_RBCServ_DR->SER_ARCIM_DR->ARCIM_Desc AS "descripcion_prestacion_agendada",
        CONVERT(VARCHAR, APPT_DateComp, 105) AS "fecha_cita",
        CONVERT(VARCHAR, APPT_TimeComp, 108) AS "hora_cita",
        CONVERT(VARCHAR, APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Date, 105) AS "fecha_creacion_diagnostico",
        CONVERT(VARCHAR, APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Time, 108) AS "hora_creacion_diagnostico",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_UserCreated_DR->SSUSR_Initials AS "codigo_usuario_registra_diagnostico",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_UserCreated_DR->SSUSR_Name AS "descripcion_usuario_registra_diagnostico",
        CONVERT(VARCHAR, APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_UpdateDate, 105) AS "fecha_actualizacion",
        CONVERT(VARCHAR, APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_UpdateTime, 108) AS "hora_actualizacion",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_UpdateUser_DR->SSUSR_Initials AS "codigo_usuario_actualiza_diagnostico",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_UpdateUser_DR->SSUSR_Name AS "descripcion_usuario_actualiza_diagnostico",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_ICDCode_DR->MRCID_Code AS "codigo_diagnostico",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_ICDCode_DR->MRCID_Desc AS "descripcion_diagnostico",
        CASE
            WHEN APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Suspicion = 'Y' THEN 'Si'
            WHEN APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Suspicion = 'N' THEN 'No'
            ELSE APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Suspicion
        END AS "ges",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MR_DiagType->TYP_MRCDiagTyp->DTYP_Code AS "codigo_tipo_diagnostico",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MR_DiagType->TYP_MRCDiagTyp->DTYP_Desc AS "descripcipon_tipo_diagnostico",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_DiagStat_DR->DSTAT_Code AS "codigo_etapa_GES",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_DiagStat_DR->DSTAT_Desc AS "descripcion_etapa_ges",
        CASE
            WHEN APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Approximate = 'Y' THEN 'Si'
            WHEN APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Approximate = 'N' THEN 'No'
            ELSE APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Approximate
        END AS "diagnostico_principal",
        CASE
            WHEN APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Active = 'Y' THEN 'Si'
            WHEN APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Active = 'N' THEN 'No'
            ELSE APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Active
        END AS "diagnostico_activo",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_DeletionReason_DR->RCH_Desc AS "motivo_inactivacion",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Desc AS "fundamento_y_complemento_del_diagnostico",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Laterality_DR->LATER_Desc AS "lateridad",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Severity_DR->SEV_Desc AS "severidad"	
    FROM
        RB_Appointment
    WHERE
        APPT_DateComp >='2025-10-01' AND APPT_DateComp <= CURRENT_TIMESTAMP
        AND APPT_Adm_DR->PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448
        AND APPT_AS_ParRef->AS_RES_ParRef->RES_CTLOC_DR = 2831
        AND APPT_Status <> 'X'
        AND APPT_TimeComp > 0
        AND APPT_AS_ParRef->AS_RES_ParRef->RES_RowId > 0 
        AND APPT_AS_ParRef->AS_ChildSub > 0 
        AND APPT_ChildSub > 0
        AND APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Date = APPT_DateComp;
    '''

    cursor_iris = conn_iris.cursor()
    try:
        cursor_iris.execute(query)
        rows = cursor_iris.fetchall()
    except Exception as e:
        fail_and_exit(f"Error ejecutando consulta IRIS: {e}")

    # Convertir filas a formato adecuado para MySQL
    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))
        
    # Conectar a MySQL
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

    # Truncar la tabla en MySQL
    try:
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_coloproctologia_agendas_diagnosticos")
        conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error al truncar tabla MySQL: {e}")

    # Insertar datos
    insert_query = """
        INSERT INTO z_usabilidad_coloproctologia_agendas_diagnosticos (
        nro_episodio,
        nro_registro,
        run,
        nombres,
        apellido_paterno,
        apellido_materno,
        edad,
        nro_le,
        descripcion_grupo_departamento,
        descripcion_local_agendamiento,
        descripcion_recurso,
        codigo_prestacion_agendada,
        descripcion_prestacion_agendada,
        fecha_cita,
        hora_cita,
        fecha_creacion_diagnostico,
        hora_creacion_diagnostico,
        codigo_usuario_registra_diagnostico,
        descripcion_usuario_registra_diagnostico,
        fecha_actualizacion,
        hora_actualizacion,
        codigo_usuario_actualiza_diagnostico,
        descripcion_usuario_actualiza_diagnostico,
        codigo_diagnostico,
        descripcion_diagnostico,
        ges,
        codigo_tipo_diagnostico,
        descripcipon_tipo_diagnostico,
        codigo_etapa_GES,
        descripcion_etapa_ges,
        diagnostico_principal,
        diagnostico_activo,
        motivo_inactivacion,
        fundamento_y_complemento_del_diagnostico,
        lateridad,
        severidad,
        fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
    """

    try:
        chunk_size = 1000
        for i in range(0, len(formatted_rows), chunk_size):
            chunk = formatted_rows[i:i + chunk_size]
            cursor_mysql.executemany(insert_query, chunk)
            conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error insertando registros en MySQL: {e}")

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
