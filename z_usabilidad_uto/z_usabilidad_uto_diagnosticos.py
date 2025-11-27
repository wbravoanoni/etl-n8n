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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_usabilidad_uto_diagnosticos.log"),
        logging.StreamHandler()
    ]
)

jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')

iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = os.getenv('DB_MYSQL_PORT')
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

def fail_and_exit(message):
    logging.error(message)
    sys.exit(1)

def encrypt_parity_check(message):
    key = os.getenv('ENCRYPTION_KEY').encode()
    fernet = Fernet(key)
    return fernet.encrypt(message.encode())

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # VALIDAR VARIABLES
    if not jdbc_driver_name or not jdbc_driver_loc:
        fail_and_exit("JDBC_DRIVER_NAME o JDBC_DRIVER_PATH no configurados.")

    if not iris_connection_string or not iris_user or not iris_password:
        fail_and_exit("Variables IRIS no configuradas correctamente.")

    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        fail_and_exit("Variables MySQL no configuradas correctamente.")

    # INICIAR JVM
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # CONEXIÓN IRIS
    try:
        conn_iris = jaydebeapi.connect(
            jdbc_driver_name,
            iris_connection_string,
            {'user': iris_user, 'password': iris_password},
            jdbc_driver_loc
        )
    except Exception as e:
        fail_and_exit(f"Error conectando a IRIS: {e}")

    # CONSULTA
    query = ''' 
        SELECT
        APPT_Adm_DR->PAADM_ADMNO "nro_episodio",
        APPT_PAPMI_DR->PAPMI_No AS "nro_registro",
        APPT_PAPMI_DR->PAPMI_ID "run",
        APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name2 "nombres",
        APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name "apellido_paterno",
        APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name3 "apellido_materno",
        APPT_PAPMI_DR->papmi_paper_dr->paper_ageyr "edad",
        APPT_WaitList_DR->WL_NO "nro_le",
        APPT_Adm_DR->PAADM_DepCode_DR->CTLOC_DEP_DR->DEP_desc "descripcion_grupo_departamento",
        APPT_AS_ParRef->AS_RES_ParRef->RES_CTLOC_DR->CTLOC_desc "descripcion_local_agendamiento",
        APPT_AS_ParRef->AS_RES_ParRef->RES_Desc as "descripcion_recurso",
        APPT_RBCServ_DR->SER_ARCIM_DR->ARCIM_code as "codigo_prestacion_agendada",
        APPT_RBCServ_DR->SER_ARCIM_DR->ARCIM_desc as "descripcion_prestacion_agendada",
        convert(varchar,APPT_DateComp,105) as "fecha_cita",
        convert(varchar,APPT_TimeComp,108) as "hora_cita",
        CONVERT(VARCHAR,APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Date,105) "fecha_creacion_diagnostico",
        CONVERT(VARCHAR,APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Time,108) "hora_creacion_diagnostico",
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_UserCreated_DR->ssusr_initials "codigo_usuario_registra_diagnostico",
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_UserCreated_DR->ssusr_name "descripcion_usuario_registra_diagnostico",
        CONVERT(VARCHAR,APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_UpdateDate,105) "fecha_actualizacion",
        CONVERT(VARCHAR,APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_UpdateTime,108) "hora_actualizacion",
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_UpdateUser_DR->ssusr_initials "codigo_usuario_actualiza_diagnostico",
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_UpdateUser_DR->ssusr_name "descripcion_usuario_actualiza_diagnostico",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_ICDCode_DR->MRCID_code "Codigo Diagnostico",
        APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_ICDCode_DR->MRCID_desc "descripcion_diagnostico",
        (CASE
            when APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_suspicion = 'Y' then 'Si'
            when APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_suspicion = 'N' then 'No'
            else APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_suspicion end
        ) as ges,
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MR_DiagType->TYP_MRCDiagTyp->DTYP_Code "codigo_tipo_diagnostico",
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MR_DiagType->TYP_MRCDiagTyp->DTYP_Desc "descripcipon_tipo_diagnostico",
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_DiagStat_DR->DSTAT_code "codigo_etapa_GES",
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_DiagStat_DR->DSTAT_Desc "descripcion_etapa_ges",
        (CASE
            when APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Approximate = 'Y' then 'Si'
            when APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Approximate = 'N' then 'No'
            else APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Approximate end
        ) as "diagnostico_principal",
        (CASE
            when APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_DIAGNOS->MRDIA_Active = 'Y' then 'Si'
            when APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_DIAGNOS->MRDIA_Active = 'N' then 'No'
            else APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_DIAGNOS->MRDIA_Active end
        ) as "diagnostico_activo",
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_DeletionReason_DR->RCH_Desc "motivo_inactivacion",
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Desc "fundamento_y_complemento_del_diagnostico",
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Laterality_DR->LATER_Desc "lateridad",
        APPT_Adm_DR->PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Severity_DR->SEV_desc "severidad"
        FROM RB_Appointment
        WHERE
            APPT_DateComp >= '2025-04-23'
            and APPT_AS_ParRef->AS_RES_ParRef->RES_CTLOC_DR in (4070,4994)
            and APPT_TimeComp > 0
            AND APPT_STATUS <> 'X'
            and APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Date = APPT_DateComp
            and APPT_Adm_DR->PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448;
    '''

    cursor_iris = conn_iris.cursor()
    try:
        cursor_iris.execute(query)
        rows = cursor_iris.fetchall()
    except Exception as e:
        fail_and_exit(f"Error ejecutando consulta IRIS: {e}")

    # FORMATEAR RESULTADOS
    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))

    # CONEXIÓN MYSQL
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
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_uto_diagnosticos")
        conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error al truncar MySQL: {e}")

    # INSERT
    insert_query = """
        INSERT INTO z_usabilidad_uto_diagnosticos (
            nro_episodio, nro_registro, run, nombres, apellido_paterno,
            apellido_materno, edad, nro_le, descripcion_grupo_departamento,
            descripcion_local_agendamiento, descripcion_recurso,
            codigo_prestacion_agendada, descripcion_prestacion_agendada,
            fecha_cita, hora_cita, fecha_creacion_diagnostico,
            hora_creacion_diagnostico, codigo_usuario_registra_diagnostico,
            descripcion_usuario_registra_diagnostico, fecha_actualizacion,
            hora_actualizacion, codigo_usuario_actualiza_diagnostico,
            descripcion_usuario_actualiza_diagnostico, codigo_diagnostico,
            descripcion_diagnostico, ges, codigo_tipo_diagnostico,
            descripcipon_tipo_diagnostico, codigo_etapa_GES,
            descripcion_etapa_ges, diagnostico_principal, diagnostico_activo,
            motivo_inactivacion, fundamento_y_complemento_del_diagnostico,
            lateridad, severidad, fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
    """

    try:
        for i in range(0, len(formatted_rows), 1000):
            chunk = formatted_rows[i:i + 1000]
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
