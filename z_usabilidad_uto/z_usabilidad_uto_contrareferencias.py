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
        logging.FileHandler("logs/z_usabilidad_uto_contrarreferencias.log"),
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
    if not jdbc_driver_name or not jdbc_driver_loc:
        fail_and_exit("JDBC_DRIVER_NAME o JDBC_DRIVER_PATH no configurados.")

    if not iris_connection_string or not iris_user or not iris_password:
        fail_and_exit("Variables IRIS no configuradas correctamente.")

    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        fail_and_exit("Variables MySQL no configuradas correctamente.")

    if not jpype.isJVMStarted():
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            "-Djava.class.path=" + jdbc_driver_loc
        )

    try:
        conn_iris = jaydebeapi.connect(
            jdbc_driver_name,
            iris_connection_string,
            {'user': iris_user, 'password': iris_password},
            jdbc_driver_loc
        )
    except Exception as e:
        fail_and_exit(f"Error conectando a IRIS: {e}")

    query = ''' 
        SELECT top 100
            PAADM_ADMNO "nro_episodio",
            PAADM_PAPMI_DR->PAPMI_no "nro_registro",
            (SELECT TOP 1
                APPT_AS_ParRef->AS_RES_ParRef->RES_CTLOC_DR->CTLOC_desc
                FROM RB_APPOINTMENT 
                WHERE 
                APPT_AS_ParRef->AS_Date <= PAADM_DischgDate  
                and APPT_PAPMI_DR = PAADM_PAPMI_DR
                and APPT_Adm_DR = PAADM_RowID 
                order by APPT_AS_ParRef->AS_Date desc 
                ) as "local",
            (SELECT TOP 1
                APPT_AS_ParRef->AS_RES_ParRef->RES_Code
                FROM RB_APPOINTMENT 
                WHERE 
                APPT_AS_ParRef->AS_Date <= PAADM_DischgDate  
                and APPT_PAPMI_DR = PAADM_PAPMI_DR
                and APPT_Adm_DR = PAADM_RowID 
                order by APPT_AS_ParRef->AS_Date desc 
                ) as "cod_recurso_cita",
            (SELECT TOP 1
                APPT_AS_ParRef->AS_RES_ParRef->RES_Desc
                FROM RB_APPOINTMENT 
                WHERE 
                APPT_AS_ParRef->AS_Date <= PAADM_DischgDate  
                and APPT_PAPMI_DR = PAADM_PAPMI_DR
                and APPT_Adm_DR = PAADM_RowID 
                order by APPT_AS_ParRef->AS_Date desc 
                ) as "recurso_cita",
            (SELECT TOP 1
                CONVERT (varchar,APPT_AS_ParRef->AS_Date,105)
                FROM RB_APPOINTMENT 
                WHERE 
                APPT_AS_ParRef->AS_Date <= PAADM_DischgDate  
                and APPT_PAPMI_DR = PAADM_PAPMI_DR    
                and APPT_Adm_DR = PAADM_RowID 
                order by APPT_AS_ParRef->AS_Date desc 
                ) as "fecha_cita",
            (SELECT TOP 1
                (CASE
                    WHEN APPT_status = 'P' then 'Agendado'
                    WHEN APPT_status = 'D' then 'Atendido'
                    WHEN APPT_status = 'X' then 'Cancelado'
                    WHEN APPT_status = 'A' then 'Llegó'
                    WHEN APPT_status = 'N' then 'No atendido'
                    WHEN APPT_status = 'T' then 'Transferido'
                    WHEN APPT_status = 'H' then 'En Espera'
                    ELSE APPT_status end)
                FROM RB_APPOINTMENT 
                WHERE 
                APPT_AS_ParRef->AS_Date <= PAADM_DischgDate  
                and APPT_PAPMI_DR = PAADM_PAPMI_DR    
                and APPT_Adm_DR = PAADM_RowID 
                order by APPT_AS_ParRef->AS_Date desc 
                ) as "estado_cita",
            CONVERT(VARCHAR,PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_Date,105) "fecha_contrarref",
            PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->PA_DischargeSummaryRevStat->REVSTAT_ReviewStatus_DR->CLSRS_Desc "etapa_contrarreferencia",
            PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_ModeOfSeparation_DR->CTDSP_Desc  "etapa_alta_amb",
            PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_DischargeDestination_DR->DDEST_Desc "destino_alta",
            PAADM_WaitList_DR->WL_NO "nro_lista_espera",
            PAADM_WaitList_DR->WL_RefHosp_DR->CTRFC_Desc "establecimiento_origen",
            PAADM_WaitList_DR->WL_Hospital_DR->HOSP_Desc "establecimiento_destino",
            PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_PrincipalDiagnosis "diagnostico_cr",
            PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_ICDCode_DR->mrcid_desc  "diagnostico",
            PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_DiagStat_DR->DSTAT_Desc  "etapa_ges",
            PAADM_MAINMRADM_DR->MR_Diagnos->MR_DiagType->TYP_MRCDiagTyp->DTYP_Desc "tipo_diagnostico",
            (case when PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Approximate = 'Y' THEN 'Si'
            when PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Approximate = 'N' THEN 'No' end)"diagnostico_principal",
            PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_Procedures "tratamiento_recibido",
            PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_ClinicalOpinion "indicaciones_al_alta",
            PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_CareProv_DR->CTPCP_Desc "nombre_profesional_regitra",
            PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_CareProv_DR->CTPCP_Code "rut_profesional_registra",
            PAADM_WaitList_DR->WL_CTLOC_DR->CTLOC_DEP_DR->DEP_desc "especialidad_le",
            PAADM_WaitList_DR->WL_CTLOC_DR->CTLOC_hospital_dr->hosp_Desc "referido_por_el_establecimiento_le",
            PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_TextBox2 "referido_por_el_establecimiento_contrarreferencia",
            (CASE WHEN PAADM_RefClinTo_DR->CTRFC_Desc IS NULL THEN PAADM_WaitList_DR->WL_RefHosp_DR->CTRFC_Desc ELSE PAADM_RefClinTo_DR->CTRFC_Desc END) "referido_al_establecimiento_contrarreferencia",
            PAADM_RefClinic_DR->CTRFC_Desc "referido_por_alta_amb",
            PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_UpdateUser_DR->SSUSR_Initials "cod_usuario_actualiza",
            PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_UpdateUser_DR->SSUSR_name "usuario_actualiza"
            FROM PA_ADM
            WHERE PAADM_HOSPITAL_DR = 10448
            AND PAADM_AdmDate>='2025-04-23'
            -- AND PAADM_DepCode_DR IN (4070,4994)
            AND PAADM_RowID > 0
            AND PAADM_TYPE='O'
            AND PAADM_ADMDATE IS NOT NULL
            AND PAADM_ADMTIME IS NOT NULL
            AND PAADM_VisitStatus IS NOT NULL
            AND PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->PA_DischargeSummaryRevStat->REVSTAT_ReviewStatus_DR in (5,6)
            AND PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_ModeOfSeparation_DR->CTDSP_Desc = 'Contrareferencia'
            AND PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_Date >= '2025-04-23';
    '''

    cursor_iris = conn_iris.cursor()
    try:
        cursor_iris.execute(query)
        rows = cursor_iris.fetchall()
    except Exception as e:
        fail_and_exit(f"Error ejecutando consulta IRIS: {e}")

    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))

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

    try:
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_uto_contrarreferencias")
        conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error al truncar MySQL: {e}")

    insert_query = """
        INSERT INTO z_usabilidad_uto_contrarreferencias (
            nro_episodio,
            nro_registro,
            local,
            cod_recurso_cita,
            recurso_cita,
            fecha_cita,
            estado_cita,
            fecha_contrarref,
            etapa_contrarreferencia,
            etapa_alta_amb,
            destino_alta,
            nro_lista_espera,
            establecimiento_origen,
            establecimiento_destino,
            diagnostico_cr,
            diagnostico,
            etapa_ges,
            tipo_diagnostico,
            diagnostico_principal,
            tratamiento_recibido,
            Indicaciones_al_alta,
            nombre_profesional_regitra,
            rut_profesional_registra,
            especialidad_le,
            referido_por_el_establecimiento_le,
            referido_por_el_establecimiento_contrarreferencia,
            referido_al_establecimiento_contrarreferencia,
            referido_por_alta_amb,
            cod_usuario_actualiza,
            usuario_actualiza,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s
        )
    """

    try:
        for i in range(0, len(formatted_rows), 1000):
            chunk = formatted_rows[i:i+1000]
            cursor_mysql.executemany(insert_query, chunk)
            conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error insertando en MySQL: {e}")

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
