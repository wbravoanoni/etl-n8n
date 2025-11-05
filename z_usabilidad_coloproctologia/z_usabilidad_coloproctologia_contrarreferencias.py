import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging

from datetime import datetime
from cryptography.fernet import Fernet

load_dotenv(override=True)

# Configurar logging para que también imprima en la consola
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("logs/z_usabilidad_coloproctologia_contrarreferencias.log"),
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

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # Validar variables de entorno
    if not jdbc_driver_name or not jdbc_driver_loc:
        logging.error("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
        raise ValueError("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
    if not iris_connection_string or not iris_user or not iris_password:
        logging.error("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
        raise ValueError("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        logging.error("Las variables de entorno de MySQL no están configuradas correctamente.")
        raise ValueError("Las variables de entorno de MySQL no están configuradas correctamente.")
    
    # Iniciar JVM si no está ya iniciada
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # Crear conexión con InterSystems IRIS
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )

    # Consulta SQL para obtener datos
    query = ''' 
            SELECT
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
            -- AND PAADM_DepCode_DR = 2680
            AND PAADM_AdmDate>='2025-04-23'
            AND PAADM_RowID > 0
            AND PAADM_TYPE='O'
            AND PAADM_ADMDATE IS NOT NULL
            AND PAADM_ADMTIME IS NOT NULL
            AND PAADM_VisitStatus IS NOT NULL
            AND PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_ModeOfSeparation_DR->CTDSP_Desc = 'Contrareferencia'
            AND PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_Date >= '2025-04-23';
        '''
        #   2831 Policlínico de Cirugía-Proctología HDS
        #AND PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->PA_DischargeSummaryRevStat->REVSTAT_ReviewStatus_DR in (5,6)

    # Ejecutar consulta en InterSystems IRIS
    cursor_iris = conn_iris.cursor()
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # Convertir filas a formato adecuado para MySQL
    formatted_rows = []
    for row in rows:
        nro_episodio = '' if row[0] is None else str(row[0])
        nro_registro = '' if row[1] is None else str(row[1])
        local = '' if row[2] is None else str(row[2])
        cod_recurso_cita = '' if row[3] is None else str(row[3])
        recurso_cita = '' if row[4] is None else str(row[4])
        fecha_cita = '' if row[5] is None else str(row[5])
        estado_cita = '' if row[6] is None else str(row[6])
        fecha_contrarref = '' if row[7] is None else str(row[7])
        etapa_contrarreferencia = '' if row[8] is None else str(row[8])
        etapa_alta_amb = '' if row[9] is None else str(row[9])
        destino_alta = '' if row[10] is None else str(row[10])
        nro_lista_espera = '' if row[11] is None else str(row[11])
        establecimiento_origen = '' if row[12] is None else str(row[12])
        establecimiento_destino = '' if row[13] is None else str(row[13])
        diagnostico_cr = '' if row[14] is None else str(row[14])
        diagnostico = '' if row[15] is None else str(row[15])
        etapa_ges = '' if row[16] is None else str(row[16])
        tipo_diagnostico = '' if row[17] is None else str(row[17])
        diagnostico_principal = '' if row[18] is None else str(row[18])
        tratamiento_recibido = '' if row[19] is None else str(row[19])
        Indicaciones_al_alta = '' if row[20] is None else str(row[20])
        nombre_profesional_regitra = '' if row[21] is None else str(row[21])
        rut_profesional_registra = '' if row[22] is None else str(row[22])
        especialidad_le = '' if row[23] is None else str(row[23])
        referido_por_el_establecimiento_le = '' if row[24] is None else str(row[24])
        referido_por_el_establecimiento_contrarreferencia = '' if row[25] is None else str(row[25])
        referido_al_establecimiento_contrarreferencia = '' if row[26] is None else str(row[26])
        referido_por_alta_amb = '' if row[27] is None else str(row[27])
        cod_usuario_actualiza = '' if row[28] is None else str(row[28])
        usuario_actualiza = '' if row[29] is None else str(row[29])
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        formatted_rows.append(( 
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
        ))

    # Conectar a MySQL
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    # Truncar la tabla en MySQL
    try:
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_coloproctologia_contrarreferencias")
        conn_mysql.commit()
        logging.info("Tabla 'z_usabilidad_coloproctologia_contrarreferencias' truncada exitosamente.")
    except mysql.connector.Error as e:
        logging.error(f"Error al truncar la tabla: {e}")
        raise

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_usabilidad_coloproctologia_contrarreferencias (
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

    chunk_size = 1000  # Ajusta este tamaño según sea necesario
    for i in range(0, len(formatted_rows), chunk_size):
        chunk = formatted_rows[i:i + chunk_size]
        cursor_mysql.executemany(insert_query, chunk)
        conn_mysql.commit()
    logging.info("Datos transferidos exitosamente.")
except jaydebeapi.DatabaseError as e:
    logging.error(f"Error en InterSystems IRIS: {e}")
except mysql.connector.Error as e:
    logging.error(f"Error en MySQL: {e}")
except ValueError as e:
    logging.error(f"Error en la configuración: {e}")
except Exception as e:
    logging.error(f"Error: {e}")
finally:
    # Cerrar cursores y conexiones
    if cursor_iris:
        cursor_iris.close()
    if conn_iris:
        conn_iris.close()
    if cursor_mysql:
        cursor_mysql.close()
    if conn_mysql:
        conn_mysql.close()
    # Detener la JVM si la iniciamos
    if jpype.isJVMStarted():
        jpype.shutdownJVM()