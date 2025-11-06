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
                    handlers=[logging.FileHandler("logs/z_oftamologia_diagnosticos.log"),
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
        FROM
            RB_Appointment
        WHERE
            APPT_DateComp >= '2025-04-23'
            AND APPT_Adm_DR->PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448
            AND APPT_AS_ParRef->AS_RES_ParRef->RES_CTLOC_DR IN (2680,2874,2871,2872,2869,2875,2873,4531,4532)
            AND APPT_STATUS <> 'X'
            AND APPT_TimeComp > 0
            AND APPT_AS_ParRef->AS_RES_ParRef->RES_RowId > 0 
            AND APPT_AS_ParRef->AS_ChildSub > 0 
            AND APPT_ChildSub > 0
            AND APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Date = APPT_DateComp;
        '''
    
# AND APPT_Adm_DR->PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_Date = APPT_DateComp;

#   2680    HDS-07-400-9    Policlínico de Oftalmologia HDS
#   2874    HDS-07-400-F    Policlínico de Oftalmología Depto. Cornea HDS
#   2871    HDS-07-400-C    Policlínico de Oftalmología Depto. Estrabismo HDS
#   2872    HDS-07-400-D    Policlínico de Oftalmología Depto. Glaucoma HDS
#   2869    HDS-07-400-A    Policlínico de Oftalmología Depto. Orbita HDS
#   2875    HDS-07-400-G    Policlínico de Oftalmología Depto. Retina HDS
#   2873    HDS-07-400-E    Policlínico de Oftalmología Depto.  UVEA HDS
#   4531    HDS-POFG    Policlínico de Oftalmología GES
#   4532    HDS-POFNG   Policlínico de Oftalmología no GES

    # Ejecutar consulta en InterSystems IRIS
    cursor_iris = conn_iris.cursor()
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # Convertir filas a formato adecuado para MySQL
    formatted_rows = []
    for row in rows:

        nro_episodio= '' if row[0] is None else str(row[0])
        nro_registro= '' if row[1] is None else str(row[1])
        run= '' if row[2] is None else str(row[2])
        nombres= '' if row[3] is None else str(row[3])
        apellido_paterno= '' if row[4] is None else str(row[4])
        apellido_materno= '' if row[5] is None else str(row[5])
        edad= '' if row[6] is None else str(row[6])
        nro_le= '' if row[7] is None else str(row[7])
        descripcion_grupo_departamento= '' if row[8] is None else str(row[8])
        descripcion_local_agendamiento= '' if row[9] is None else str(row[9])
        descripcion_recurso= '' if row[10] is None else str(row[10])
        codigo_prestacion_agendada= '' if row[11] is None else str(row[11])
        descripcion_prestacion_agendada= '' if row[12] is None else str(row[12])
        fecha_cita= '' if row[13] is None else str(row[13])
        hora_cita= '' if row[14] is None else str(row[14])
        fecha_creacion_diagnostico= '' if row[15] is None else str(row[15])
        hora_creacion_diagnostico= '' if row[16] is None else str(row[16])
        codigo_usuario_registra_diagnostico= '' if row[17] is None else str(row[17])
        descripcion_usuario_registra_diagnostico= '' if row[18] is None else str(row[18])
        fecha_actualizacion= '' if row[19] is None else str(row[19])
        hora_actualizacion= '' if row[20] is None else str(row[20])
        codigo_usuario_actualiza_diagnostico= '' if row[21] is None else str(row[21])
        descripcion_usuario_actualiza_diagnostico= '' if row[22] is None else str(row[22])
        codigo_diagnostico= '' if row[23] is None else str(row[23])
        descripcion_diagnostico= '' if row[24] is None else str(row[24])
        ges= '' if row[25] is None else str(row[25])
        codigo_tipo_diagnostico= '' if row[26] is None else str(row[26])
        descripcipon_tipo_diagnostico= '' if row[27] is None else str(row[27])
        codigo_etapa_GES= '' if row[28] is None else str(row[28])
        descripcion_etapa_ges= '' if row[29] is None else str(row[29])
        diagnostico_principal= '' if row[30] is None else str(row[30])
        diagnostico_activo= '' if row[31] is None else str(row[31])
        motivo_inactivacion= '' if row[32] is None else str(row[32])
        fundamento_y_complemento_del_diagnostico= '' if row[33] is None else str(row[33])
        lateridad= '' if row[34] is None else str(row[34])
        severidad= '' if row[35] is None else str(row[35])
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        formatted_rows.append(( 
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
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_oftalmologia_diagnosticos")
        conn_mysql.commit()
        logging.info("Tabla 'z_usabilidad_oftalmologia_diagnosticos' truncada exitosamente.")
    except mysql.connector.Error as e:
        logging.error(f"Error al truncar la tabla: {e}")
        raise

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_usabilidad_oftalmologia_diagnosticos (
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