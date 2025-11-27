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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_usabilidad_hospitalizados_epicrisis.log"),
        logging.StreamHandler()
    ]
)

# Cargar variables de entorno IRIS
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc  = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# Cargar variables de entorno MySQL
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

    # ============================================================
    # CONSULTA A IRIS
    # ============================================================
    query = """ 
        select %nolock PAADM_DepCode_DR->CTLOC_Hospital_DR->HOSP_Code as HOSP_Code,
        isnull(PAADM_PAPMI_DR->PAPMI_Name,'') ||', '|| isnull(PAADM_PAPMI_DR->PAPMI_Name3,'') ||', '|| isnull(PAADM_PAPMI_DR->PAPMI_Name2,'') as NombrePaciente,
        PAADM_PAPMI_DR->PAPMI_ID as RUNPaciente,
        PAADM_PAPMI_DR->PAPMI_Sex_DR->CTSEX_Code as SexoCodigo, PAADM_PAPMI_DR->PAPMI_Sex_DR->CTSEX_Desc as Sexo,
        PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_CityCode_DR->CTCIT_Desc as Comuna,
        PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_HCP_DR->HCP_Desc as "EstablecimientoInscripción",
        PAADM_CurrentWard_DR->WARD_Code as ServicioClinicoCodigo, PAADM_CurrentWard_DR->WARD_Desc as ServicioClinico,
        convert( varchar, PAADM_AdmDate, 103 ) as FechaAtencion,
        convert( varchar, PAADM_DischgDate, 103 ) as FechaEgreso,
        convert( varchar, DIS_Date, 103 ) as FechaAlta,
        DIS_DischargeDestination_DR->DDEST_Desc as DestinoEgreso,
        PAADM_AdmNo as NumeroEpisodio,
        DIS_CareProv_DR->CTPCP_Desc as MedicoContacto,
        PAADM_Hospital_DR->HOSP_Desc as Hosp,
        PAADM_Epissubtype_DR->SUBT_Desc as subtipoepi,
        DIS_Procedures as TratamientoRecibido,
        DIS_TextBox4 as ProximoControl,
        DIS_ClinicalOpinion as IndicacionesAlAlta,
        DIS_PrincipalDiagnosis as DiagnosticoQueMotivoIngreso,
        PAADM_CurrentWard_DR->WARD_Desc AS "local_actual",
        PA_DischargeSummary.DIS_Status as "estado_epicrisis",
        (CASE  
            WHEN PA_DischargeSummary.DIS_Status = 'A' THEN 'Autorizado'
            WHEN PA_DischargeSummary.DIS_Status = 'E' THEN 'En Progreso'
            WHEN PA_DischargeSummary.DIS_Status is null THEN 'Sin Epicrisis'
            ELSE 'Otro'
        END) AS "descripcion_estado_epicrisis",
        SSUSR_Name as "usuario_update_epicrisis",
        convert( varchar, PAADM_AdmTime, 108 ) as HoraAtencion,
        convert( varchar, PAADM_DischgTime, 108 ) as HoraEgreso
        from PA_Adm
        left join PA_DischargeSummary on
            DIS_RowId = ( select %nolock top 1 DIS_PADischargeSummary_DR
                           from PA_Adm2DischargeSummary 
                           where DIS_ParRef = PAADM_RowId 
                           order by dis_childsub desc )
        LEFT JOIN SS_User on PA_DischargeSummary.DIS_UpdateUser_DR = SSUSR_RowId
        WHERE
            PAADM_DischgDate >= '2024-10-09'
        and PAADM_Hospital_DR = 10448
        AND PAADM_Type = 'I';
    """

    cursor_iris = conn_iris.cursor()

    try:
        cursor_iris.execute(query)
        rows = cursor_iris.fetchall()
    except Exception as e:
        fail_and_exit(f"Error ejecutando consulta IRIS: {e}")

    # Formatear filas
    formatted_rows = []
    for row in rows:
        formatted_rows.append(tuple(
            '' if r is None else str(r) for r in row
        ) + (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))

    # ============================================================
    # CONEXIÓN A MYSQL
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
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_hospitalizados_epicrisis")
        conn_mysql.commit()
    except Exception as e:
        fail_and_exit(f"Error al truncar MySQL: {e}")

    # INSERT
    insert_query = """
        INSERT INTO z_usabilidad_hospitalizados_epicrisis (
            HOSP_Code, NombrePaciente, RUNPaciente, SexoCodigo, Sexo, Comuna,
            EstablecimientoInscripcion, ServicioClinicoCodigo, ServicioClinico,
            FechaAtencion, FechaEgreso, FechaAlta, DestinoEgreso, NumeroEpisodio,
            MedicoContacto, Hosp, subtipoepi, TratamientoRecibido, ProximoControl,
            IndicacionesAlAlta, DiagnosticoQueMotivoIngreso, local_actual, estado_epicrisis,
            descripcion_estado_epicrisis, usuario_update_epicrisis, HoraAtencion, HoraEgreso,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
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
