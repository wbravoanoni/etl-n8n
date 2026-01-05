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
    logging.error(message)
    sys.exit(1)

def encrypt_parity_check(message):
    load_dotenv()
    key = os.getenv('ENCRYPTION_KEY').encode()
    fernet = Fernet(key)
    return fernet.encrypt(message.encode())


# ============================================================
# FUNCIÓN NUEVA → CREA TABLA SI NO EXISTE
# ============================================================

def create_table_if_not_exists(cursor, conn):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_name = 'z_usabilidad_hospitalizados_epicrisis';
    """, (mysql_database,))

    exists = cursor.fetchone()[0]

    if exists:
        logging.info("La tabla ya existe. No es necesario crearla.")
        return

    logging.info("La tabla NO existe. Creándola con tamaños optimizados...")

    create_sql = """
    CREATE TABLE `z_usabilidad_hospitalizados_epicrisis` (
      `HOSP_Code` varchar(6) DEFAULT NULL,
      `NombrePaciente` varchar(60) DEFAULT NULL,
      `RUNPaciente` varchar(10) DEFAULT NULL,
      `SexoCodigo` varchar(1) DEFAULT NULL,
      `Sexo` varchar(23) DEFAULT NULL,
      `Comuna` varchar(20) DEFAULT NULL,
      `EstablecimientoInscripcion` varchar(86) DEFAULT NULL,
      `ServicioClinicoCodigo` varchar(27) DEFAULT NULL,
      `ServicioClinico` varchar(36) DEFAULT NULL,
      `FechaAtencion` varchar(10) DEFAULT NULL,
      `FechaEgreso` varchar(10) DEFAULT NULL,
      `FechaAlta` varchar(10) DEFAULT NULL,
      `DestinoEgreso` varchar(49) DEFAULT NULL,
      `NumeroEpisodio` varchar(11) DEFAULT NULL,
      `MedicoContacto` varchar(44) DEFAULT NULL,
      `Hosp` varchar(45) DEFAULT NULL,
      `subtipoepi` varchar(43) DEFAULT NULL,
      `TratamientoRecibido` TEXT,
      `ProximoControl` varchar(29) DEFAULT NULL,
      `IndicacionesAlAlta` TEXT,
      `DiagnosticoQueMotivoIngreso` TEXT,
      `local_actual` varchar(36) DEFAULT NULL,
      `estado_epicrisis` varchar(1) DEFAULT NULL,
      `descripcion_estado_epicrisis` varchar(13) DEFAULT NULL,
      `usuario_update_epicrisis` varchar(44) DEFAULT NULL,
      `HoraAtencion` varchar(10) DEFAULT NULL,
      `HoraEgreso` varchar(10) DEFAULT NULL,
      `fechaActualizacion` datetime DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
    """

    cursor.execute(create_sql)
    conn.commit()
    logging.info("Tabla creada exitosamente.")


# ============================================================
# SCRIPT PRINCIPAL
# ============================================================

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # VALIDACIONES
    if not jdbc_driver_name or not jdbc_driver_loc:
        fail_and_exit("JDBC_DRIVER_NAME o JDBC_DRIVER_PATH no configurados.")

    if not iris_connection_string or not iris_user or not iris_password:
        fail_and_exit("Variables IRIS no configuradas correctamente.")

    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        fail_and_exit("Variables MySQL no configuradas correctamente.")

    # INICIAR JVM
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # CONEXIÓN A IRIS
    logging.info("Conectando a InterSystems IRIS...")
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )

    cursor_iris = conn_iris.cursor()

    # CONSULTA IRIS
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
        (CASE WHEN PA_DischargeSummary.DIS_Status = 'A' THEN 'Autorizado'
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
            PAADM_DischgDate >= DATEADD(MONTH, -12, CURRENT_DATE)
        and PAADM_Hospital_DR = 10448
        AND PAADM_Type = 'I';
    """

    #PAADM_DischgDate >= '2024-10-09'

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    formatted_rows = [
        tuple('' if r is None else str(r) for r in row) 
        + (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),)
        for row in rows
    ]

    # CONEXIÓN A MYSQL
    logging.info("Conectando a MySQL...")
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    # ================================
    # CREAR TABLA SI NO EXISTE
    # ================================
    create_table_if_not_exists(cursor_mysql, conn_mysql)

    # TRUNCATE
    cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_hospitalizados_epicrisis")
    conn_mysql.commit()

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

    for i in range(0, len(formatted_rows), 1000):
        chunk = formatted_rows[i:i+1000]
        cursor_mysql.executemany(insert_query, chunk)
        conn_mysql.commit()

    logging.info("Datos transferidos exitosamente.")
    sys.exit(0)

except Exception as e:
    fail_and_exit(f"Error inesperado: {e}")

finally:
    if cursor_iris: cursor_iris.close()
    if conn_iris: conn_iris.close()
    if cursor_mysql: cursor_mysql.close()
    if conn_mysql: conn_mysql.close()
    if jpype.isJVMStarted(): jpype.shutdownJVM()
