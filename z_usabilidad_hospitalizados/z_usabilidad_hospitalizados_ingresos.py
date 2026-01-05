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
# CONFIGURACIÓN
# ============================================================

load_dotenv(override=True)

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_usabilidad_hospitalizados_ingresos.log"),
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

# ============================================================
# ERROR HANDLER
# ============================================================

def fail_and_exit(message):
    logging.error(message)
    sys.exit(1)

# ============================================================
# NUEVA FUNCIÓN → CREAR TABLA SI NO EXISTE
# ============================================================

def create_table_if_not_exists_ingresos(cursor, conn):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_name = 'z_usabilidad_hospitalizados_ingresos';
    """, (mysql_database,))

    exists = cursor.fetchone()[0]

    if exists:
        logging.info("Tabla z_usabilidad_hospitalizados_ingresos ya existe.")
        return

    logging.info("Creando tabla z_usabilidad_hospitalizados_ingresos...")

    create_sql = """
    CREATE TABLE `z_usabilidad_hospitalizados_ingresos` (
      `HOSP_Code` VARCHAR(6),
      `NombrePaciente` VARCHAR(70),
      `RUNPaciente` VARCHAR(10),
      `SexoCodigo` VARCHAR(1),
      `Sexo` VARCHAR(23),
      `Comuna` VARCHAR(20),
      `EstablecimientoInscripcion` VARCHAR(86),
      `ServicioClinicoCodigo` VARCHAR(27),
      `ServicioClinico` VARCHAR(36),
      `FechaAtencion` VARCHAR(10),
      `FechaEgreso` VARCHAR(10),
      `FechaAlta` VARCHAR(10),
      `DestinoEgreso` VARCHAR(49),
      `NumeroEpisodio` VARCHAR(11),
      `MedicoContacto` VARCHAR(44),
      `Hosp` VARCHAR(45),
      `subtipoepi` VARCHAR(43),
      `TratamientoRecibido` TEXT,
      `ProximoControl` VARCHAR(29),
      `IndicacionesAlAlta` TEXT,
      `DiagnosticoQueMotivoIngreso` TEXT,
      `local_actual` VARCHAR(36),
      `estado_epicrisis` VARCHAR(1),
      `descripcion_estado_epicrisis` VARCHAR(13),
      `usuario_update_epicrisis` VARCHAR(44),
      `fechaActualizacion` DATETIME
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;
    """

    cursor.execute(create_sql)
    conn.commit()
    logging.info("Tabla creada con tamaños óptimos.")

# ============================================================
# INICIO DEL PROCESO
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
        fail_and_exit("Variables IRIS no configuradas.")

    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        fail_and_exit("Variables MySQL no configuradas.")

    # INICIAR JVM
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # CONEXIÓN A IRIS
    logging.info("Conectando a IRIS...")
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )
    cursor_iris = conn_iris.cursor()

    # CONSULTA IRIS
    query = ''' 
            SELECT %nolock PAADM_DepCode_DR->CTLOC_Hospital_DR->HOSP_Code,
            isnull(PAADM_PAPMI_DR->PAPMI_Name,'') ||', '|| isnull(PAADM_PAPMI_DR->PAPMI_Name3,'') ||', '|| isnull(PAADM_PAPMI_DR->PAPMI_Name2,'') as NombrePaciente,
            PAADM_PAPMI_DR->PAPMI_ID,
            PAADM_PAPMI_DR->PAPMI_Sex_DR->CTSEX_Code, 
            PAADM_PAPMI_DR->PAPMI_Sex_DR->CTSEX_Desc,
            PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_CityCode_DR->CTCIT_Desc,
            PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_HCP_DR->HCP_Desc,
            PAADM_CurrentWard_DR->WARD_Code, 
            PAADM_CurrentWard_DR->WARD_Desc,
            convert(varchar, PAADM_AdmDate, 103),
            convert(varchar, PAADM_DischgDate, 103),
            convert(varchar, DIS_Date, 103),
            DIS_DischargeDestination_DR->DDEST_Desc,
            PAADM_AdmNo,
            DIS_CareProv_DR->CTPCP_Desc,
            PAADM_Hospital_DR->HOSP_Desc,
            PAADM_Epissubtype_DR->SUBT_Desc,
            DIS_Procedures,
            DIS_TextBox4,
            DIS_ClinicalOpinion,
            DIS_PrincipalDiagnosis,
            PAADM_CurrentWard_DR->WARD_Desc,
            PA_DischargeSummary.DIS_Status,
            CASE 
                WHEN PA_DischargeSummary.DIS_Status = 'A' THEN 'Autorizado'
                WHEN PA_DischargeSummary.DIS_Status = 'E' THEN 'En Progreso'
                WHEN PA_DischargeSummary.DIS_Status IS NULL THEN 'Sin Epicrisis'
                ELSE 'Otro'
            END,
            SSUSR_Name
            from PA_Adm
            left join PA_DischargeSummary on
                DIS_RowId = (
                    select %nolock top 1 DIS_PADischargeSummary_DR 
                    from PA_Adm2DischargeSummary 
                    where DIS_ParRef = PAADM_RowId 
                    order by dis_childsub desc
                )
            LEFT JOIN SS_User on PA_DischargeSummary.DIS_UpdateUser_DR = SSUSR_RowId
            WHERE
                PAADM_AdmDate >= DATEADD(MONTH, -12, CURRENT_DATE)
            and PAADM_Hospital_DR = 10448
            AND PAADM_Type = 'I';
        '''
    
    #PAADM_AdmDate >= '2024-10-09'
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    formatted_rows = [
        tuple("" if r is None else str(r) for r in row)
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

    # CREAR TABLA SI NO EXISTE
    create_table_if_not_exists_ingresos(cursor_mysql, conn_mysql)

    # TRUNCATE
    cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_hospitalizados_ingresos")
    conn_mysql.commit()

    # INSERT
    insert_query = """
        INSERT INTO z_usabilidad_hospitalizados_ingresos (
            HOSP_Code, NombrePaciente, RUNPaciente, SexoCodigo, Sexo, Comuna,
            EstablecimientoInscripcion, ServicioClinicoCodigo, ServicioClinico,
            FechaAtencion, FechaEgreso, FechaAlta, DestinoEgreso, NumeroEpisodio,
            MedicoContacto, Hosp, subtipoepi, TratamientoRecibido, ProximoControl,
            IndicacionesAlAlta, DiagnosticoQueMotivoIngreso, local_actual,
            estado_epicrisis, descripcion_estado_epicrisis,
            usuario_update_epicrisis, fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        );
    """

    for i in range(0, len(formatted_rows), 1000):
        cursor_mysql.executemany(insert_query, formatted_rows[i:i+1000])
        conn_mysql.commit()

    logging.info(" Datos transferidos exitosamente.")
    sys.exit(0)

except Exception as e:
    fail_and_exit(f"Error inesperado: {e}")

finally:
    if cursor_iris: cursor_iris.close()
    if conn_iris: conn_iris.close()
    if cursor_mysql: cursor_mysql.close()
    if conn_mysql: conn_mysql.close()
    if jpype.isJVMStarted(): jpype.shutdownJVM()
