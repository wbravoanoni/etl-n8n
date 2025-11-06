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
                    handlers=[logging.FileHandler("logs/z_usabilidad_hospitalizados_epicrisis.log"),
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
                        ELSE 'Otro' END) AS "descripcion_estado_epicrisis",
                        SSUSR_Name as "usuario_update_epicrisis",
                        convert( varchar, PAADM_AdmTime, 108 ) as HoraAtencion,
                        convert( varchar, PAADM_DischgTime, 108 ) as HoraEgreso
            from PA_Adm
                left join PA_DischargeSummary on
                DIS_RowId = ( select %nolock top 1 DIS_PADischargeSummary_DR from PA_Adm2DischargeSummary where DIS_ParRef = PAADM_RowId order by dis_childsub desc )
                LEFT JOIN SS_User on PA_DischargeSummary.DIS_UpdateUser_DR = SSUSR_RowId
                WHERE
                PAADM_DischgDate >= '2024-10-09'
            and PAADM_Hospital_DR = 10448
            AND
            PAADM_Type = 'I';
        '''
    
    # Ejecutar consulta en InterSystems IRIS
    cursor_iris = conn_iris.cursor()
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # Convertir filas a formato adecuado para MySQL
    formatted_rows = []
    for row in rows:
        hOSP_Code = '' if row[0] is None else str(row[0])
        NombrePaciente = '' if row[1] is None else str(row[1])
        RUNPaciente = '' if row[2] is None else str(row[2])
        SexoCodigo = '' if row[3] is None else str(row[3])
        Sexo = '' if row[4] is None else str(row[4])
        Comuna = '' if row[5] is None else str(row[5])
        EstablecimientoInscripcion = '' if row[6] is None else str(row[6])
        ServicioClinicoCodigo = '' if row[7] is None else str(row[7])
        ServicioClinico = '' if row[8] is None else str(row[8])
        FechaAtencion = '' if row[9] is None else str(row[9])
        FechaEgreso = '' if row[10] is None else str(row[10])
        FechaAlta = '' if row[11] is None else str(row[11])
        DestinoEgreso = '' if row[12] is None else str(row[12])
        NumeroEpisodio = '' if row[13] is None else str(row[13])
        MedicoContacto = '' if row[14] is None else str(row[14])
        Hosp = '' if row[15] is None else str(row[15])
        subtipoepi  = '' if row[16] is None else str(row[16])
        TratamientoRecibido = '' if row[17] is None else str(row[17])
        ProximoControl = '' if row[18] is None else str(row[18])
        IndicacionesAlAlta = '' if row[19] is None else str(row[19])
        DiagnosticoQueMotivoIngreso = '' if row[20] is None else str(row[20])
        local_actual = '' if row[21] is None else str(row[21])
        estado_epicrisis = '' if row[22] is None else str(row[22])
        descripcion_estado_epicrisis = '' if row[23] is None else str(row[23])
        usuario_update_epicrisis = '' if row[24] is None else str(row[24])
        HoraAtencion = '' if row[24] is None else str(row[25])
        HoraEgreso = '' if row[24] is None else str(row[26])
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        formatted_rows.append(( 
            hOSP_Code,
            NombrePaciente,
            RUNPaciente,
            SexoCodigo,
            Sexo,
            Comuna,
            EstablecimientoInscripcion,
            ServicioClinicoCodigo,
            ServicioClinico,
            FechaAtencion,
            FechaEgreso,
            FechaAlta,
            DestinoEgreso,
            NumeroEpisodio,
            MedicoContacto,
            Hosp,
            subtipoepi,
            TratamientoRecibido,
            ProximoControl,
            IndicacionesAlAlta,
            DiagnosticoQueMotivoIngreso,
            local_actual,
            estado_epicrisis,
            descripcion_estado_epicrisis,
            usuario_update_epicrisis,
            HoraAtencion,
            HoraEgreso,
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
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_hospitalizados_epicrisis")
        conn_mysql.commit()
        logging.info("Tabla 'z_usabilidad_hospitalizados_epicrisis' truncada exitosamente.")
    except mysql.connector.Error as e:
        logging.error(f"Error al truncar la tabla: {e}")
        raise

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_usabilidad_hospitalizados_epicrisis (
            HOSP_Code,
            NombrePaciente,
            RUNPaciente,
            SexoCodigo,
            Sexo,
            Comuna,
            EstablecimientoInscripcion,
            ServicioClinicoCodigo,
            ServicioClinico,
            FechaAtencion,
            FechaEgreso,
            FechaAlta,
            DestinoEgreso,
            NumeroEpisodio,
            MedicoContacto,
            Hosp,
            subtipoepi,
            TratamientoRecibido,
            ProximoControl,
            IndicacionesAlAlta,
            DiagnosticoQueMotivoIngreso,
            local_actual,
            estado_epicrisis,
            descripcion_estado_epicrisis,
            usuario_update_epicrisis,
            HoraAtencion,
            HoraEgreso,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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