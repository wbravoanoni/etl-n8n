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
                    handlers=[logging.FileHandler("logs/z_usabilidad_hospitalizados_evoluciones.log"),
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
            select %nolock
            NOT_Hospital_DR->HOSP_Code,
            NOT_Hospital_DR->HOSP_Desc,
            NOT_ParRef->MRADM_ADM_DR->PAAdm_AdmNo as NumeroEpisodio,
            NOT_Status_DR->NNS_Desc as Estado_Evolucion,
            NOT_ClinNoteSens_DR->CNS_Desc as Grupo_Evolucion,
            NOT_ClinNotesType_DR->CNT_Desc as Tipo_Evolucion,
            NOT_User_DR->SSUSR_Name as Usuario_Evolucion,
            CONVERT(VARCHAR,NOT_Date ,105) as FechaEvolucion,
            convert( varchar(5), NOT_Time, 108 )as HoraEvolucion,
            NOT_NurseId_DR->CTPCP_Desc as ProfesionalEvolucion,
            NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc as EstamentoProfesional,
            NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_ID as RUNPaciente,
            NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_Name2 as NombresPaciente,
            NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_Name as AppPaternoPaciente,
            NOT_ParRef->MRADM_ADM_DR->PAADM_PAPMI_DR->PAPMI_Name3 as AppMaternoPaciente,
            NOT_ParRef->MRADM_ADM_DR->PAADM_CurrentWard_DR->WARD_Desc AS "local_actual",
            NOT_Hospital_DR,
            convert(varchar, NOT_ParRef->MRADM_ADM_DR->PAADM_EstimDischargeDate, 105) AS "fecha_alta_medica", 
            convert(varchar, NOT_ParRef->MRADM_ADM_DR->PAADM_EstimDischargeTime, 108) AS "hora_alta_medica",
            convert(varchar, NOT_ParRef->MRADM_ADM_DR->PAADM_DischgDate, 105) AS "fecha_alta_adm", 
            convert(varchar, NOT_ParRef->MRADM_ADM_DR->PAADM_DischgTime, 108) AS "hora_alta_adm"
            from
                SQLUser.MR_NursingNotes
            where
            NOT_Date >= '2024-10-09'
            and NOT_Hospital_DR  = 10448
            AND
            NOT_ParRef->MRADM_ADM_DR->PAAdm_Type='I'
        '''
    
    # Ejecutar consulta en InterSystems IRIS
    cursor_iris = conn_iris.cursor()
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # Convertir filas a formato adecuado para MySQL
    formatted_rows = []
    for row in rows:
        hOSP_Code = '' if row[0] is None else str(row[0])
        hOSP_Desc = '' if row[1] is None else str(row[1])
        numeroEpisodio = '' if row[2] is None else str(row[2])
        estado_Evolucion = '' if row[3] is None else str(row[3])
        grupo_Evolucion = '' if row[4] is None else str(row[4])
        tipo_Evolucion = '' if row[5] is None else str(row[5])
        usuario_Evolucion = '' if row[6] is None else str(row[6])
        fechaEvolucion = '' if row[7] is None else str(row[7])
        horaEvolucion = '' if row[8] is None else str(row[8])
        profesionalEvolucion = '' if row[9] is None else str(row[9])
        estamentoProfesional = '' if row[10] is None else str(row[10])
        rUNPaciente = '' if row[11] is None else str(row[11])
        nombresPaciente = '' if row[12] is None else str(row[12])
        appPaternoPaciente = '' if row[13] is None else str(row[13])
        appMaternoPaciente = '' if row[14] is None else str(row[14])
        appMaternoPaciente = '' if row[14] is None else str(row[14])
        local_actual = '' if row[15] is None else str(row[15])
        nOT_Hospital_DR = '' if row[16] is None else str(row[16])
        fecha_alta_medica = '' if row[17] is None else str(row[17])
        hora_alta_medica = '' if row[18] is None else str(row[18])
        fecha_alta_adm = '' if row[19] is None else str(row[19])
        hora_alta_adm = '' if row[20] is None else str(row[20])
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        formatted_rows.append(( 
            hOSP_Code,
            hOSP_Desc,
            numeroEpisodio,
            estado_Evolucion,
            grupo_Evolucion,
            tipo_Evolucion,
            usuario_Evolucion,
            fechaEvolucion,
            horaEvolucion,
            profesionalEvolucion,
            estamentoProfesional,
            rUNPaciente,
            nombresPaciente,
            appPaternoPaciente,
            appMaternoPaciente,
            local_actual,
            nOT_Hospital_DR,
            fecha_alta_medica,
            hora_alta_medica,
            fecha_alta_adm,
            hora_alta_adm,
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
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_hospitalizados_evoluciones")
        conn_mysql.commit()
        logging.info("Tabla 'z_usabilidad_hospitalizados_evoluciones' truncada exitosamente.")
    except mysql.connector.Error as e:
        logging.error(f"Error al truncar la tabla: {e}")
        raise

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_usabilidad_hospitalizados_evoluciones (
            HOSP_Code,
            HOSP_Desc,
            NumeroEpisodio,
            Estado_Evolucion,
            Grupo_Evolucion,
            Tipo_Evolucion,
            Usuario_Evolucion,
            FechaEvolucion,
            HoraEvolucion,
            ProfesionalEvolucion,
            EstamentoProfesional,
            RUNPaciente,
            NombresPaciente,
            AppPaternoPaciente,
            AppMaternoPaciente,
            local_actual,
            nOT_Hospital_DR,
            fecha_alta_medica,
            hora_alta_medica,
            fecha_alta_adm,
            hora_alta_adm,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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