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
                    handlers=[logging.FileHandler("logs/z_usabilidad_coloproctologia_agendas_evoluciones.log"),
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
            appt.APPT_Adm_DR->PAADM_ADMNO AS "nro_episodio",
            appt.APPT_PAPMI_DR->PAPMI_ID AS "rut_del_paciente",
            appt.APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_NAME2 AS "nombre_paciente",
            appt.APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_NAME AS "apellido_paterno_paciente",
            appt.APPT_PAPMI_DR->PAPMI_PAPER_DR->PAPER_NAME3 AS "apellido_materno_paciente",
            sched.AS_Date AS "fecha_agendada",
            sched.AS_SessStartTime AS "hora_agendada",
            CASE
                WHEN appt.APPT_Status = 'P' THEN 'Agendado'
                WHEN appt.APPT_Status = 'D' THEN 'Atendido'
                WHEN appt.APPT_Status = 'X' THEN 'Cancelado'
                WHEN appt.APPT_Status = 'A' THEN 'Llegó'
                WHEN appt.APPT_Status = 'N' THEN 'No atendido'
                WHEN appt.APPT_Status = 'T' THEN 'Transferido'
                WHEN appt.APPT_Status = 'H' THEN 'En Espera'
                ELSE appt.APPT_Status
            END AS "estado_de_la_cita",
            sched.AS_RES_ParRef->RES_Desc AS "profesional_agenda",
            sched.AS_RES_ParRef->RES_CTLOC_DR->CTLOC_Desc AS "local",
            sched.AS_RES_ParRef->RES_CTLOC_DR->CTLOC_Dep_DR->DEP_Desc AS "especialidad_local",
            sched.AS_RES_ParRef->RES_CTPCP_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc AS "tipo_profesional",
            sched.AS_RBEffDateSession_DR->SESS_SessionType_DR->SESS_Desc AS "tipo_de_sesion",
            CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM MR_NursingNotes b
                    WHERE 
                        b.NOT_Hospital_DR = 10448
                        AND b.NOT_ParRef->MRADM_ADM_DR->PAADM_ADMNo = appt.APPT_Adm_DR->PAADM_ADMNo
                        AND b.NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc IN 
                            ('Médico Cirujano', 'Médico', 'Cirujano Dentista', 
                            'Psiquiatría', 'Psiquiatría Adultos', 'Odontólogo/Dentista')
                        AND b.NOT_Date BETWEEN DATEADD(DAY, -3, sched.AS_Date) AND DATEADD(DAY, 3, sched.AS_Date)
                ) THEN 'Sí tiene evolución'
                ELSE 'No tiene evolución'
            END AS "tiene_evoluciones_medico_odontologo",
            (
                SELECT TOP 1 
                    CONVERT(VARCHAR(10), b.NOT_Date, 105)
                FROM MR_NursingNotes b
                WHERE 
                    b.NOT_Hospital_DR = 10448
                    AND b.NOT_ParRef->MRADM_ADM_DR->PAADM_ADMNo = appt.APPT_Adm_DR->PAADM_ADMNo
                    AND b.NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc IN 
                        ('Médico Cirujano', 'Médico', 'Cirujano Dentista', 
                        'Psiquiatría', 'Psiquiatría Adultos', 'Odontólogo/Dentista')
                    AND b.NOT_Date BETWEEN DATEADD(DAY, -3, sched.AS_Date) AND DATEADD(DAY, 3, sched.AS_Date)
                ORDER BY ABS(DATEDIFF(DAY, b.NOT_Date, sched.AS_Date))
            ) AS "fecha_evolucion_medico_odontologo",
            (
                SELECT TOP 1 
                    b.NOT_User_DR->SSUSR_Name
                FROM MR_NursingNotes b
                WHERE 
                    b.NOT_Hospital_DR = 10448
                    AND b.NOT_ParRef->MRADM_ADM_DR->PAADM_ADMNo = appt.APPT_Adm_DR->PAADM_ADMNo
                    AND b.NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc IN 
                        ('Médico Cirujano', 'Médico', 'Cirujano Dentista', 
                        'Psiquiatría', 'Psiquiatría Adultos', 'Odontólogo/Dentista')
                    AND b.NOT_Date BETWEEN DATEADD(DAY, -3, sched.AS_Date) AND DATEADD(DAY, 3, sched.AS_Date)
                ORDER BY ABS(DATEDIFF(DAY, b.NOT_Date, sched.AS_Date))
            ) AS "usuario_evolucion_medico_odontologo",
            CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM MR_NursingNotes b
                    WHERE 
                        b.NOT_Hospital_DR = 10448
                        AND b.NOT_ParRef->MRADM_ADM_DR->PAADM_ADMNo = appt.APPT_Adm_DR->PAADM_ADMNo
                        AND b.NOT_NurseId_DR->CTPCP_Desc = sched.AS_RES_ParRef->RES_Desc
                        AND b.NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc = 'Enfermera (o)'
                        AND b.NOT_Date BETWEEN DATEADD(DAY, -3, sched.AS_Date) AND DATEADD(DAY, 3, sched.AS_Date)
                ) THEN 'Sí tiene evolución'
                ELSE 'No tiene evolución'
            END AS "tiene_evoluciones_enfermería",
            (
                SELECT TOP 1 
                    CONVERT(VARCHAR(10), b.NOT_Date, 105)
                FROM MR_NursingNotes b
                WHERE 
                    b.NOT_Hospital_DR = 10448
                    AND b.NOT_ParRef->MRADM_ADM_DR->PAADM_ADMNo = appt.APPT_Adm_DR->PAADM_ADMNo
                    AND b.NOT_NurseId_DR->CTPCP_Desc = sched.AS_RES_ParRef->RES_Desc
                    AND b.NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc = 'Enfermera (o)'
                    AND b.NOT_Date BETWEEN DATEADD(DAY, -3, sched.AS_Date) AND DATEADD(DAY, 3, sched.AS_Date)
                ORDER BY ABS(DATEDIFF(DAY, b.NOT_Date, sched.AS_Date))
            ) AS "fecha_evolucion_enfermería",
            (
                SELECT TOP 1 
                    b.NOT_User_DR->SSUSR_Name
                FROM MR_NursingNotes b
                WHERE 
                    b.NOT_Hospital_DR = 10448
                    AND b.NOT_ParRef->MRADM_ADM_DR->PAADM_ADMNo = appt.APPT_Adm_DR->PAADM_ADMNo
                    AND b.NOT_NurseId_DR->CTPCP_Desc = sched.AS_RES_ParRef->RES_Desc
                    AND b.NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc = 'Enfermera (o)'
                    AND b.NOT_Date BETWEEN DATEADD(DAY, -3, sched.AS_Date) AND DATEADD(DAY, 3, sched.AS_Date)
                ORDER BY ABS(DATEDIFF(DAY, b.NOT_Date, sched.AS_Date))
            ) AS "usuario_evolucion_enfermería"
        FROM RB_Appointment appt
        LEFT JOIN RB_ApptSchedule sched 
            ON appt.APPT_AS_ParRef = sched.AS_RowId
        WHERE 
            sched.AS_Date >='2025-10-01' AND sched.AS_Date <= CURRENT_TIMESTAMP
            AND sched.AS_RES_ParRef->RES_CTLOC_DR->CTLOC_HOSPITAL_DR = 10448 
            AND sched.AS_RES_ParRef->RES_CTLOC_DR->CTLOC_RowID = 2831;
        '''
    
        #   2831 Policlínico de Cirugía-Proctología HDS

    # Ejecutar consulta en InterSystems IRIS
    cursor_iris = conn_iris.cursor()
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # Convertir filas a formato adecuado para MySQL
    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))
        
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
        cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_coloproctologia_agendas_evoluciones")
        conn_mysql.commit()
        logging.info("Tabla 'z_usabilidad_coloproctologia_agendas_evoluciones' truncada exitosamente.")
    except mysql.connector.Error as e:
        logging.error(f"Error al truncar la tabla: {e}")
        raise

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_usabilidad_coloproctologia_agendas_evoluciones (
        nro_episodio,
        rut_del_paciente,
        nombre_paciente,
        apellido_paterno_paciente,
        apellido_materno_paciente,
        fecha_agendada,
        hora_agendada,
        estado_de_la_cita,
        profesional_agenda,
        local,
        especialidad_local,
        tipo_profesional,
        tipo_de_sesion,
        tiene_evoluciones_medico_odontologo,
        fecha_evolucion_medico_odontologo,
        usuario_evolucion_medico_odontologo,
        tiene_evoluciones_enfermería,
        fecha_evolucion_enfermería,
        usuario_evolucion_enfermería,
        fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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