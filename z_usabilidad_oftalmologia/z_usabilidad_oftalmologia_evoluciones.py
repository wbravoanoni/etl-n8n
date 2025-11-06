import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
from cryptography.fernet import Fernet

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("logs/z_usabilidad_oftalmologia_evoluciones.log"),
                        logging.StreamHandler()])

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

def encrypt_parity_check(message):
    load_dotenv()
    key = os.getenv('ENCRYPTION_KEY').encode()
    fernet = Fernet(key)
    encrypted = fernet.encrypt(message.encode())
    return encrypted

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    if not jdbc_driver_name or not jdbc_driver_loc:
        logging.error("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
        raise ValueError("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
    if not iris_connection_string or not iris_user or not iris_password:
        logging.error("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
        raise ValueError("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        logging.error("Las variables de entorno de MySQL no están configuradas correctamente.")
        raise ValueError("Las variables de entorno de MySQL no están configuradas correctamente.")

    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )

    cursor_iris = conn_iris.cursor()

    query = f'''
        SELECT DISTINCT 
        b.NOT_ParRef->MRADM_ADM_DR->PAAdm_AdmNo AS "NumeroEpisodio",
        b.NOT_Status_DR->NNS_Desc AS "Estado_Evolucion",
        b.NOT_ClinNoteSens_DR->CNS_Desc AS "Grupo_Evolucion",
        b.NOT_ClinNotesType_DR->CNT_Desc AS "Tipo_Evolucion",
        b.NOT_User_DR->SSUSR_Name AS "Usuario_Evolucion",
        CONVERT(VARCHAR, b.NOT_Date ,105) AS "FechaEvolucion",
        CONVERT(VARCHAR(5), b.NOT_Time, 108) AS "HoraEvolucion",
        b.NOT_NurseId_DR->CTPCP_Desc AS "ProfesionalEvolucion",
        b.NOT_NurseId_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc AS "EstamentoProfesional",
        b.NOT_UserAuth_DR->SSUSR_DefaultDept_DR->CTLOC_Desc AS "local_usuario",
        a.ENC_Loc_DR->CTLOC_Desc AS "Local_Encuentro"
    FROM 
        MR_NursingNotes b
    JOIN 
        MR_Encounter a ON b.NOT_ParRef = a.ENC_MRAdm_DR
    WHERE 
        b.NOT_Date >= '2025-04-23'
        AND b.NOT_Hospital_DR = 10448
        AND b.NOT_ParRef->MRADM_ADM_DR->PAAdm_Type = 'O'
        AND a.ENC_StartDate >= '2025-04-23'
        AND a.ENC_Loc_DR IN (2869, 2871, 2872, 2873, 2874, 2875, 2680, 3437, 
        3850,4260,4254,4264,4257,4261,4253,4259,4258,4617,4531,4532);
    '''


#   2869 Policlínico de Oftalmología Depto. Orbita HDS
#   2871 Policlínico de Oftalmología Depto. Estrabismo HDS
#   2872 Policlínico de Oftalmología Depto. Glaucoma HDS
#   2873 Policlínico de Oftalmología Depto.  UVEA HDS
#   2874 Policlínico de Oftalmología Depto. Cornea HDS
#   2875 Policlínico de Oftalmología Depto. Retina HDS
#   2680 Policlínico de Oftalmologia HDS
#   3437 PROCEDIMIENTOS OFTALMOLOGIA HDS
#   3850 EXAMENES ESPONTANEOS TM HDS
#   4260 OCT TM HDS
#   4254 Angiografia Retinal TM HDS
#   4264 Campos Visuales 1 TM HDS
#   4257 Campos Visuales 2 TM HDS
#   4261 ECO A/CALCULO LIO TM HDS
#   4253 MICROSCOPIA ESPECULAR TM HDS
#   4259 PAQUIMETRIA TM HDS
#   4258 AUTOFLUORESCENCIA TM HDS
#   3853 OPD/PENTACAM TM HDS
#   4617 Inyección Intraocular ( avastin )
#   4531    HDS-POFG    Policlínico de Oftalmología GES
#   4532    HDS-POFNG   Policlínico de Oftalmología no GES

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [fechaActualizacion]))

    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    cursor_mysql.execute("TRUNCATE TABLE z_usabilidad_oftalmologia_evoluciones")
    conn_mysql.commit()

    insert_query = """
        INSERT INTO z_usabilidad_oftalmologia_evoluciones (
        NumeroEpisodio,
        Estado_Evolucion,
        Grupo_Evolucion,
        Tipo_Evolucion,
        Usuario_Evolucion,
        FechaEvolucion,
        HoraEvolucion,
        ProfesionalEvolucion,
        EstamentoProfesional,
        local_usuario,
        Local_Encuentro,
        fechaActualizacion

        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s
        )
    """

    chunk_size = 1000
    for i in range(0, len(formatted_rows), chunk_size):
        chunk = formatted_rows[i:i + chunk_size]
        cursor_mysql.executemany(insert_query, chunk)
        conn_mysql.commit()
    logging.info("Datos transferidos exitosamente.")

except Exception as e:
    logging.error(f"Error: {e}")

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
