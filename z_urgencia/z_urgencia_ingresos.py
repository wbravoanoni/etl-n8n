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
                    handlers=[logging.FileHandler("logs/z_urgencia_ingresos_resumen.log"),
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


# -----------------------------
# FUNCIONES PARA EPISODIO CIFRADO
# -----------------------------
def base36encode(number):
    chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    result = ''
    while number > 0:
        number, i = divmod(number, 36)
        result = chars[i] + result
    return result or '0'

def base36decode(s):
    return int(s, 36)

def prefijo_a_codigo(prefijo):
    mapa = {'U': 1, 'A': 2, 'H': 3}  # puedes agregar más si es necesario
    return mapa.get(prefijo.upper(), 0)

def codificar_episodio(episodio, semilla=7919, offset=123):
    if not episodio:
        return ''
    prefijo = episodio[0].upper()
    codigo_prefijo = prefijo_a_codigo(prefijo)
    numero = int(''.join(filter(str.isdigit, episodio)))
    combinado = (codigo_prefijo * 10**10) + numero
    return base36encode(combinado * semilla + offset)


# -----------------------------
# CONEXIONES
# -----------------------------
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
            SELECT
            -- datos de ingreso
            PAADM_ADMNO AS "nroEpisodio",
            CONVERT(VARCHAR, PAADM_ADMDATE, 105) AS "fechaEpisodio",
            CONVERT(VARCHAR, PAADM_ADMTIME, 108) AS "horaEpisodio",
            PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name2 AS "nombres",
            PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name AS "apellidoPaterno",
            PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name3 AS "apellidoMaterno",
            PAADM_TriageDate as "fecha_categorizacion",
            PAADM_TriageTime as "hora_categorizacion",
            PAADM_TriageNurse_DR->CTPCP_Desc as "categorizador",
            PAADM_Priority_DR->CTACU_Desc as "categorización",
            -- Sección modificada: datos de atención clínica real desde MR_EncEntry
            ISNULL((
                SELECT TOP 1 CONVERT(VARCHAR, ENTRY_StartDate, 105)
                FROM MR_EncEntry
                WHERE ENTRY_Encounter_DR IN (
                    SELECT ENC_RowId
                    FROM MR_Encounter
                    WHERE ENC_MRAdm_DR = PAADM_MainMRADM_DR
                )
                AND ENTRY_StartUser_DR->SSUSR_CareProv_DR->CTPCP_CarPrvTp_DR IN (56, 60, 71, 61, 73, 80, 83)
                ORDER BY ENTRY_StartDate, ENTRY_StartTime
            ), '') AS "fechaCreacionEncuentro",
            ISNULL((
                SELECT TOP 1 CONVERT(VARCHAR, ENTRY_StartTime, 108)
                FROM MR_EncEntry
                WHERE ENTRY_Encounter_DR IN (
                    SELECT ENC_RowId
                    FROM MR_Encounter
                    WHERE ENC_MRAdm_DR = PAADM_MainMRADM_DR
                )
                AND ENTRY_StartUser_DR->SSUSR_CareProv_DR->CTPCP_CarPrvTp_DR IN (56, 60, 71, 61, 73, 80, 83)
                ORDER BY ENTRY_StartDate, ENTRY_StartTime
            ), '') AS "horaCreacionEncuentro",
            ISNULL((
                SELECT TOP 1 ENTRY_StartUser_DR->SSUSR_CareProv_DR->CTPCP_Code
                FROM MR_EncEntry
                WHERE ENTRY_Encounter_DR IN (
                    SELECT ENC_RowId
                    FROM MR_Encounter
                    WHERE ENC_MRAdm_DR = PAADM_MainMRADM_DR
                )
                AND ENTRY_StartUser_DR->SSUSR_CareProv_DR->CTPCP_CarPrvTp_DR IN (56, 60, 71, 61, 73, 80, 83)
                ORDER BY ENTRY_StartDate, ENTRY_StartTime
            ), '') AS "profEncuentroCodigo",
            ISNULL((
                SELECT TOP 1 ENTRY_StartUser_DR->SSUSR_CareProv_DR->CTPCP_Desc
                FROM MR_EncEntry
                WHERE ENTRY_Encounter_DR IN (
                    SELECT ENC_RowId
                    FROM MR_Encounter
                    WHERE ENC_MRAdm_DR = PAADM_MainMRADM_DR
                )
                AND ENTRY_StartUser_DR->SSUSR_CareProv_DR->CTPCP_CarPrvTp_DR IN (56, 60, 71, 61, 73, 80, 83)
                ORDER BY ENTRY_StartDate, ENTRY_StartTime
            ), '') AS "profEncuentroDescripcion",
            ISNULL((
                SELECT TOP 1 ENTRY_CareProvType_DR->CTCPT_Desc
                FROM MR_EncEntry
                WHERE ENTRY_Encounter_DR IN (
                    SELECT ENC_RowId
                    FROM MR_Encounter
                    WHERE ENC_MRAdm_DR = PAADM_MainMRADM_DR
                )
                AND ENTRY_StartUser_DR->SSUSR_CareProv_DR->CTPCP_CarPrvTp_DR IN (56, 60, 71, 61, 73, 80, 83)
                ORDER BY ENTRY_StartDate, ENTRY_StartTime
            ), '') AS "profEncuentroCargo",
            -- Alta y egreso
            PAADM_MainMRADM_DR->MRADM_DischType_DR->CTDSP_Desc AS "motivoCierreInterrumpido",
            CONVERT(VARCHAR, PAADM_EstimDischargeDate, 105) AS "fechaAltaMedica",
            CONVERT(VARCHAR, PAADM_EstimDischargeTime, 108) AS "horaAltaMedica",
            PAADM_MedDischDoc_DR->CTPCP_Code AS "medicoAltaClinicaCodigo",
            PAADM_MedDischDoc_DR->CTPCP_Desc AS "medicoAltaClinicaDescripcion",
            PAADM_DischCond_DR->DISCON_DESC AS "condicionAlCierreDeAtencion",
            PAADM_TrafficAccident_DR->TRF_AccidentCode_DR->TRF_Desc AS "pronosticoMedicoLegal",
            PAADM_MainMRADM_DR->MRADM_DischClassif_DR->DSCL_Desc AS "destino",
            CONVERT(VARCHAR, PAADM_DischgDate, 105) AS "fechaAltaAdm",
            CONVERT(VARCHAR, PAADM_DischgTime, 108) AS "horaAltaAdm",
            CASE PAADM_VISITSTATUS 
                WHEN 'A' THEN 'Actual'
                WHEN 'C' THEN 'Suspendido'
                WHEN 'D' THEN 'Egreso'
                WHEN 'P' THEN 'Pre Admision'
                WHEN 'R' THEN 'Liberado'
                WHEN 'N' THEN 'No Atendido'
                ELSE NULL 
            END AS "estadoAtencion"
        FROM PA_ADM
        WHERE
            PAADM_ADMDATE >= '2025-01-01'
            AND PAADM_ADMTIME IS NOT NULL
            AND PAADM_TYPE = 'E'
            AND PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448;
    '''

    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    formatted_rows = []
    for row in rows:
        valores = [str(col) if col is not None else '' for col in row]
        nro_episodio = valores[0]
        episodio_cifrado = codificar_episodio(nro_episodio)
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        formatted_rows.append(tuple(valores + [episodio_cifrado, fechaActualizacion]))

    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    cursor_mysql.execute("TRUNCATE TABLE z_urgencia_ingresos_resumen")
    conn_mysql.commit()

    insert_query = """
        INSERT INTO z_urgencia_ingresos_resumen (
        nroEpisodio,fechaEpisodio,horaEpisodio,nombres,apellidoPaterno,apellidoMaterno,fecha_categorizacion,hora_categorizacion,
        categorizador,categorización,fechaCreacionEncuentro,horaCreacionEncuentro,profEncuentroCodigo,profEncuentroDescripcion,profEncuentroCargo,
        motivoCierreInterrumpido,fechaAltaMedica,horaAltaMedica,medicoAltaClinicaCodigo,
        medicoAltaClinicaDescripcion,condicionAlCierreDeAtencion,pronosticoMedicoLegal,destino,
        fechaAltaAdm,horaAltaAdm,estadoAtencion,episodioCifrado,fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
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
