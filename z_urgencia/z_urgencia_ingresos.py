import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
from cryptography.fernet import Fernet

load_dotenv(override=True)

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_urgencia_ingresos_resumen.log"),
        logging.StreamHandler()
    ]
)

# =========================
# VARIABLES ENTORNO
# =========================
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = int(os.getenv('DB_MYSQL_PORT', 3306))
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

# -----------------------------
# FUNCIONES EPISODIO CIFRADO
# -----------------------------
def base36encode(number):
    chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    result = ''
    while number > 0:
        number, i = divmod(number, 36)
        result = chars[i] + result
    return result or '0'

def prefijo_a_codigo(prefijo):
    return {'U': 1, 'A': 2, 'H': 3}.get(prefijo.upper(), 0)

def codificar_episodio(episodio, semilla=7919, offset=123):
    if not episodio:
        return ''
    prefijo = episodio[0]
    numero = int(''.join(filter(str.isdigit, episodio)))
    combinado = (prefijo_a_codigo(prefijo) * 10**10) + numero
    return base36encode(combinado * semilla + offset)

# -----------------------------
# MYSQL: RECREAR TABLA
# -----------------------------
def recreate_table_mysql(cursor_mysql):
    cursor_mysql.execute("DROP TABLE IF EXISTS z_urgencia_ingresos_resumen")
    cursor_mysql.execute("""
        CREATE TABLE z_urgencia_ingresos_resumen (
            nroEpisodio                     VARCHAR(11),
            fechaEpisodio                   VARCHAR(10),
            horaEpisodio                    VARCHAR(8),
            nombres                         VARCHAR(46),
            apellidoPaterno                 VARCHAR(25),
            apellidoMaterno                 VARCHAR(22),
            fecha_categorizacion            VARCHAR(10),
            hora_categorizacion             VARCHAR(8),
            categorizador                   VARCHAR(41),
            categorización                  VARCHAR(52),
            fechaCreacionEncuentro          VARCHAR(10),
            horaCreacionEncuentro           VARCHAR(8),
            profEncuentroCodigo             VARCHAR(12),
            profEncuentroDescripcion        VARCHAR(42),
            profEncuentroCargo              VARCHAR(19),
            motivoCierreInterrumpido        VARCHAR(42),
            fechaAltaMedica                 VARCHAR(10),
            horaAltaMedica                  VARCHAR(8),
            medicoAltaClinicaCodigo         VARCHAR(12),
            medicoAltaClinicaDescripcion    VARCHAR(42),
            condicionAlCierreDeAtencion     VARCHAR(9),
            pronosticoMedicoLegal           VARCHAR(17),
            destino                         VARCHAR(39),
            fechaAltaAdm                    VARCHAR(10),
            horaAltaAdm                     VARCHAR(8),
            estadoAtencion                  VARCHAR(6),
            diagnosticoDescripcion          TEXT,
            episodioCifrado                 VARCHAR(9),
            fechaActualizacion              DATETIME
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

# -----------------------------
# CONEXIONES
# -----------------------------
conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # =========================
    # VALIDACIONES
    # =========================
    if not jdbc_driver_name or not jdbc_driver_loc:
        raise ValueError("JDBC no configurado")
    if not iris_connection_string or not iris_user or not iris_password:
        raise ValueError("Credenciales IRIS incompletas")
    if not mysql_host or not mysql_user or not mysql_password or not mysql_database:
        raise ValueError("Credenciales MySQL incompletas")

    # =========================
    # JVM
    # =========================
    if not jpype.isJVMStarted():
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            "-Djava.class.path=" + jdbc_driver_loc
        )

    # =========================
    # CONEXIÓN IRIS
    # =========================
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )
    cursor_iris = conn_iris.cursor()

    # =========================
    # QUERY IRIS (SIN CAMBIOS)
    # =========================
    cursor_iris.execute(""" 
        SELECT
            PAADM_ADMNO,
            CONVERT(VARCHAR, PAADM_ADMDATE, 105),
            CONVERT(VARCHAR, PAADM_ADMTIME, 108),
            PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name2,
            PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name,
            PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name3,
            PAADM_TriageDate,
            PAADM_TriageTime,
            PAADM_TriageNurse_DR->CTPCP_Desc,
            PAADM_Priority_DR->CTACU_Desc,
            '',
            '',
            '',
            '',
            '',
            PAADM_MainMRADM_DR->MRADM_DischType_DR->CTDSP_Desc,
            CONVERT(VARCHAR, PAADM_EstimDischargeDate, 105),
            CONVERT(VARCHAR, PAADM_EstimDischargeTime, 108),
            PAADM_MedDischDoc_DR->CTPCP_Code,
            PAADM_MedDischDoc_DR->CTPCP_Desc,
            PAADM_DischCond_DR->DISCON_DESC,
            PAADM_TrafficAccident_DR->TRF_AccidentCode_DR->TRF_Desc,
            PAADM_MainMRADM_DR->MRADM_DischClassif_DR->DSCL_Desc,
            CONVERT(VARCHAR, PAADM_DischgDate, 105),
            CONVERT(VARCHAR, PAADM_DischgTime, 108),
            CASE PAADM_VISITSTATUS 
                WHEN 'A' THEN 'Actual'
                WHEN 'C' THEN 'Suspendido'
                WHEN 'D' THEN 'Egreso'
                WHEN 'P' THEN 'PreAdm'
                WHEN 'R' THEN 'Liberado'
                WHEN 'N' THEN 'NoAtnd'
                ELSE ''
            END
        FROM PA_ADM
        WHERE PAADM_ADMDATE >= '2025-01-01'
          AND PAADM_TYPE = 'E'
          AND PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448
    """)

    rows = cursor_iris.fetchall()
    logging.info(f"Filas IRIS obtenidas: {len(rows)}")

    # =========================
    # FORMATEO
    # =========================
    formatted_rows = []
    for r in rows:
        valores = [str(c) if c is not None else '' for c in r]
        episodio_cifrado = codificar_episodio(valores[0])
        formatted_rows.append(tuple(
            valores + [''] + [episodio_cifrado, datetime.now()]
        ))

    # =========================
    # MYSQL
    # =========================
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    recreate_table_mysql(cursor_mysql)
    conn_mysql.commit()

    insert_sql = """
        INSERT INTO z_urgencia_ingresos_resumen VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s
        )
    """

    chunk_size = 1000
    for i in range(0, len(formatted_rows), chunk_size):
        cursor_mysql.executemany(insert_sql, formatted_rows[i:i+chunk_size])
        conn_mysql.commit()

    logging.info("Carga z_urgencia_ingresos_resumen finalizada correctamente")

except Exception as e:
    logging.error(f"Error general: {e}", exc_info=True)

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
