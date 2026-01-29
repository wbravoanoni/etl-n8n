import jaydebeapi
import jpype
import os
import logging
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from dotenv import load_dotenv

# =========================================================
# CONFIGURACIÓN DE LOGS
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("z_usabilidad_5_salida_en_vivo/logs/9_hospitalizados_dias_paso1_descarga.log"),
        logging.StreamHandler()
    ]
)

# =========================================================
# VARIABLES DE ENTORNO
# =========================================================
load_dotenv()

jdbc_driver_name = os.getenv("JDBC_DRIVER_NAME")
jdbc_driver_loc = os.getenv("JDBC_DRIVER_PATH")
iris_connection_string = os.getenv("CONEXION_STRING")
iris_user = os.getenv("DB_USER")
iris_password = os.getenv("DB_PASSWORD")

archivo_excel = "z_usabilidad_5_salida_en_vivo/1_entrada/9_hospitalizados_dias_paso1_descarga.xlsx"

# =========================================================
# LIMPIEZA DE ARCHIVO PREVIO
# =========================================================
if os.path.exists(archivo_excel):
    try:
        os.remove(archivo_excel)
        logging.info(f"Archivo anterior eliminado: {archivo_excel}")
    except Exception as e:
        logging.warning(f"No se pudo eliminar el archivo {archivo_excel}: {e}")

# =========================================================
# FUNCIÓN AUXILIAR
# =========================================================
def convertir_valor(valor):
    if valor is None:
        return ""
    try:
        if hasattr(valor, "toString"):
            return str(valor.toString())
        if isinstance(valor, (bytes, bytearray)):
            return valor.decode("utf-8", errors="replace")
        return str(valor)
    except Exception as e:
        logging.warning(f"Error convirtiendo valor {valor}: {e}")
        return ""

# =========================================================
# EJECUCIÓN PRINCIPAL
# =========================================================
try:
    if not jpype.isJVMStarted():
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            f"-Djava.class.path={jdbc_driver_loc}"
        )

    conn = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {"user": iris_user, "password": iris_password},
        jdbc_driver_loc
    )

    cursor = conn.cursor()

    query = """
     SELECT
        ADM.PAADM_ADMNO AS episodio,
        CASE PAADM_VISITSTATUS
            WHEN 'A' THEN 'Actual'
            WHEN 'C' THEN 'Suspendido'
            WHEN 'D' THEN 'Egreso'
            WHEN 'P' THEN 'Pre Admision'
            WHEN 'R' THEN 'Liberado'
            WHEN 'N' THEN 'No Atendido'
        END AS EstadoAtencion,
        PAADM_ADMDATE AS fecha_admision,
        PAADM_ADMTIME AS hora_admision,
        TRANS.TRANS_StartDate AS fecha_inicio_servicio,
        TRANS.TRANS_StartTime AS hora_inicio_servicio,
        TRANS.TRANS_EndDate   AS fecha_termino_servicio,
        TRANS.TRANS_EndTime   AS hora_termino_servicio,
        WARD.WARD_Desc AS servicio,
        PAADM_DischgDate AS fechaAltaAdm,
        PAADM_DischgTime AS horaAltaAdm,
        ADM.PAADM_PAPMI_DR->PAPMI_ID AS rut_paciente,
        ADM.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name2 AS nombre_paciente,
        ADM.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name  AS apellidop_paciente,
        ADM.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name3 AS apellidom_paciente
    FROM PA_Adm ADM
    INNER JOIN PA_AdmTransaction TRANS
        ON ADM.PAADM_RowID = TRANS.TRANS_ParRef
    LEFT JOIN PAC_Ward WARD
        ON TRANS.TRANS_Ward_DR = WARD.WARD_RowID
    WHERE ADM.PAADM_ADMDATE >= '2026-01-01'
        AND PAADM_HOSPITAL_DR->HOSP_code = '112100'
        AND ADM.PAADM_TYPE = 'I'
        AND WARD.WARD_Desc IS NOT NULL
        AND ADM.PAADM_VISITSTATUS IN ('A','D')
        AND ADM.PAADM_ADMDATE < DATEADD('day', 1, CURRENT_DATE)
        AND WARD_LocationDR NOT IN (4709,3140)
    ORDER BY
        ADM.PAADM_ADMNO,
        TRANS.TRANS_StartDate,
        TRANS.TRANS_StartTime;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "9_hospitalizados_dias"

    ws.append([str(col) for col in columns])
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")

    for row in rows:
        ws.append([convertir_valor(v) for v in row])

    wb.save(archivo_excel)
    logging.info(f"Archivo crudo generado correctamente: {archivo_excel}")

except Exception as e:
    logging.error(f"Error general: {e}")

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
