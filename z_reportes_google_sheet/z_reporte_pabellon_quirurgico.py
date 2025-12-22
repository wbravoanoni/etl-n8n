import sys
import jaydebeapi
import jpype
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from dotenv import load_dotenv
import os
import logging
from datetime import datetime
import time

# ======================================================
# CONFIGURACIÓN UTF-8
# ======================================================
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# ======================================================
# LOGS
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("z_reportes_google_sheet/logs/pabellon_quirurgico.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ======================================================
# VARIABLES DE ENTORNO
# ======================================================
load_dotenv()

jdbc_driver_name = os.getenv("JDBC_DRIVER_NAME")
jdbc_driver_loc = os.getenv("JDBC_DRIVER_PATH")
iris_connection_string = os.getenv("CONEXION_STRING")
iris_user = os.getenv("DB_USER")
iris_password = os.getenv("DB_PASSWORD")

service_account_path = os.getenv("GSHEET_CREDENTIALS")
sheet_name = os.getenv(
    "GSHEET_NAME_PABELLON",
    "Pabellón Quirúrgico - Hospital del Salvador"
)

# Fecha dinámica (por defecto hoy)
fecha_reporte = os.getenv(
    "FECHA_REPORTE",
    datetime.now().strftime("%Y-%m-%d")
)

# ======================================================
# QUERY IRIS
# ======================================================
query = f"""
SELECT
    String(RB_OperatingRoom.RBOP_RowId) AS "IDTRAK",
    RBOP_PAADM_DR->PAADM_Hospital_DR->HOSP_Desc AS "Establecimiento",
    RBOP_PAADM_DR->PAADM_ADMNo AS "Episodio",

    CASE RBOP_PAADM_DR->PAADM_Type
        WHEN 'I' THEN 'Hospitalizado'
        WHEN 'O' THEN 'Ambulatorio'
        WHEN 'E' THEN 'Urgencia'
    END AS "Tipo_Episodio",

    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_No AS "Numero_Registro",
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name AS "Apellido_Paterno",
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name3 AS "Apellido_Materno",
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name2 AS "Nombre",

    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name2 || ' ' ||
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name3 || ' ' ||
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_Name AS "Paciente",

    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_ID AS "RUT",

    RB_OperatingRoom.RBOP_Operation_DR->OPER_Code AS "Codigo_Cirugia",
    RB_OperatingRoom.RBOP_Operation_DR->OPER_Desc AS "Descripcion_Cirugia",

    CASE RBOP_BookingType
        WHEN 'EL'  THEN 'Cirugía Electiva'
        WHEN 'ENP' THEN 'Cirugía Electiva No Programada'
        WHEN 'EM'  THEN 'Cirugía Urgencia'
    END AS "Tipo_Cirugia",

    RBOP_PreopDiagn_DR->MRCID_Desc AS "Diagnostico",
    RBOP_EstimatedTime AS "Tiempo_Estimado",
    RBOP_WaitLIST_DR->WL_NO AS "Lista_Espera",
    RBOP_ReasonSuspend_DR->SUSP_Desc AS "Suspension",
    RBOP_Resource_DR->RES_CTLOC_DR->CTLOC_DESC AS "Area",
    RBOP_Resource_DR->RES_Desc AS "Pabellon",

    CONVERT(VARCHAR, RB_OperatingRoom.RBOP_DateOper, 105) AS "Fecha",
    %EXTERNAL(RB_OperatingRoom.RBOP_TimeOper) AS "Hora",

    RBOP_Surgeon_DR->CTPCP_Desc AS "Cirujano",
    RBOP_OperDepartment_DR->CTLOC_Desc AS "Especialidad",

    RBOP_DaySurgery AS "Cirugia_Ambulatoria",
    RBOP_PreopTestDone AS "Cirugia_Condicional",
    RBOP_YesNo3 AS "GES",

    CASE RBOP_Status
        WHEN 'B'  THEN 'AGENDADO'
        WHEN 'N'  THEN 'NO LISTO'
        WHEN 'P'  THEN 'POSTERGADO'
        WHEN 'D'  THEN 'REALIZADO'
        WHEN 'A'  THEN 'RECEPCIONADO'
        WHEN 'R'  THEN 'SOLICITADO'
        WHEN 'X'  THEN 'SUSPENDIDO'
        WHEN 'DP' THEN 'SALIDA'
        WHEN 'CL' THEN 'CERRADO'
        WHEN 'C'  THEN 'CONFIRMADO'
        WHEN 'SF' THEN 'ENVIADO POR'
        WHEN 'SK' THEN 'ENVIADO POR RECONOCIDO'
    END AS "Estado"
FROM RB_OperatingRoom
WHERE RBOP_PAADM_DR->PAADM_Hospital_DR = '10448'
AND RB_OperatingRoom.RBOP_DateOper = CURRENT_DATE + 1
ORDER BY RBOP_Resource_DR->RES_Desc
"""

# ======================================================
# CONEXIÓN A IRIS
# ======================================================
try:
    if not jpype.isJVMStarted():
        jpype.startJVM(
            jpype.getDefaultJVMPath(),
            "-Djava.class.path=" + jdbc_driver_loc
        )

    logging.info("Conectando a InterSystems IRIS...")
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {"user": iris_user, "password": iris_password},
        jdbc_driver_loc
    )

    cursor = conn_iris.cursor()
    cursor.execute(query)

    rows = []
    for row in cursor.fetchall():
        py_row = []
        for cell in row:
            if cell is None:
                py_row.append("")
            else:
                try:
                    py_row.append(str(cell))
                except Exception:
                    py_row.append(f"[ERROR:{type(cell).__name__}]")
        rows.append(py_row)

    columns = [str(desc[0]) for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    df = df.astype(str)

    logging.info(f"Filas obtenidas: {len(df)}")

except Exception as e:
    logging.error(f"Error al ejecutar query en IRIS: {e}")
    raise

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn_iris' in locals():
        conn_iris.close()
    if jpype.isJVMStarted():
        jpype.shutdownJVM()

# ======================================================
# CONEXIÓN GOOGLE SHEETS
# ======================================================
try:
    logging.info("Conectando a Google Sheets...")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(
        service_account_path,
        scopes=scopes
    )
    gc = gspread.authorize(creds)
    sh = gc.open(sheet_name)

    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        ws = sh.worksheet("Detalle Pabellón")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(
            title="Detalle Pabellón",
            rows="1000",
            cols="40"
        )

    ws.clear()
    ws.update(values=[[f"Última actualización: {fecha_actual}"]], range_name="A1")
    ws.update(values=[list(df.columns)] + df.values.tolist(), range_name="A2")

    logging.info("Hoja 'Detalle Pabellón' actualizada correctamente.")

    time.sleep(2)

    sh.batch_update({
        "requests": [
            {
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(df.columns) + 1
                    }
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(df.columns) + 1
                    },
                    "properties": {"pixelSize": 180},
                    "fields": "pixelSize"
                }
            }
        ]
    })

    logging.info("Ancho de columnas ajustado correctamente.")

except gspread.SpreadsheetNotFound:
    logging.error(
        f"No se encontró la hoja '{sheet_name}'. "
        f"Verifica que esté compartida con la cuenta de servicio."
    )
except Exception as e:
    logging.error(f"Error al actualizar Google Sheets: {e}")
    raise
