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

# === CONFIGURACIÓN DE CONSOLA UTF-8 ===
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# === CONFIGURACIÓN DE LOGS ===
logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("z_reportes_google_sheet/logs/operaciones_gsheet.log", encoding='utf-8'),
        logging.StreamHandler()
    ])

# === CARGAR VARIABLES DE ENTORNO ===
load_dotenv()
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')
service_account_path = os.getenv('GSHEET_CREDENTIALS')
sheet_name = os.getenv('GSHEET_NAME', 'Listado de Operaciones - Hospital del Salvador')

# === QUERY IRIS ===
query = """
SELECT 
  OPER_Code AS "CIRUGIA/PROCEDIMIENTO CODIGO",
  OPER_Desc AS "CIRUGIA/PROCEDIMIENTO DESCRIPCION",
  ARCIM_Code AS "ITEM CODIGO",
  ARCIM_Desc AS "ITEM DESCRIPCION",
  OPER_DateActiveFrom AS "ITEM FECHA DESDE",
  OPER_ActiveDateTo AS "ITEM FECHA HASTA",
  ARC_ItemHosp.HOSP_Hospital_DR->HOSP_Desc AS "HOSPITALES ASOCIADOS"
FROM ORC_Operation
LEFT JOIN ARC_ItmMast ON ORC_Operation.OPER_ARCIM_DR = ARC_ItmMast.ARCIM_RowId 
LEFT JOIN ARC_ItemHosp ON ARC_ItmMast.ARCIM_RowId = ARC_ItemHosp.HOSP_ParRef 
"""

# === CONEXIÓN A IRIS ===
try:
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    logging.info("Conectando a InterSystems IRIS...")
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )

    cursor = conn_iris.cursor()
    cursor.execute(query)

    # Convertir resultados a tipos nativos de Python
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
    df.columns = [str(c) for c in df.columns]
    df = df.copy(deep=True)
    df = df.astype(str)
    logging.info(f"Consulta ejecutada correctamente. Filas obtenidas: {len(df)}")

    # Limpiar filas vacías (evita fila vacía en A4)
    df = df[~((df["CIRUGIA/PROCEDIMIENTO CODIGO"].str.strip() == "") &
              (df["CIRUGIA/PROCEDIMIENTO DESCRIPCION"].str.strip() == ""))]

    # Preparar data principal
    data_to_upload = [list(df.columns)] + df.values.tolist()

    # Agrupar por código de cirugía
    df_grouped = (
        df.groupby(
            ["CIRUGIA/PROCEDIMIENTO CODIGO", "CIRUGIA/PROCEDIMIENTO DESCRIPCION"],
            dropna=False
        )
        .agg({
            "ITEM CODIGO": lambda x: ", ".join(sorted(set([v for v in x if v]))),
            "ITEM DESCRIPCION": lambda x: ", ".join(sorted(set([v for v in x if v]))),
            "ITEM FECHA DESDE": lambda x: ", ".join(sorted(set([v for v in x if v]))),
            "ITEM FECHA HASTA": lambda x: ", ".join(sorted(set([v for v in x if v]))),
            "HOSPITALES ASOCIADOS": lambda x: ", ".join(sorted(set([v for v in x if v]))),
        })
        .reset_index()
    )

    data_group = [list(df_grouped.columns)] + df_grouped.values.tolist()

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

# === CONEXIÓN A GOOGLE SHEETS ===
try:
    logging.info("Conectando con Google Sheets...")
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_file(service_account_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open(sheet_name)

    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # === ASEGURAR ESTRUCTURA DE HOJAS ===
    existing_sheets = [ws.title for ws in sh.worksheets()]
    logging.info(f"Hojas existentes antes de limpieza: {existing_sheets}")

    keep = ["Cirugías por Hospital", "Cirugías agrupadas por Código"]
    ws = None
    ws_group = None

    # Crear las hojas oficiales si no existen
    try:
        ws = sh.worksheet("Cirugías por Hospital")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Cirugías por Hospital", rows="100", cols="20")

    try:
        ws_group = sh.worksheet("Cirugías agrupadas por Código")
    except gspread.WorksheetNotFound:
        ws_group = sh.add_worksheet(title="Cirugías agrupadas por Código", rows="100", cols="20")

    # Eliminar hojas no deseadas (dejando las dos principales)
    for ws_existente in sh.worksheets():
        if ws_existente.title not in keep:
            if len(sh.worksheets()) > 2:
                sh.del_worksheet(ws_existente)
                logging.info(f"Hoja eliminada: {ws_existente.title}")

    logging.info("Estructura de hojas asegurada correctamente.")

    # === HOJA 1: DETALLE ===
    ws.clear()
    ws.update(values=[[f"Última actualización: {fecha_actual}"]], range_name="A1")
    ws.update(values=data_to_upload, range_name="A2")
    logging.info("Hoja 'Cirugías por Hospital' actualizada correctamente.")

    # === HOJA 2: AGRUPADA ===
    ws_group.clear()
    ws_group.update(values=[[f"Última actualización: {fecha_actual}"]], range_name="A1")
    ws_group.update(values=data_group, range_name="A2")
    logging.info("Hoja 'Cirugías agrupadas por Código' actualizada correctamente.")

    # === AJUSTAR ANCHO DE COLUMNAS (MEJORADO) ===
    time.sleep(2)  # Espera para que Sheets procese la carga

    try:
        sh.batch_update({
            "requests": [
                # Autoajuste hoja 1
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
                # Autoajuste hoja 2
                {
                    "autoResizeDimensions": {
                        "dimensions": {
                            "sheetId": ws_group.id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": len(df_grouped.columns) + 1
                        }
                    }
                },
                # Ancho mínimo hoja 1 (backup)
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
                },
                # Ancho mínimo hoja 2 (backup)
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws_group.id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": len(df_grouped.columns) + 1
                        },
                        "properties": {"pixelSize": 180},
                        "fields": "pixelSize"
                    }
                }
            ]
        })
        logging.info("Ancho de columnas ajustado correctamente en ambas hojas.")
    except Exception as e:
        logging.warning(f"No se pudo ajustar el ancho de columnas: {e}")

except gspread.SpreadsheetNotFound:
    logging.error(f"No se encontró la hoja '{sheet_name}'. "
                  f"Verifica que exista y esté compartida con la cuenta de servicio.")
except Exception as e:
    logging.error(f"Error general al actualizar Google Sheets: {e}")
    raise
