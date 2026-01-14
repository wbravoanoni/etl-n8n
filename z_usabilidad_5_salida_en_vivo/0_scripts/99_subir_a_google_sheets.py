import pandas as pd
import logging
import time
from datetime import datetime
from google.oauth2.service_account import Credentials
import gspread
from dotenv import load_dotenv
import os

# ======================================================
# CONFIGURACIÓN LOGS
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# ======================================================
# CARGAR ENV
# ======================================================
load_dotenv()
service_account_path = os.getenv('GSHEET_CREDENTIALS')

SHEET_NAME = (
    '5ta etapa implementacion registro clinico electronico '
    'trakcare - Hospital del Salvador'
)

# ======================================================
# RUTA ÚNICA DE RESULTADOS
# ======================================================
BASE_RESULTADOS = 'z_usabilidad_5_salida_en_vivo/3_resultados'

# ======================================================
# ARCHIVOS FINALES → HOJAS
# ======================================================
ARCHIVOS_RESULTADOS = {
    '1_profesionales_pro.xlsx': 'profesionales',
    '2_ingreso_medico_pro.xlsx': 'ingreso_medico',
    '3_diagnosticos_pro.xlsx': 'diagnosticos',
    '4_altas_medicas_pro.xlsx': 'altas_medicas',
    '5_epicrisis_pro.xlsx': 'epicrisis',
    '6_evoluciones_pro.xlsx': 'evoluciones',
    '7_pacientes_hospitalizados_pro.xlsx': 'pacientes_hospitalizados',
    '8_cuestionario_QTCERIESGO_pro.xlsx': 'cuestionario_qt'
}

# ======================================================
# ARCHIVO RESUMEN (LOOKER / DASHBOARD)
# ======================================================
ARCHIVO_RESUMEN = {
    '9_df_clinico_FILTRADO_eventos_pro.xlsx': 'resumen'
}

# ======================================================
# CONEXIÓN GOOGLE SHEETS
# ======================================================
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

try:
    sh = gc.open(SHEET_NAME)
    logging.info(f"Google Sheet encontrado: {SHEET_NAME}")
except gspread.SpreadsheetNotFound:
    logging.error(f"No se encontró el Google Sheet '{SHEET_NAME}'")
    raise

# ======================================================
# FUNCIÓN GENÉRICA DE SUBIDA (LIMPIA PARA LOOKER)
# ======================================================
def subir_excel_a_hoja(ruta_excel, nombre_hoja):
    logging.info(f"Subiendo {ruta_excel} → hoja '{nombre_hoja}'")

    if not os.path.exists(ruta_excel):
        logging.warning(f"Archivo no encontrado: {ruta_excel}")
        return

    df = pd.read_excel(ruta_excel)

    if df.empty:
        logging.warning(f"Archivo vacío, se omite: {ruta_excel}")
        return

    # Fecha de actualización (misma para todas las filas)
    fecha_actualizacion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df['fecha_actualizacion'] = fecha_actualizacion

    # Normalizar columnas de fecha (Looker-friendly)
    for col in df.columns:
        if 'fecha' in col.lower() and col != 'fecha_actualizacion':
            try:
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
            except Exception:
                pass

    df = df.astype(str)
    data = [list(df.columns)] + df.values.tolist()

    # Asegurar hoja
    try:
        ws = sh.worksheet(nombre_hoja)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=nombre_hoja, rows="100", cols="50")

    ws.clear()
    ws.update(values=data, range_name="A1")

    logging.info(f"Hoja '{nombre_hoja}' actualizada. Filas: {len(df)}")

    # Autoajustar columnas
    time.sleep(1)
    try:
        sh.batch_update({
            "requests": [{
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(df.columns) + 1
                    }
                }
            }]
        })
    except Exception:
        pass

# ======================================================
# FUNCIÓN PARA MOVER HOJA AL INICIO
# ======================================================
def mover_hoja_al_inicio(nombre_hoja):
    try:
        ws = sh.worksheet(nombre_hoja)

        sh.batch_update({
            "requests": [{
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": ws.id,
                        "index": 0
                    },
                    "fields": "index"
                }
            }]
        })

        logging.info(f"Hoja '{nombre_hoja}' movida al inicio")

    except Exception as e:
        logging.warning(f"No se pudo mover la hoja '{nombre_hoja}': {e}")

# ======================================================
# SUBIR ARCHIVOS DE RESULTADOS
# ======================================================
for archivo, hoja in ARCHIVOS_RESULTADOS.items():
    ruta = os.path.join(BASE_RESULTADOS, archivo)
    subir_excel_a_hoja(ruta, hoja)

# ======================================================
# SUBIR RESUMEN (LOOKER)
# ======================================================
for archivo, hoja in ARCHIVO_RESUMEN.items():
    ruta = os.path.join(BASE_RESULTADOS, archivo)
    subir_excel_a_hoja(ruta, hoja)

# ======================================================
# MOVER RESUMEN AL INICIO
# ======================================================
mover_hoja_al_inicio('resumen')

logging.info("Todas las hojas fueron subidas correctamente")
