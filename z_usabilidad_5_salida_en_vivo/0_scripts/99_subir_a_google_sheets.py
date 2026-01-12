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

SHEET_NAME = '5ta etapa implementacion registro clinico electronico trakcare - Hospital del Salvador'

BASE_ENTRADA = 'z_usabilidad_5_salida_en_vivo/1_entrada'
BASE_PROCESO = 'z_usabilidad_5_salida_en_vivo/2_proceso'

# ======================================================
# DEFINICIÓN DE ARCHIVOS → HOJAS
# ======================================================
ARCHIVOS_ENTRADA = {
    '1_profesionales.xlsx': 'profesionales',
    '2_ingreso_medico.xlsx': 'ingreso_medico',
    '3_diagnosticos.xlsx': 'diagnosticos',
    '4_altas_medicas.xlsx': 'altas_medicas',
    '5_epicrisis.xlsx': 'epicrisis',
    '6_evoluciones.xlsx': 'evoluciones',
    '7_pacientes_hospitalizados.xlsx': 'pacientes_hospitalizados',
    '8_cuestionario_QTCERIESGO.xlsx': 'cuestionario_qt'
}

ARCHIVO_RESUMEN = {
    'df_clinico_FILTRADO_eventos.xlsx': 'resumen'
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
# FUNCIÓN GENÉRICA DE SUBIDA
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

    # Normalizar fechas
    for col in df.columns:
        if 'fecha' in col.lower():
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

    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ws.clear()
    ws.update(values=[[f"Última actualización: {fecha_actual}"]], range_name="A1")
    ws.update(values=data, range_name="A2")

    logging.info(f"Hoja '{nombre_hoja}' actualizada. Filas: {len(df)}")

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
# SUBIR RESUMEN (LOOKER)
# ======================================================
for archivo, hoja in ARCHIVO_RESUMEN.items():
    ruta = os.path.join(BASE_PROCESO, archivo)
    subir_excel_a_hoja(ruta, hoja)

# ======================================================
# SUBIR ARCHIVOS DE ENTRADA
# ======================================================
for archivo, hoja in ARCHIVOS_ENTRADA.items():
    ruta = os.path.join(BASE_ENTRADA, archivo)
    subir_excel_a_hoja(ruta, hoja)

logging.info("Todas las hojas fueron subidas correctamente")
