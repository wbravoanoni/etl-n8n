import jaydebeapi
import jpype
import openpyxl
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

# Configuración de logs
logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("z_usabilidad_5_salida_en_vivo/logs/4_altas_medicas.log"),
        logging.StreamHandler()
    ])

load_dotenv()

jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

fecha_hoy = datetime.now().strftime('%Y-%m-%d')
archivo_excel = f"z_usabilidad_5_salida_en_vivo/1_entrada/4_altas_medicas.xlsx"

if os.path.exists(archivo_excel):
    try:
        os.remove(archivo_excel)
        print(f"Archivo anterior eliminado: {archivo_excel}")
        logging.info(f"Archivo anterior eliminado: {archivo_excel}")
    except Exception as e:
        print(f"No se pudo eliminar el archivo {archivo_excel}: {e}")
        logging.warning(f"No se pudo eliminar el archivo {archivo_excel}: {e}")

def convertir_valor(cell):
    try:
        if cell is None:
            return ""
        if hasattr(cell, "toString"): 
            return str(cell.toString())
        if isinstance(cell, bytes):
            return cell.decode("utf-8", errors="replace")
        return str(cell)
    except Exception as e:
        logging.warning(f"No se pudo convertir valor '{cell}' ({type(cell)}): {e}")
        return "[ERROR]"

try:
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )
    cursor = conn_iris.cursor()

    query = """
        SELECT DISTINCT
        PAADM_DepCode_DR->CTLOC_Desc                                          AS "Local de atención",
        PAADM_DepCode_DR,
        PAADM_ADMNO                                                           AS "Nro Episodio",
        CONVERT(VARCHAR, PAADM_ADMDATE, 105)                                  AS "Fecha episodio",
        CONVERT(VARCHAR, PAADM_ADMTIME, 108)                                  AS "Hora episodio",
        PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name2                           AS "Nombres",
        PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name                            AS "Apellido Paterno",
        PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name3                           AS "Apellido Materno",
        PAADM_PAPMI_DR->PAPMI_ID                                              AS "RUN",
        PAADM_PAPMI_DR->PAPMI_NO                                              AS "Nro Registro",
        PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_AgeYr                           AS "Edad",
        PAADM_PAPMI_DR->PAPMI_Sex_DR->CTSEX_Desc                              AS "Sexo",
        -- ALTA MÉDICA
        PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_ModeOfSeparation_DR->CTDSP_Desc AS "Tipo de Alta",
        PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_Date AS "Fecha Alta",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_ICDCode_DR->MRCID_Desc AS "Diagnóstico Principal",
        PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_ClinicalOpinion AS "Indicaciones al Alta",
        PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_CareProv_DR->CTPCP_Desc AS "Profesional Alta",
        PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_UpdateUser_DR->SSUSR_Initials
            || ' ' ||
        PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_UpdateUser_DR->SSUSR_Name AS "Usuario Registro",
        PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_DischargeSummaryType_DR,
        PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_CareProv_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc
        FROM PA_ADM
        WHERE PAADM_HOSPITAL_DR->HOSP_code='112100'
        AND PAADM_CurrentWard_DR in (416,402,417,509,428,422,415)
        AND PAADM_RowID > 0
        AND PAADM_TYPE = 'I'
        AND PAADM_ADMDATE >='2025-01-01'
        AND PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_Date>='2026-01-07'
        -- SOLO ALTAS MÉDICAS
        AND PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_ModeOfSeparation_DR IS NOT NULL
        AND PAADM_PAAdm2_DR->PA_Adm2DischargeSummary->DIS_PADischargeSummary_DR->DIS_CareProv_DR->CTPCP_CarPrvTp_DR->CTCPT_Desc IN (
        'Médico',
        'Médico Cirujano', 
        'Psiquiatria',
        'Ortoproteisista',
        'Odontólogo',
        'Dentista',
        'Cirujano Dentista',
        'Ginecólogo',
        'Ginecóloga'
);
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "4_altas_medicas"

    ws.append([str(col) for col in columns])
    for cell in ws[1]:
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center")

    for row in rows:
        safe_row = [convertir_valor(cell) for cell in row]
        ws.append(safe_row)

    for col in ws.columns:
        max_len = max(len(str(cell.value)) if cell.value else 0 for cell in col)
        col_letter = col[0].column_letter
        ws.column_dimensions[col_letter].width = max_len + 2

    wb.save(archivo_excel)
    logging.info(f"Archivo Excel generado correctamente: {archivo_excel}")

except Exception as e:
    logging.error(f"Error general: {e}")
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn_iris' in locals():
        conn_iris.close()
    if jpype.isJVMStarted():
        jpype.shutdownJVM()