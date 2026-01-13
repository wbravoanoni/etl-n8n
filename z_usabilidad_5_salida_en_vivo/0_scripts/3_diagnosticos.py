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
        logging.FileHandler("z_usabilidad_5_salida_en_vivo/logs/3_diagnosticos.log"),
        logging.StreamHandler()
    ])

load_dotenv()

jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

fecha_hoy = datetime.now().strftime('%Y-%m-%d')
archivo_excel = f"z_usabilidad_5_salida_en_vivo/1_entrada/3_diagnosticos.xlsx"

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
        PAADM_PAPMI_DR->PAPMI_No AS "nro_de_registro",
        PAADM_ADMNO "nro_episodio",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Date "fecha_creacion",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Time "hora_creacion",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_UpdateDate "fecha_actualizacion",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_UpdateTime "Hora_actualizacion",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_UserCreated_DR->ssusr_initials "run_medico_registra_diagnostico",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_UserCreated_DR->ssusr_name "nombre_medico_registra_diagnostico",
        PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_ICDCode_DR->MRCID_code "codigo_diagnostico",
        PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_ICDCode_DR->MRCID_desc "descripcon_diagnostico",
        PAADM_MainMRADM_DR->MR_Diagnos->MRDIA_suspicion as GES,
        PAADM_MAINMRADM_DR->MR_Diagnos->MR_DiagType->TYP_MRCDiagTyp->DTYP_Desc "tipo_diagnostico",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_DiagStat_DR->DSTAT_Desc "etapa_GES",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Approximate "diagnostico_principal",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Desc "fundamento_y_complemento_del_diagnostico",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Laterality_DR->LATER_Desc "lateridad",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Severity_DR->SEV_desc "severidad",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Active "activo",
        PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_DeletionReason_DR->RCH_Desc "motivo_inactivacion",
        PAADM_CurrentWard_DR,
        PAADM_CurrentWard_DR->WARD_Desc
        FROM 
        PA_Adm
        where PAADM_ADMDATE >= '2025-01-01' 
        AND PAADM_TYPE = 'I'
        AND PAADM_VISITSTATUS='A'
        -- and PAADM_HOSPITAL_DR->HOSP_code='112100'
        and PAADM_MAINMRADM_DR->MR_Diagnos->MRDIA_Date >= '2026-01-07'
        AND PAADM_CurrentWard_DR in (416,402,417,399,428,415)
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "3_diagnosticos"

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