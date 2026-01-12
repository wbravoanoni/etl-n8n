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
        logging.FileHandler("z_usabilidad_5_salida_en_vivo/logs/5_epicrisis.log"),
        logging.StreamHandler()
    ])

load_dotenv()

jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

fecha_hoy = datetime.now().strftime('%Y-%m-%d')
archivo_excel = f"z_usabilidad_5_salida_en_vivo/1_entrada/5_epicrisis.xlsx"

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
        SELECT 
        %nolock PAADM_DepCode_DR->CTLOC_Hospital_DR->HOSP_Code as HOSP_Code,
        isnull(PAADM_PAPMI_DR->PAPMI_Name,'') ||', '|| isnull(PAADM_PAPMI_DR->PAPMI_Name3,'') ||', '|| isnull(PAADM_PAPMI_DR->PAPMI_Name2,'') as NombrePaciente,
        PAADM_PAPMI_DR->PAPMI_ID as RUNPaciente,
        PAADM_PAPMI_DR->PAPMI_Sex_DR->CTSEX_Code as SexoCodigo, PAADM_PAPMI_DR->PAPMI_Sex_DR->CTSEX_Desc as Sexo,
        PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_CityCode_DR->CTCIT_Desc as Comuna,
        PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_HCP_DR->HCP_Desc as "EstablecimientoInscripción",
        PAADM_CurrentWard_DR->WARD_Code as ServicioClinicoCodigo, 
        PAADM_CurrentWard_DR->WARD_Desc as ServicioClinico,
        convert( varchar, PAADM_AdmDate, 103 ) as FechaAtencion,
        convert( varchar, PAADM_DischgDate, 103 ) as FechaEgreso,
        convert( varchar, DIS_Date, 103 ) as FechaAlta,
        DIS_DischargeDestination_DR->DDEST_Desc as DestinoEgreso,
        PAADM_AdmNo as NumeroEpisodio,
        PAADM_CurrentWard_DR->WARD_Desc AS "Local Actual",
        PAADM_CurrentWard_DR->WARD_Code as "code",
        DIS_CareProv_DR->CTPCP_Desc as MedicoContacto,
        PAADM_Hospital_DR->HOSP_Desc as Hosp,
        PAADM_Epissubtype_DR->SUBT_Desc as subtipoepi,
        DIS_Procedures as TratamientoRecibido,
        DIS_TextBox4 as ProximoControl,
        DIS_ClinicalOpinion as IndicacionesAlAlta,
        DIS_PrincipalDiagnosis as DiagnosticoQueMotivoIngreso,
        DIS_CareProv_DR->CTPCP_Code as rutMedicoContacto,
        DIS_CareProv_DR->CTPCP_Desc as MedicoContacto,
        PAADM_CurrentWard_DR
        FROM 
        PA_Adm
        LEFT JOIN PA_DischargeSummary on
        DIS_RowId = ( select %nolock top 1 DIS_PADischargeSummary_DR from PA_Adm2DischargeSummary where DIS_ParRef = PAADM_RowId order by dis_childsub desc )
        WHERE
        PAADM_ADMDATE >= '2025-01-01' 
        AND PAADM_TYPE = 'I'
        and PA_DischargeSummary.DIS_Date >= '2026-01-01'
        -- AND PAADM_Hospital_DR = 10448
        AND PAADM_Type = 'I' 
        AND PAADM_VISITSTATUS='A'
        AND PAADM_CurrentWard_DR in (416,402,417,399,428,415);
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "5_epicrisis"

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