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
        logging.FileHandler("z_usabilidad_5_salida_en_vivo/logs/8_cuestionario_QTCERIESGO.log"),
        logging.StreamHandler()
    ])

load_dotenv()

jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

fecha_hoy = datetime.now().strftime('%Y-%m-%d')
fecha_ejecucion = datetime.now().strftime('%Y-%m-%d_%H%M%S')
archivo_excel = f"z_usabilidad_5_salida_en_vivo/1_entrada/8_cuestionario_QTCERIESGO.xlsx"


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
                QUESPAAdmDR->PAADM_ADMNo AS episodio,
                QUESPAPatMasDR->PAPMI_ID AS rut_paciente,
                CONVERT(VARCHAR, PAADM_ADMDATE, 105) as fecha_admision,
                QUESDate AS fecha_registro,
                QUESTime AS hora_registro,
                QUESUserDR->SSUSR_Initials AS rut_usuario,
                QUESUserDR->SSUSR_Name AS nombre_usuario,
                PAADM_CurrentWard_DR->WARD_Desc AS servicio,
                CT_CarPrvTp.CTCPT_Desc AS tipo_profesional,
                PAADM_Hospital_DR->HOSP_Desc AS hospital_episodio_registro
            FROM 
                questionnaire.QTCERIESGO a
                INNER JOIN SS_User b ON a.QUESUserDR = b.SSUSR_RowId
                LEFT JOIN PA_Adm c ON a.QUESPAAdmDR = c.PAADM_RowID
                LEFT JOIN CT_CareProv ON b.SSUSR_CareProv_DR = CT_CareProv.CTPCP_RowId1
                LEFT JOIN CT_CarPrvTp ON CT_CareProv.CTPCP_CarPrvTp_DR = CT_CarPrvTp.CTCPT_RowId
            WHERE 
                QUESDate >= '2026-01-01'
                AND PAADM_ADMDATE >= '2026-01-01'
                -- AND PAADM_Hospital_DR->HOSP_RowId = 10448  -- Si necesitas filtrar por hospital específico
                AND PAADM_CurrentWard_DR IN (416, 402,417,509,428,415);
        """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "7_pacientes_hospitalizados"

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