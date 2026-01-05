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
        logging.FileHandler("logs/z_teamcoder_descargaReporte.log"),
        logging.StreamHandler()
    ])

load_dotenv()

jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

fecha_hoy = datetime.now().strftime('%Y-%m-%d')
archivo_excel = f"z_teamcoder/entrada/z_teamcoder_descargaReporte_original.xlsx"

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
            CASE 
                WHEN a.PAADM_DepCode_DR->CTLOC_Hospital_DR = '10448' THEN '112100'
                ELSE 'DESCONOCIDO'
            END AS "HOSPITAL",
            r.RTMAS_MRNo AS "HISTORIA",
            a.PAADM_ADMNo as "EPISODIO",
            CONVERT(VARCHAR(10), a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Dob, 103) || ' 00:00:00' AS FECNAC,
            a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Sex_DR->CTSEX_code as "SEXO",
            CONVERT(VARCHAR(10), a.PAADM_AdmDate, 103) || ' ' ||
            COALESCE(CONVERT(VARCHAR(8), a.PAADM_AdmTime, 108), '00:00:00') AS FECING,
            '1' as TIPING,
            a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_AuxInsType_DR->AUXIT_Desc AS "REGCON_01",
            a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_IndigStat_DR->INDST_code AS "ETNIA",
            a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_CityCode_DR->CTCIT_Province_DR->PROV_Code AS "DIST_PAC",
            a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_CityCode_DR->CTCIT_Code AS "MRES",
            a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Nation_DR->CTNAT_Code AS "PAIS",
            REPLACE(
                CONVERT(VARCHAR, a.PAADM_DischgDate, 103) || ' ' ||
                CONVERT(VARCHAR, a.PAADM_DischgTime, 108),
                '-', '/'
            ) AS FECALT,
            CASE 
                WHEN PAADM_DischCond_DR->DISCON_Desc = 'Fallecido' THEN '2'
                ELSE '1'
            END AS "TIPALT",
            a.PAADM_DepCode_DR->CTLOC_Code AS "SERVING",
            a.PAADM_CurrentWard_DR->WARD_Code AS "SERVALT",
            CASE UPPER(TRIM(a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_PatType_DR->PTYPE_Code))
                WHEN 'ID' THEN '1'
                WHEN 'SD' THEN '3'
                WHEN 'PN' THEN '3'
                WHEN 'OI' THEN '4'
                ELSE 'DESCONOCIDO'
            END AS TIPO_CIP,
            a.PAADM_MedDischDoc_DR->CTPCP_Spec_DR->CTSPC_Code AS ESPECIALIDAD,
            '12' as "SERVICIO_SALUD",
            '' as "FEC_INT_1",
            '' as FECPART,
            '' as TGESTAC,
            '' as RN_PESO_01,
            '' as RN_SEXO_01,
            '' as RN_PESO_02,
            '' as RN_SEXO_02,
            '' as RN_PESO_03,
            '' as RN_SEXO_03,
            '' as RN_PESO_04,
            '' as RN_SEXO_04,
            '' as TRASHOSPITAL,
            '' as PROCHOSPITAL,
            a.PAADM_MedDischDoc_DR->CTPCP_Code as "MEDICOALT",
            a.PAADM_PAPMI_DR->PAPMI_ID as "CIP",
            '' as PROC,
            '1' AS TIPO_ACTIVIDAD,
            '' as RN_EST_01,
            '' as RN_EST_02,
            '' as RN_EST_03,
            '' as RN_EST_04,
            '' as ESTADO,
            a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name2 as "NOMBRE",
            a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name as "APELLIDO1",
            a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name3 as "APELLIDO2",
            '00' AS "PROGRAMA",
            '' as RN_COND_ING_01,
            '' as RN_COND_ING_02,
            '' as RN_COND_ING_03,
            '' as RN_COND_ING_04,
            '' as CUMPLIMENTACION,
            '' as MED_INT_1,
            '' as ESPINT,
            '' as TIP_PAB,
            '' as TRAS_FEC_01,
            '' as TRAS_SRV_01,
            '' as TRAS_FEC_02,
            '' as TRAS_SRV_02,
            '' as TRAS_FEC_03,
            '' as TRAS_SRV_03,
            '' as TRAS_FEC_04,
            '' as TRAS_SRV_04,
            '' as TRAS_FEC_05,
            '' as TRAS_SRV_05,
            '' as TRAS_FEC_06,
            '' as TRAS_SRV_06,
            '' as TRAS_FEC_07,
            '' as TRAS_SRV_07,
            '' as TRAS_FEC_08,
            '' as TRAS_SRV_08,
            '' as TRAS_FEC_09,
            '' as TRAS_SRV_09,
            '' as TRAS_FEC_10,
            '' as TRAS_SRV_10,
            CONVERT(VARCHAR(10), a.PAADM_InpatBedReqDate, 105) || ' ' ||
            COALESCE(CONVERT(VARCHAR(8), a.PAADM_InpatBedReqTime, 108), '00:00:00') AS "FEC_URGENCIAS",
            '99' as OCUPACION,
            '99' AS CAT_OCP
        FROM PA_ADM a
        LEFT JOIN (
            SELECT RTMAS_PatNo_DR, RTMAS_Hospital_DR, MIN(RTMAS_MRNo) AS RTMAS_MRNo
            FROM RT_Master
            GROUP BY RTMAS_PatNo_DR, RTMAS_Hospital_DR
        ) r ON r.RTMAS_PatNo_DR = a.PAADM_PAPMI_DR 
        AND r.RTMAS_Hospital_DR = a.PAADM_Hospital_DR
        WHERE PAADM_Type='I' 
        AND PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448
        AND PAADM_DischgDate BETWEEN DATEADD('d', -1, CURRENT_DATE) AND CURRENT_DATE
        AND PAADM_DepCode_DR->CTLOC_RowID<>'4771';
    """

# a.PAADM_DepCode_DR->CTLOC_Hospital_DR as "HOSPITAL",
# a.PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Occupation_DR->CTOCC_Desc as "DESCRIPCION_OCUPACION",
#  PAADM_DepCode_DR->CTLOC_Desc AS "ESPECIALIDAD",

#CASE 
#    WHEN a.PAADM_MainMRADM_DR->MRADM_DischClassif_DR->DSCL_Desc = 'Died in ED' THEN '2'
#    WHEN a.PAADM_MainMRADM_DR->MRADM_DischClassif_DR->DSCL_Desc = 'Dead on arrival' THEN '2'
#    ELSE '1'
#END AS "TIPALT",

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "z_teamcoder_descargaReporte"

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