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
        logging.FileHandler("logs/z_reporte_semanal_oficina_ges.log"),
        logging.StreamHandler()
    ])

load_dotenv()

jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')
iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

fecha_hoy = datetime.now().strftime('%Y-%m-%d')
archivo_excel = f"z_reporte_semanal_oficina_ges/resultados/reporte_semanal_oficina_ges.xlsx"

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
        SELECT %NOLOCK DISTINCT
            RBOP_PAADM_DR->PAADM_ADMNO AS "Nro Episodio",
            RBOP_PAADM_DR->PAADM_EpisSubType_DR->SUBT_Desc AS "Subtipo Episodio",
            CONVERT(VARCHAR, RBOP_PAADM_DR->PAADM_AdmDate, 105) AS "Fecha Admisión Hospitalizado",
            (
                SELECT TOP 1 
                    t.TRANS_StartDate
                FROM SQLUser.PA_AdmTransaction t
                WHERE t.TRANS_ParRef->PAADM_ADMNo = RBOP_PAADM_DR->PAADM_ADMNo
                AND t.TRANS_Status_DR = 5
                AND t.TRANS_Ward_DR IS NOT NULL  -- solo movimientos con cama/unidad asignada
                ORDER BY t.TRANS_StartDate DESC
            ) AS "Fecha Ingreso Hospitalizado",
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_ID AS "RUN Paciente",
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name2 AS "Nombres Paciente",
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name AS "Apellido Paterno",
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name3 AS "Apellido Materno",
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Dob AS "Fecha de Nacimiento",
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_AgeYr AS "Edad",
            RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Sex_DR->CTSEX_Desc AS "Sexo",
            RBOP_Loc_DR->CTLOC_Desc AS "Pabellón",
            RBOP_Resource_DR->RES_Desc as "Quirófano",
            (CASE WHEN RBOP_Appoint_DR IS NULL THEN 'No' ELSE 'Sí' END) AS "Programada (S/N)",
            pr.RBP_Desc AS "Prioridad",
            CASE 
                WHEN RBOP_BookingType = 'EL' THEN 'Electiva'
                WHEN RBOP_BookingType = 'EM' THEN 'Urgencia'
                ELSE 'Otro'
            END AS "Tipo de Cirugía(Electiva/Urgencia)",
                (CASE WHEN RB_OperatingRoom.RBOP_Status = 'B' THEN 'Agendado'
                    WHEN RB_OperatingRoom.RBOP_Status = 'X' THEN 'Suspendido'
                    WHEN RB_OperatingRoom.RBOP_Status = 'CL'THEN 'Cerrado'
                    WHEN RB_OperatingRoom.RBOP_Status = 'C' THEN 'Confirmado'
                    WHEN RB_OperatingRoom.RBOP_Status = 'P' THEN 'Postergado'
                    WHEN RB_OperatingRoom.RBOP_Status = 'D' THEN 'Realizado'
                    WHEN RB_OperatingRoom.RBOP_Status = 'R' THEN 'Solicitado'
                    WHEN RB_OperatingRoom.RBOP_Status = 'A' THEN 'Recepcionado'
                    WHEN RB_OperatingRoom.RBOP_Status = 'N' THEN 'No Listo'
                    WHEN RB_OperatingRoom.RBOP_Status = 'SK'THEN 'Enviado por Reconocido'
                    WHEN RB_OperatingRoom.RBOP_Status = 'SF'THEN 'Enviado por'
                    ELSE 'Otro' END) AS "Estado de Cirugía",
                    RBOP_OperDepartment_DR->CTLOC_Dep_DR->DEP_Desc AS "Especialidad",
                    convert(varchar,RBOP_DateOper,105) AS "Fecha Cita Cirugía",
                    convert(varchar,RBOP_TimeOper,108) AS "Hora Agendada",
                (CASE 
                WHEN RBOP_DaySurgery = 'Y' THEN 'Sí' 
                WHEN RBOP_DaySurgery = 'N' THEN 'No' 
                ELSE 'No especifica' 
            END) AS "Cirugía Ambulatoria",
            ANAOP_CancelReason AS "Causal de Suspensión",
            RBOP_Operation_DR->OPER_Desc AS "Descripción Código 1",
            RBOP_Operation_DR->OPER_Code AS "Código 1",
            RBOP_Surgeon_DR->CTPCP_Desc as "Cirujano",
            OR_An_Oper_SecondaryProc->SECPR_Operation_DR->OPER_Code as "Código 2",
            RBOP_PreOpDiagnosis AS "DG de Agendamiento",
            RBOP_PreopDiagn_DR->MRCID_Desc as "Diagnóstico PreOperatorio",
            RBOP_PreopDiagn_DR->MRCID_Code AS "Cod (CIE-10) Diagnóstico PreOperatorio",
            RBOP_Operation_DR->OPER_Desc as "Procedimiento Quirúrgico",
            CASE 
                WHEN RBOP_YesNo3 = 'Y' THEN 'Sí'
                WHEN RBOP_YesNo3 = 'N' THEN 'No'
            ELSE ''
            END AS "Es GES (S / N)"
        FROM SQLUser.RB_OperatingRoom
        LEFT JOIN SQLUser.OR_Anaesthesia 
            ON RB_OperatingRoom.RBOP_PAADM_DR = OR_Anaesthesia.ANA_PAADM_ParRef
        LEFT JOIN SQLUser.OR_Anaest_Operation 
            ON OR_Anaesthesia.ANA_RowId = OR_Anaest_Operation.ANAOP_Par_Ref
        LEFT JOIN SQLUser.ORC_RoomBookPriority pr
            ON pr.RBP_RowId = RBOP_Priority_DR    
        WHERE
            RBOP_DateOper BETWEEN DATEADD(DAY, -7, CURRENT_DATE) AND CURRENT_DATE
            AND RBOP_RowId > 0
            AND RBOP_PAADM_DR->PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448;
    """

    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    wb = Workbook()
    ws = wb.active
    ws.title = "Reporte Semanal Oficina GES"

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