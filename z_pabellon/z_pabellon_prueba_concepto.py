import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging
from datetime import datetime

# ============================================================
# CONFIGURACIÓN
# ============================================================

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/z_pabellon_prueba_concepto.log"),
        logging.StreamHandler()
    ]
)

# ============================================================
# VARIABLES DE ENTORNO
# ============================================================

jdbc_driver_name = os.getenv("JDBC_DRIVER_NAME")
jdbc_driver_loc = os.getenv("JDBC_DRIVER_PATH")
iris_connection_string = os.getenv("CONEXION_STRING")
iris_user = os.getenv("DB_USER")
iris_password = os.getenv("DB_PASSWORD")

mysql_host = os.getenv("DB_MYSQL_HOST")
mysql_port = int(os.getenv("DB_MYSQL_PORT", 3306))
mysql_user = os.getenv("DB_MYSQL_USER")
mysql_password = os.getenv("DB_MYSQL_PASSWORD")
mysql_database = os.getenv("DB_MYSQL_DATABASE")

# ============================================================
# FUNCIÓN: CREAR TABLA SI NO EXISTE
# ============================================================

def crear_tabla(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS z_pabellon_prueba_concepto (
        numero_episodio VARCHAR(11),
        fecha_admision VARCHAR(10),
        fecha_episodio VARCHAR(10),
        numero_de_registro VARCHAR(9),
        numero_de_ficha VARCHAR(11),
        tipo_de_paciente VARCHAR(32),
        rut_paciente VARCHAR(11),
        nombres_paciente VARCHAR(37),
        apellido_paterno_paciente VARCHAR(22),
        apellido_materno_paciente VARCHAR(22),
        fecha_nacimiento_paciente VARCHAR(10),
        edad_paciente_cirugia VARCHAR(3),
        nacionalidad_paciente VARCHAR(36),
        sexo_paciente VARCHAR(23),
        codigo_comuna_paciente VARCHAR(5),
        descripcion_comuna_paciente VARCHAR(20),
        estado_conyugal_paciente VARCHAR(14),
        pueblo_originario_paciente VARCHAR(23),
        prevision VARCHAR(24),
        plan_prevision VARCHAR(20),
        ambulatoria VARCHAR(1),
        tipo_agendamiento_cirugia VARCHAR(16),
        fecha_agendamiento VARCHAR(10),
        hora_agendamiento VARCHAR(8),
        tiempo_programado_minutos VARCHAR(3),
        estado_pabellon VARCHAR(12),
        paciente_condicional VARCHAR(1),
        es_ges_s_n VARCHAR(1),
        pabellon_programado VARCHAR(1),
        diagnostico_prequirurgico_lista_espera TEXT,
        profilaxis_antibiotica_tcelcpi_s_n VARCHAR(1),
        numero_protocolo_anestesico VARCHAR(11),
        estado_protanest VARCHAR(10),
        nro_protoper VARCHAR(11),
        estado_protocolo_operatorio VARCHAR(10),
        motivo_suspension_protocolo_operatorio VARCHAR(53),
        motivo_suspencion VARCHAR(109),
        categoria_cirugia VARCHAR(13),
        codigo_cirugia_principal VARCHAR(16),
        descripcion_cirugia_principal TEXT,
        descripcion_equipo_quirurgico VARCHAR(46),
        fecha_ingreso_quirofano_anestesico VARCHAR(10),
        hora_ingreso_quirofano_anestesico VARCHAR(8),
        fecha_egreso_quirofano_anestesico VARCHAR(10),
        hora_egreso_quirofano_anestesico VARCHAR(8),
        fecha_inicio_cirugia_anestesico VARCHAR(10),
        hora_inicio_cirugia_anestesico VARCHAR(8),
        fecha_inicio_cirugia_operatorio VARCHAR(10),
        hora_inicio_cirugia_operatorio VARCHAR(8),
        fecha_termino_cirugia_anestesico VARCHAR(10),
        hora_termino_cirugia_anestesico VARCHAR(8),
        fecha_termino_cirugia_operatorio VARCHAR(10),
        hora_termino_cirugia_operatorio VARCHAR(8),
        fecha_inicio_anestesia VARCHAR(10),
        hora_inicio_anestesia VARCHAR(8),
        fecha_termino_anestesia VARCHAR(10),
        hora_termino_anestesia VARCHAR(8),
        fecha_salida_quirofano VARCHAR(10),
        hora_salida_quirofano VARCHAR(8),
        fecha_traslado_sala_egreso VARCHAR(10),
        hora_traslado_sala_egreso VARCHAR(8),
        fecha_egreso_area_quirurgica VARCHAR(10),
        hora_egreso_area_quirurgica VARCHAR(8),
        numero_cirugia VARCHAR(11),
        sitio_operacion_principal VARCHAR(37),
        descripcion_sitio_operacion_secundaria VARCHAR(30),
        descripcion_cirugia_secundaria TEXT,
        rut_cirujano_principal VARCHAR(12),
        cirujano_principal VARCHAR(42),
        rut_cirujano_2 VARCHAR(12),
        cirujano_2 VARCHAR(44),
        rut_cirujano_3 VARCHAR(12),
        cirujano_3 VARCHAR(42),
        rut_cirujano_4 VARCHAR(10),
        cirujano_4 VARCHAR(42),
        rut_cirujano_5 VARCHAR(10),
        cirujano_5 VARCHAR(36),
        rut_arsenalera VARCHAR(26),
        arsenalera VARCHAR(49),
        rut_pabellonera VARCHAR(26),
        pabellonera VARCHAR(49),
        rut_anestesista VARCHAR(11),
        anestesista VARCHAR(39),
        riesgo_anestesico VARCHAR(70),
        tipo_anestesia VARCHAR(49),
        bypass_recuperacion_s_n VARCHAR(1),
        codigo_staff_anestesistas TEXT,
        fechaActualizacion VARCHAR(19)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

# ============================================================
# EJECUCIÓN
# ============================================================

conn_iris = conn_mysql = cursor_iris = cursor_mysql = None

try:
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {"user": iris_user, "password": iris_password},
        jdbc_driver_loc
    )
    cursor_iris = conn_iris.cursor()

    # 🔴 QUERY ORIGINAL – NO MODIFICADA
    query = """
    SELECT %nolock DISTINCT
    RBOP_PAADM_DR->PAADM_ADMNO AS numero_episodio,
    CONVERT(varchar, PA_ADM.PAADM_AdmDate, 105) AS fecha_admision,
    CONVERT(varchar, RBOP_PAADM_DR->PAADM_ADMDATE, 105) AS fecha_episodio,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_NO AS numero_de_registro,
    (
        SELECT RTMAS_MRNo
        FROM RT_Master
        WHERE RTMAS_PatNo_DR = RBOP_PAADM_DR->PAADM_PAPMI_DR
          AND RTMAS_Hospital_DR = RBOP_PAADM_DR->PAADM_DepCode_DR->CTLOC_Hospital_DR
    ) AS numero_de_ficha,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_PatType_DR->PTYPE_Desc AS tipo_de_paciente,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_ID AS rut_paciente,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name2 AS nombres_paciente,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name AS apellido_paterno_paciente,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Name3 AS apellido_materno_paciente,
    CONVERT(varchar, RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Dob, 105) AS fecha_nacimiento_paciente,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->papmi_paper_dr->paper_ageyr AS edad_paciente_cirugia,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Nation_DR->CTNAT_Desc AS nacionalidad_paciente,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Sex_DR->CTSEX_Desc AS sexo_paciente,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_CityCode_DR->CTCIT_code AS codigo_comuna_paciente,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_CityCode_DR->CTCIT_Desc AS descripcion_comuna_paciente,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_Marital_DR->CTMAR_Desc AS estado_conyugal_paciente,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_IndigStat_DR->INDST_Desc AS pueblo_originario_paciente,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_AuxInsType_DR->AUXIT_InsType_DR->INST_Desc AS prevision,
    RBOP_PAADM_DR->PAADM_PAPMI_DR->PAPMI_PAPER_DR->PAPER_AuxInsType_DR->AUXIT_Desc AS plan_prevision,
     RBOP_DaySurgery AS ambulatoria,
    CASE 
        WHEN RB_OperatingRoom.RBOP_BookingType = 'EL' THEN 'cirugia_electiva'
        WHEN RB_OperatingRoom.RBOP_BookingType = 'EM' THEN 'cirugia_urgencia'
        WHEN RB_OperatingRoom.RBOP_BookingType = 'EL' THEN 'cirugia_electiva_no_programada'
        ELSE NULL 
    END AS tipo_agendamiento_cirugia,
    CONVERT(varchar, RBOP_DateOper, 105) AS fecha_agendamiento,
    CONVERT(varchar, RBOP_TimeOper, 108) AS hora_agendamiento,
    RBOP_EstimatedTime AS tiempo_programado_minutos,
    CASE 
        WHEN RB_OperatingRoom.RBOP_Status = 'B' THEN 'agendado'
        WHEN RB_OperatingRoom.RBOP_Status = 'X' THEN 'suspendido'
        WHEN RB_OperatingRoom.RBOP_Status = 'CL' THEN 'cerrado'
        WHEN RB_OperatingRoom.RBOP_Status = 'C' THEN 'confirmado'
        WHEN RB_OperatingRoom.RBOP_Status = 'P' THEN 'postergado'
        WHEN RB_OperatingRoom.RBOP_Status = 'D' THEN 'realizado'
        WHEN RB_OperatingRoom.RBOP_Status = 'R' THEN 'solicitado'
        WHEN RB_OperatingRoom.RBOP_Status = 'A' THEN 'recepcionado'
        WHEN RB_OperatingRoom.RBOP_Status = 'N' THEN 'no_listo'
        WHEN RB_OperatingRoom.RBOP_Status = 'SK' THEN 'enviado_por_reconocido'
        WHEN RB_OperatingRoom.RBOP_Status = 'SF' THEN 'enviado_por'
        ELSE 'otro'
    END AS estado_pabellon,
    RBOP_PreopTestDone AS paciente_condicional,
    RBOP_YesNo3 AS es_ges_s_n,
    CASE 
        WHEN RBOP_Appoint_DR IS NULL THEN 'N' 
        ELSE 'S' 
    END AS pabellon_programado,
    RBOP_PreopDiagn_DR->MRCID_DESC AS diagnostico_prequirurgico_lista_espera,
    q30 AS profilaxis_antibiotica_tcelcpi_s_n,
    ANAOP_Par_Ref->ANA_no AS numero_protocolo_anestesico,
    CASE 
        WHEN ANAOP_Par_Ref->ANA_Status = 'D' THEN 'realizado'
        WHEN ANAOP_Par_Ref->ANA_Status = 'S' THEN 'suspendido'
        WHEN ANAOP_Par_Ref->ANA_Status = 'A' THEN 'cancelado'
    END AS estado_protanest,
     ANAOP_No AS nro_protoper,
    CASE 
        WHEN ANAOP_Status = 'D' THEN 'realizado'
        WHEN ANAOP_Status = 'S' THEN 'suspendido'
        WHEN ANAOP_Status = 'A' THEN 'cancelado'
    END AS estado_protocolo_operatorio,
    ANAOP_CancelReason AS motivo_suspension_protocolo_operatorio,
    RBOP_ReasonSuspend_DR->SUSP_Desc as "motivo_suspencion",
    CASE 
        WHEN ANAOP_OperType = 'M' THEN 'cirugia_menor'
        WHEN ANAOP_OperType = 'S' THEN 'cirugia_mayor'
        ELSE ANAOP_OperType 
    END AS categoria_cirugia,
    ANAOP_Type_DR->OPER_code AS codigo_cirugia_principal,
    ANAOP_Type_DR->OPER_Desc AS descripcion_cirugia_principal,
    ANAOP_Depar_Oper_DR->CTLOC_Desc AS descripcion_equipo_quirurgico,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_TheatreInDate, 105) AS fecha_ingreso_quirofano_anestesico,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_TheatreInTime, 108) AS hora_ingreso_quirofano_anestesico,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_TheatreOutDate, 105) AS fecha_egreso_quirofano_anestesico,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_TheatreOutTime, 108) AS hora_egreso_quirofano_anestesico,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_CustomDate1, 105) AS fecha_inicio_cirugia_anestesico,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_CustomTime1, 108) AS hora_inicio_cirugia_anestesico,
    CONVERT(varchar, ANAOP_OpStartDate, 105) AS fecha_inicio_cirugia_operatorio,
    CONVERT(varchar, ANAOP_OpStartTime, 108) AS hora_inicio_cirugia_operatorio,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_CustomDate2, 105) AS fecha_termino_cirugia_anestesico,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_CustomTime2, 108) AS hora_termino_cirugia_anestesico,
    CONVERT(varchar, ANAOP_OpEndDate, 105) AS fecha_termino_cirugia_operatorio,
    CONVERT(varchar, ANAOP_OpEndTime, 108) AS hora_termino_cirugia_operatorio,
        CONVERT(varchar, ANAOP_Par_Ref->ANA_Date, 105) AS fecha_inicio_anestesia,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_AnaStartTime, 108) AS hora_inicio_anestesia,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_FinishDate, 105) AS fecha_termino_anestesia,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_AnaFinishTime, 108) AS hora_termino_anestesia,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_TheatreOutDate, 105) AS fecha_salida_quirofano,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_TheatreOutTime, 108) AS hora_salida_quirofano,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_PACU_ReadyLeaveDate, 105) AS fecha_traslado_sala_egreso,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_PACU_ReadyLeaveTime, 108) AS hora_traslado_sala_egreso,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_AreaOutDate, 105) AS fecha_egreso_area_quirurgica,
    CONVERT(varchar, ANAOP_Par_Ref->ANA_AreaOutTime, 108) AS hora_egreso_area_quirurgica,
    ANAOP_No AS numero_cirugia,
    ANAOP_BodySite_DR->BODS_Desc AS sitio_operacion_principal,
    OR_An_Oper_SecondaryProc->SECPR_BodySite_DR->BODS_Desc AS descripcion_sitio_operacion_secundaria,
    OR_An_Oper_SecondaryProc->SECPR_Operation_DR->OPER_Desc AS descripcion_cirugia_secundaria,
    ANAOP_Surgeon_DR->CTPCP_Code AS rut_cirujano_principal,
    ANAOP_Surgeon_DR->CTPCP_Desc AS cirujano_principal,
    ANAOP_SecondSurgeon_DR->CTPCP_Code AS rut_cirujano_2,
    ANAOP_SecondSurgeon_DR->CTPCP_Desc AS cirujano_2,
        (
        SELECT OPAS_CareProv_DR->CTPCP_Code
        FROM OR_An_Oper_Additional_Staff
        WHERE OPAS_ParRef = OR_Anaest_Operation.%id 
          AND OPAS_OperatingStaffRole_DR->OPSTFRL_Desc = 'Tercer Cirujano'
    ) AS rut_cirujano_3,
    (
        SELECT OPAS_CareProv_DR->CTPCP_Desc
        FROM OR_An_Oper_Additional_Staff
        WHERE OPAS_ParRef = OR_Anaest_Operation.%id 
          AND OPAS_OperatingStaffRole_DR->OPSTFRL_Desc = 'Tercer Cirujano'
    ) AS cirujano_3,
    (
        SELECT OPAS_CareProv_DR->CTPCP_Code
        FROM OR_An_Oper_Additional_Staff
        WHERE OPAS_ParRef = OR_Anaest_Operation.%id 
          AND OPAS_OperatingStaffRole_DR->OPSTFRL_Desc = 'Cuarto Cirujano'
    ) AS rut_cirujano_4,
    (
        SELECT OPAS_CareProv_DR->CTPCP_Desc
        FROM OR_An_Oper_Additional_Staff
        WHERE OPAS_ParRef = OR_Anaest_Operation.%id 
          AND OPAS_OperatingStaffRole_DR->OPSTFRL_Desc = 'Cuarto Cirujano'
    ) AS cirujano_4,
    (
        SELECT OPAS_CareProv_DR->CTPCP_Code
        FROM OR_An_Oper_Additional_Staff
        WHERE OPAS_ParRef = OR_Anaest_Operation.%id 
          AND OPAS_OperatingStaffRole_DR->OPSTFRL_Desc = 'Quinto Cirujano'
    ) AS rut_cirujano_5,
    (
        SELECT OPAS_CareProv_DR->CTPCP_Desc
        FROM OR_An_Oper_Additional_Staff
        WHERE OPAS_ParRef = OR_Anaest_Operation.%id 
          AND OPAS_OperatingStaffRole_DR->OPSTFRL_Desc = 'Quinto Cirujano'
    ) AS cirujano_5,
        ANAOP_ItemsCountedBy_DR->CTPCP_Code AS rut_arsenalera,
    ANAOP_ItemsCountedBy_DR->CTPCP_Desc AS arsenalera,
    ANAOP_ItemsReCountedBy_DR->CTPCP_Code AS rut_pabellonera,
    ANAOP_ItemsReCountedBy_DR->CTPCP_Desc AS pabellonera,
    ANAOP_Par_Ref->ANA_Anaesthetist_DR->CTPCP_Code AS rut_anestesista,
    ANAOP_Par_Ref->ANA_Anaesthetist_DR->CTPCP_Desc AS anestesista,
    ANAOP_Par_Ref->ANA_ASA_DR->ORASA_Desc AS riesgo_anestesico,
    ANAOP_Par_Ref->ANA_Method->ANMET_Desc AS tipo_anestesia,
    ANAOP_Par_Ref->ANA_BypassRec AS bypass_recuperacion_s_n,
    list(
        OR_AnaestAdditionalStaff->ANAAS_CareProv_DR->CTPCP_Code || ' ' || 
        OR_AnaestAdditionalStaff->ANAAS_OperatingStaffRole_DR->OPSTFRL_Desc
    ) AS codigo_staff_anestesistas
FROM
    RB_OperatingRoom
LEFT JOIN OR_Anaesthesia 
    ON RB_OperatingRoom.RBOP_PAADM_DR = OR_Anaesthesia.ANA_PAADM_ParRef
LEFT JOIN OR_Anaest_Operation 
    ON OR_Anaesthesia.ANA_RowId = OR_Anaest_Operation.ANAOP_Par_Ref
JOIN PA_Adm 
    ON RB_OperatingRoom.RBOP_PAADM_DR = PA_ADM.PAADM_RowID
LEFT JOIN questionnaire.QTCELCPI 
    ON PA_Adm.PAADM_RowID = questionnaire.QTCELCPI.QUESPAAdmDR
LEFT JOIN OR_An_Oper_SecondaryProc 
    ON OR_Anaest_Operation.ANAOP_RowId = OR_An_Oper_SecondaryProc.SECPR_ParRef
WHERE 
    PAADM_AdmDate >= DATEADD(MONTH, -12, CURRENT_DATE)
    AND RBOP_TimeOper > 0
    AND RBOP_RowId > 0
    AND RBOP_PAADM_DR->PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448 
    """
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    formatted_rows = []
    for r in rows:
        formatted_rows.append(tuple(str(v) if v is not None else '' for v in r) +
                              (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))

    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    crear_tabla(cursor_mysql)
    conn_mysql.commit()

    cursor_mysql.execute("TRUNCATE TABLE z_pabellon_prueba_concepto")
    conn_mysql.commit()

    placeholders = ",".join(["%s"] * len(formatted_rows[0]))
    insert_sql = f"INSERT INTO z_pabellon_prueba_concepto VALUES ({placeholders})"

    cursor_mysql.executemany(insert_sql, formatted_rows)
    conn_mysql.commit()

    logging.info("ETL z_pabellon_prueba_concepto finalizado correctamente.")

except Exception as e:
    logging.error(f"Error: {e}")

finally:
    for obj in [cursor_iris, conn_iris, cursor_mysql, conn_mysql]:
        try:
            if obj:
                obj.close()
        except Exception:
            pass

    if jpype.isJVMStarted():
        jpype.shutdownJVM()
