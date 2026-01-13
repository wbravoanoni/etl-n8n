import pandas as pd
import logging
import os

# ============================
# LOGGING
# ============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ============================
# RUTAS BASE
# ============================
BASE_ENTRADA    = 'z_usabilidad_5_salida_en_vivo/1_entrada'
BASE_PROCESO    = 'z_usabilidad_5_salida_en_vivo/2_proceso'
BASE_RESULTADOS = 'z_usabilidad_5_salida_en_vivo/3_resultados'

os.makedirs(BASE_RESULTADOS, exist_ok=True)

# ============================
# CONFIGURACIÓN POR ARCHIVO
# ============================
CONFIG = {

    # --------------------------------------------------
    # 1. PROFESIONALES
    # --------------------------------------------------
    '1_profesionales.xlsx': {
        'salida': '1_profesionales_pro.xlsx',
        'rename': {
            'Codigo': 'rut_profesional',
            'Descripción': 'nombre_profesional',
            'Tipo': 'tipo_profesional',
            'Fecha desde': 'fecha_desde',
            'Fecha Hasta': 'fecha_hasta',
            'Estado': 'estado',
            'Nombres': 'nombres',
            'Apellido': 'apellido',
            'Especialidad': 'especialidad'
        },
        'drop': ['Local']
    },

    # --------------------------------------------------
    # 2. INGRESO MÉDICO
    # --------------------------------------------------
    '2_ingreso_medico.xlsx': {
        'salida': '2_ingreso_medico_pro.xlsx',
        'rename': {
            'Episodio': 'episodio',
            'QUESDate': 'fecha_creacion',
            'QUESTime': 'hora_creacion',
            'SSUSR_Initials': 'rut_profesional',
            'SSUSR_Name': 'nombre_profesional',
            'WARD_Desc': 'local_registro',
            'CTCPT_Desc': 'tipo_profesional'
        },
        'drop': [
            'SSUSR_RowId',
            'CTLOC_RowID',
            'CTLOC_Code',
            'CTLOC_Desc',
            'PAADM_CurrentWard_DR'
        ]
    },

    # --------------------------------------------------
    # 3. DIAGNÓSTICOS
    # --------------------------------------------------
    '3_diagnosticos.xlsx': {
        'salida': '3_diagnosticos_pro.xlsx',
        'rename': {
            'nro_episodio': 'episodio',
            'Hora_actualizacion': 'hora_actualizacion',
            'run_medico_registra_diagnostico': 'rut_medico',
            'descripcon_diagnostico': 'descripcion_diagnostico',
            'nombre_medico_registra_diagnostico': 'nombre_medico',
            'WARD_Desc': 'local_registro'
        },
        'drop': [
            'nro_de_registro',
            'PAADM_CurrentWard_DR'
        ]
    },

    # --------------------------------------------------
    # 4. ALTAS MÉDICAS
    # --------------------------------------------------
    '4_altas_medicas.xlsx': {
        'salida': '4_altas_medicas_pro.xlsx',
        'rename': {
            'Nro Episodio': 'episodio',
            'Fecha episodio': 'fecha_admision',
            'Hora episodio': 'hora_admision',
            'Nombres': 'nombre_paciente',
            'Apellido Paterno': 'apellido_paterno_paciente',
            'Apellido Materno': 'apellido_materno_paciente',
            'RUN': 'rut',
            'Tipo de Alta': 'tipo_alta',
            'Fecha Alta': 'fecha_alta',
            'Profesional Alta': 'profesional_alta',
            'rut Usuario Registro': 'rut_profesional_registra',
            'Usuario Registro': 'nombre_profesional',
            'CTCPT_Desc': 'tipo_profesional'
        },
        'drop': [
            'Local de atención',
            'PAADM_DepCode_DR',
            'Nro Registro',
            'Edad',
            'Sexo',
            'Diagnóstico Principal',
            'Indicaciones al Alta',
            'DIS_DischargeSummaryType_DR'
        ]
    },

    # --------------------------------------------------
    # 5. EPICRISIS
    # --------------------------------------------------
    '5_epicrisis.xlsx': {
        'salida': '5_epicrisis_pro.xlsx',
        'rename': {
            'NombrePaciente': 'nombre_paciente',
            'RUNPaciente': 'rut_paciente',
            'NumeroEpisodio': 'episodio',
            'Local Actual': 'servicio',
            'DIS_Date': 'fecha_creacion'
        },
        'drop': [
            'HOSP_Code',
            'SexoCodigo',
            'Sexo',
            'Comuna',
            'EstablecimientoInscripción',
            'ServicioClinicoCodigo',
            'ServicioClinico',
            'FechaAtencion',
            'FechaEgreso',
            'FechaAlta',
            'DestinoEgreso',
            'code',
            'MedicoContacto',
            'Hosp',
            'subtipoepi',
            'TratamientoRecibido',
            'ProximoControl',
            'IndicacionesAlAlta',
            'DiagnosticoQueMotivoIngreso',
            'rutMedicoContacto',
            'MedicoContacto.1',
            'PAADM_CurrentWard_DR'
        ]
    },

    # --------------------------------------------------
    # 6. EVOLUCIONES
    # --------------------------------------------------
    '6_evoluciones.xlsx': {
        'salida': '6_evoluciones_pro.xlsx',
        'rename': {
            'NumeroEpisodio': 'episodio',
            'Estado_Evolucion': 'estado_evolucion',
            'FechaEvolucion': 'fecha_creacion',
            'HoraEvolucion': 'hora_creacion',
            'CodeProfesionalEvolucion': 'rut_profesional',
            'ProfesionalEvolucion': 'nombre_profesional',
            'EstamentoProfesional': 'tipo_profesional',
            'RUNPaciente': 'rut_paciente',
            'NombresPaciente': 'nombre_paciente',
            'AppPaternoPaciente': 'apellido_paterno_paciente',
            'AppMaternoPaciente': 'apellido_materno_paciente',
            'local_actual': 'local_registro'
        },
        'drop': [
            'Grupo_Evolucion',
            'Tipo_Evolucion',
            'Usuario_Evolucion',
            'WARD_RowID'
        ]
    },

    # --------------------------------------------------
    # 7. PACIENTES HOSPITALIZADOS
    # --------------------------------------------------
    '7_pacientes_hospitalizados.xlsx': {
        'salida': '7_pacientes_hospitalizados_pro.xlsx',
        'rename': {
            'Episodio': 'episodio',
            'Fecha Admision': 'fecha_admision',
            'Hora Admision': 'hora_admision',
            'RUT Paciente': 'rut_paciente',
            'Nombre Paciente': 'nombre_paciente',
            'Apellido Paterno Paciente': 'apellido_paterno_paciente',
            'Apellido Materno Paciente': 'apellido_materno_paciente',
            'Rut Profesional crea': 'rut_profesional',
            'Nombre Profesional crea': 'nombre_profesional',
            'Unidad Servicio / Clínico': 'servicio'
        },
        'drop': ['Local']
    },

    # --------------------------------------------------
    # 8. CUESTIONARIO QT CERI RIESGO
    # --------------------------------------------------
    '8_cuestionario_QTCERIESGO.xlsx': {
        'salida': '8_cuestionario_QTCERIESGO_pro.xlsx',
        'rename': {},
        'drop': ['campo1']
    },

    # --------------------------------------------------
    # 9. DATASET CLÍNICO FILTRADO (origen 2_proceso)
    # --------------------------------------------------
    'df_clinico_FILTRADO_eventos.xlsx': {
        'origen': '2_proceso',
        'salida': '9_df_clinico_FILTRADO_eventos_pro.xlsx',
        'rename': {
            'Codigo': 'rut_profesional',
            'NOMBRE': 'nombre',
            'Tipo': 'tipo',
            'FECHA': 'fecha_creacion_item',
            'SERVICIO_DESC': 'servicio',
            'INGRESO MÉDICO': 'ingreso_medico',
            'DIAGNÓSTICO': 'diagnostico',
            'ALTA MÉDICA': 'alta_medica',
            'EPICRISIS': 'epicrisis',
            'EVOLUCIÓN': 'evolucion'
        },
        'drop': ['SERVICIO']
    }
}

# ============================
# PROCESAMIENTO GENERAL
# ============================
for archivo, reglas in CONFIG.items():

    # Origen correcto
    base_origen = BASE_ENTRADA
    if reglas.get('origen') == '2_proceso':
        base_origen = BASE_PROCESO

    ruta_entrada = os.path.join(base_origen, archivo)
    ruta_salida  = os.path.join(BASE_RESULTADOS, reglas['salida'])

    if not os.path.exists(ruta_entrada):
        logging.warning(f"Archivo no encontrado: {ruta_entrada}")
        continue

    logging.info(f"Procesando {archivo}")
    df = pd.read_excel(ruta_entrada)

    # Renombrar columnas
    if reglas.get('rename'):
        df = df.rename(columns=reglas['rename'])

    # Eliminar columnas
    cols_drop = [c for c in reglas.get('drop', []) if c in df.columns]
    if cols_drop:
        df = df.drop(columns=cols_drop)
        logging.info(f"Columnas eliminadas: {cols_drop}")

    # Guardar salida
    df.to_excel(ruta_salida, index=False)
    logging.info(f"Archivo generado: {ruta_salida}")

logging.info("Preprocesamiento finalizado correctamente")
