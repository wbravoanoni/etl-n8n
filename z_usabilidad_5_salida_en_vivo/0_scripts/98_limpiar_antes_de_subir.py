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
# FUNCIONES DE NORMALIZACIÓN
# ============================
def normalizar_tipo(df, columna_tipo):
    """
    Normaliza 'Médico Cirujano' -> 'Médico'
    SIN filtrar otros estamentos
    """
    df[columna_tipo] = (
        df[columna_tipo]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    df.loc[df[columna_tipo] == 'médico cirujano', columna_tipo] = 'médico'

    df[columna_tipo] = df[columna_tipo].str.capitalize()
    return df


def normalizar_y_filtrar_medicos(df, columna_tipo):
    """
    Normaliza y deja SOLO Médicos
    """
    df = normalizar_tipo(df, columna_tipo)
    df = df[df[columna_tipo] == 'Médico'].copy()
    return df

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
    # 2. INGRESO MÉDICO (NORMALIZA + FILTRA)
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
        ],
        'accion_tipo': 'filtrar_medicos'
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
    # 4. ALTAS MÉDICAS (SOLO NORMALIZA)
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
        ],
        'accion_tipo': 'normalizar'
    },

    # --------------------------------------------------
    # 6. EVOLUCIONES (SOLO NORMALIZA)
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
        ],
        'accion_tipo': 'normalizar'
    },

    # --------------------------------------------------
    # 9. DATASET CLÍNICO FILTRADO (NORMALIZA + FILTRA)
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
        'drop': ['SERVICIO'],
        'accion_tipo': 'filtrar_medicos'
    }
}

# ============================
# PROCESAMIENTO GENERAL
# ============================
for archivo, reglas in CONFIG.items():

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

    # Renombrar
    if reglas.get('rename'):
        df = df.rename(columns=reglas['rename'])

    # Drop columnas
    cols_drop = [c for c in reglas.get('drop', []) if c in df.columns]
    if cols_drop:
        df = df.drop(columns=cols_drop)

    # Normalización / Filtro por tipo
    accion = reglas.get('accion_tipo')
    if accion:
        col_tipo = 'tipo_profesional' if 'tipo_profesional' in df.columns else 'tipo'
        if col_tipo in df.columns:
            if accion == 'normalizar':
                df = normalizar_tipo(df, col_tipo)
            elif accion == 'filtrar_medicos':
                df = normalizar_y_filtrar_medicos(df, col_tipo)

            logging.info(f"Aplicada acción '{accion}' | Filas: {len(df)}")

    df.to_excel(ruta_salida, index=False)
    logging.info(f"Archivo generado: {ruta_salida}")

logging.info("Preprocesamiento finalizado correctamente")
