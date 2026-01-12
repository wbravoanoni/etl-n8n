import pandas as pd
from datetime import datetime

# ======================================================
# 1. Cargar PROFESIONALES (Codigo, Nombre, Tipo)
# ======================================================
df_prof = pd.read_excel(
    'z_usabilidad_5_salida_en_vivo/1_entrada/1_profesionales.xlsx'
)

df_prof = df_prof.iloc[:, 0:3].copy()
df_prof.columns = ['Codigo', 'NOMBRE', 'Tipo']
df_prof['Codigo'] = df_prof['Codigo'].astype(str).str.strip()
df_prof = df_prof.drop_duplicates(subset=['Codigo']).reset_index(drop=True)

# ======================================================
# 2. Parámetros de FECHA BASE
# ======================================================
fecha_inicio = pd.to_datetime('2026-01-07')
fecha_hoy = pd.to_datetime(datetime.now().date())

# ======================================================
# 3. Generar FECHAS POR CADA CODIGO
# ======================================================
bloques = []

for _, row in df_prof.iterrows():
    df_tmp = pd.DataFrame({
        'Codigo': row['Codigo'],
        'NOMBRE': row['NOMBRE'],
        'Tipo': row['Tipo'],
        'FECHA': pd.date_range(start=fecha_inicio, end=fecha_hoy, freq='D')
    })
    bloques.append(df_tmp)

df_base = pd.concat(bloques, ignore_index=True)

# ======================================================
# 4. Replicar por SERVICIO
# ======================================================
servicios = [416, 402, 417, 509, 428, 422, 415]
df_servicios = pd.DataFrame({'SERVICIO': servicios})

df_final = df_base.merge(df_servicios, how='cross')

# ======================================================
# 5. Descripción del SERVICIO
# ======================================================
mapa_servicios = {
    416: 'Sala UPC Borquez Silva HDS',
    402: 'Sala U.C.I HDS',
    417: 'Sala UTIQ HDS',
    509: 'Sala UPC U.T.I.M HDS',
    428: 'Sala UPC Hector Ducci HDS',
    422: 'Sala J. Luco HDS',
    415: 'Sala UPC UHI HDS'
}

df_final['SERVICIO_DESC'] = df_final['SERVICIO'].map(mapa_servicios)

# Normalización base
df_final['FECHA'] = pd.to_datetime(df_final['FECHA']).dt.normalize()
df_final['SERVICIO'] = df_final['SERVICIO'].astype(int)

# ======================================================
# 6. FUNCIÓN GENÉRICA PARA FLAGS CLÍNICOS
# ======================================================
def aplicar_flag(df_base, df_evento, columnas, nombre_flag, dayfirst=False):
    df = df_evento[columnas].copy()
    df.columns = ['Codigo', 'FECHA', 'SERVICIO']
    df['Codigo'] = df['Codigo'].astype(str).str.strip()
    df['FECHA'] = pd.to_datetime(df['FECHA'], dayfirst=dayfirst).dt.normalize()
    df['SERVICIO'] = df['SERVICIO'].astype(int)
    df[nombre_flag] = 'SI'

    df = df[['Codigo', 'FECHA', 'SERVICIO', nombre_flag]].drop_duplicates()

    df_base = df_base.merge(
        df,
        on=['Codigo', 'FECHA', 'SERVICIO'],
        how='left'
    )

    df_base[nombre_flag] = df_base[nombre_flag].fillna('NO')
    return df_base

# ======================================================
# 7. INGRESO MÉDICO
# ======================================================
df_final = aplicar_flag(
    df_final,
    pd.read_excel('z_usabilidad_5_salida_en_vivo/1_entrada/2_ingreso_medico.xlsx'),
    ['SSUSR_Initials', 'QUESDate', 'PAADM_CurrentWard_DR'],
    'INGRESO MÉDICO'
)

# ======================================================
# 8. DIAGNÓSTICO
# ======================================================
df_final = aplicar_flag(
    df_final,
    pd.read_excel('z_usabilidad_5_salida_en_vivo/1_entrada/3_diagnosticos.xlsx'),
    ['run_medico_registra_diagnostico', 'fecha_creacion', 'PAADM_CurrentWard_DR'],
    'DIAGNÓSTICO'
)

# ======================================================
# 9. ALTA MÉDICA
# ======================================================
df_final = aplicar_flag(
    df_final,
    pd.read_excel('z_usabilidad_5_salida_en_vivo/1_entrada/4_altas_medicas.xlsx'),
    ['rut Usuario Registro', 'Fecha Alta', 'PAADM_DepCode_DR'],
    'ALTA MÉDICA'
)

# ======================================================
# 10. EPICRISIS
# ======================================================
df_final = aplicar_flag(
    df_final,
    pd.read_excel('z_usabilidad_5_salida_en_vivo/1_entrada/5_epicrisis.xlsx'),
    ['rutMedicoContacto', 'DIS_Date', 'PAADM_CurrentWard_DR'],
    'EPICRISIS'
)

# ======================================================
# 11. EVOLUCIONES (dayfirst OBLIGATORIO)
# ======================================================
df_final = aplicar_flag(
    df_final,
    pd.read_excel('z_usabilidad_5_salida_en_vivo/1_entrada/6_evoluciones.xlsx'),
    ['CodeProfesionalEvolucion', 'FechaEvolucion', 'WARD_RowID'],
    'EVOLUCIÓN',
    dayfirst=True
)

# ======================================================
# 12. FILTRO FINAL (ESTO ES LO QUE OPTIMIZA)
# ======================================================
columnas_eventos = [
    'INGRESO MÉDICO',
    'DIAGNÓSTICO',
    'ALTA MÉDICA',
    'EPICRISIS',
    'EVOLUCIÓN'
]

df_export = df_final[
    df_final[columnas_eventos].eq('SI').any(axis=1)
].copy()

# ======================================================
# 13. EXPORTAR SOLO LO RELEVANTE
# ======================================================
output_path = (
    'z_usabilidad_5_salida_en_vivo/2_proceso/'
    'proceso.xlsx'
)

df_export.to_excel(output_path, index=False)

print("Archivo generado correctamente")
print("Ruta:", output_path)
print("Filas exportadas:", len(df_export))
