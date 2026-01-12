import pandas as pd
from datetime import datetime

# ================================
# 1. Cargar profesionales
# ================================
df_1_profesionales = pd.read_excel(
    'z_usabilidad_5_salida_en_vivo/1_entrada/1_profesionales.xlsx'
)

df_codigos = df_1_profesionales.iloc[:, 0:2].copy()
df_codigos.columns = ['Codigo', 'NOMBRE']
df_codigos = df_codigos.drop_duplicates().reset_index(drop=True)

# ================================
# 2. Parámetros de fechas
# ================================
fecha_inicio = pd.to_datetime('2026-01-07')
fecha_hoy = pd.to_datetime(datetime.now().date())

# ================================
# 3. Generar FECHAS POR CADA CODIGO
# ================================
bloques = []

for _, row in df_codigos.iterrows():
    df_tmp = pd.DataFrame({
        'Codigo': row['Codigo'],
        'NOMBRE': row['NOMBRE'],
        'FECHA': pd.date_range(
            start=fecha_inicio,
            end=fecha_hoy,
            freq='D'
        )
    })
    bloques.append(df_tmp)

df_base = pd.concat(bloques, ignore_index=True)

# ================================
# 4. Replicar por SERVICIO
# ================================
servicios = [416, 402, 417, 509, 428, 422, 415]
df_servicios = pd.DataFrame({'SERVICIO': servicios})

df_final = df_base.merge(df_servicios, how='cross')

# ================================
# 5. Descripción del servicio
# ================================
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

# ================================
# 6. Cargar INGRESO MÉDICO
# ================================
df_2_ingreso_medico = pd.read_excel(
    'z_usabilidad_5_salida_en_vivo/1_entrada/2_ingreso_medico.xlsx'
)

df_ingreso = df_2_ingreso_medico[
    ['SSUSR_Initials', 'QUESDate', 'PAADM_CurrentWard_DR']
].copy()

df_ingreso.columns = ['Codigo', 'FECHA', 'SERVICIO']

# Normalizar FECHA (eliminar hora)
df_ingreso['FECHA'] = pd.to_datetime(df_ingreso['FECHA']).dt.normalize()

# Asegurar tipo de SERVICIO
df_ingreso['SERVICIO'] = df_ingreso['SERVICIO'].astype(int)

# Crear flag
df_ingreso['INGRESO MÉDICO'] = 'SI'

df_ingreso_llave = (
    df_ingreso[['Codigo', 'FECHA', 'SERVICIO', 'INGRESO MÉDICO']]
    .drop_duplicates()
)

# ================================
# 7. Merge con base principal
# ================================
df_final = df_final.merge(
    df_ingreso_llave,
    on=['Codigo', 'FECHA', 'SERVICIO'],
    how='left'
)

df_final['INGRESO MÉDICO'] = df_final['INGRESO MÉDICO'].fillna('NO')

# ================================
# 8. Ordenar (recomendado)
# ================================
df_final = df_final.sort_values(
    by=['Codigo', 'FECHA', 'SERVICIO']
).reset_index(drop=True)


# ================================
# 10. Cargar DIAGNÓSTICOS
# ================================
df_3_diagnosticos = pd.read_excel(
    'z_usabilidad_5_salida_en_vivo/1_entrada/3_diagnosticos.xlsx'
)

df_diag = df_3_diagnosticos[
    ['run_medico_registra_diagnostico', 'fecha_creacion', 'PAADM_CurrentWard_DR']
].copy()

df_diag.columns = ['Codigo', 'FECHA', 'SERVICIO']

# Normalizar FECHA (eliminar hora)
df_diag['FECHA'] = pd.to_datetime(df_diag['FECHA']).dt.normalize()

# Asegurar tipo de SERVICIO
df_diag['SERVICIO'] = df_diag['SERVICIO'].astype(int)

# Crear flag
df_diag['DIAGNÓSTICO'] = 'SI'

df_diag_llave = (
    df_diag[['Codigo', 'FECHA', 'SERVICIO', 'DIAGNÓSTICO']]
    .drop_duplicates()
)

# ================================
# 11. Merge con base principal
# ================================
df_final = df_final.merge(
    df_diag_llave,
    on=['Codigo', 'FECHA', 'SERVICIO'],
    how='left'
)

df_final['DIAGNÓSTICO'] = df_final['DIAGNÓSTICO'].fillna('NO')

# ================================
# 12. Exportar
# ================================
output_path = (
    'z_usabilidad_5_salida_en_vivo/2_proceso/'
    'df_base_fechas_por_codigo_con_ingreso_medico.xlsx'
)

df_final.to_excel(output_path, index=False)

print("Archivo generado correctamente:", output_path)
