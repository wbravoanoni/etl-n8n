import os
import pandas as pd
import mysql.connector
from datetime import datetime
import logging
from dotenv import load_dotenv

load_dotenv(override=True)

os.makedirs("logs", exist_ok=True)

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Leer variables de entorno
mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = int(os.getenv('DB_MYSQL_PORT', 3306))
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

# ============================================================
# FUNCIÓN: crear tabla si no existe
# ============================================================
def crear_tabla_z_pabellon_uso_gestion_tiempo_transcurrido(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS z_pabellon_uso_gestion_tiempo_transcurrido (
        episodio VARCHAR(11),
        fecha_cirugia VARCHAR(10),
        fecha_ingreso_quirofano VARCHAR(10),
        hora_ingreso_quirofano VARCHAR(8),
        fecha_egreso_quirofano VARCHAR(10),
        hora_egreso_quirofano VARCHAR(8),
        pabellon VARCHAR(15),
        hora_ingreso_siguiente VARCHAR(8),
        es_ultima_cirugia_del_dia VARCHAR(20),
        tipo_cirugia VARCHAR(30),
        estado_cirugia VARCHAR(12),
        fechaActualizacion VARCHAR(19),

        INDEX idx_episodio (episodio),
        INDEX idx_fecha_cirugia (fecha_cirugia),
        INDEX idx_pabellon (pabellon)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

# ============================================================
# Conectar a MySQL
# ============================================================
try:
    conn = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = conn.cursor()
    logging.info("Conexión a MySQL exitosa")
except mysql.connector.Error as err:
    logging.error(f"Error de conexión a MySQL: {err}")
    exit(1)

# ============================================================
# CREAR TABLA SI NO EXISTE
# ============================================================
crear_tabla_z_pabellon_uso_gestion_tiempo_transcurrido(cursor)
conn.commit()
logging.info("Tabla z_pabellon_uso_gestion_tiempo_transcurrido verificada/creada")

# ============================================================
# Consulta de origen
# ============================================================
query = """
    SELECT * FROM z_pabellon_uso_gestion_pabellones_estado_agendamiento
"""
df = pd.read_sql(query, conn)

# Columnas objetivo
columnas_objetivo = [
    'episodio',
    'fecha_cirugia',
    'fecha_ingreso_quirofano',
    'hora_ingreso_quirofano',
    'fecha_egreso_quirofano',
    'hora_egreso_quirofano',
    'pabellon',
    'tipo_cirugia',
    'estado_cirugia'
]

df = df[columnas_objetivo]

# ============================================================
# Transformaciones (SIN CAMBIOS)
# ============================================================
df['fecha_cirugia'] = pd.to_datetime(df['fecha_cirugia'], dayfirst=True, errors='coerce').dt.date

for col in ['fecha_ingreso_quirofano', 'fecha_egreso_quirofano']:
    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

df['hora_ingreso_quirofano_dt'] = pd.to_datetime(
    df['hora_ingreso_quirofano'], format='%H:%M:%S', errors='coerce'
)

df['hora_ingreso_siguiente'] = None

for i, row in df.iterrows():
    if pd.isna(row['hora_ingreso_quirofano_dt']) or pd.isna(row['fecha_cirugia']):
        continue

    posteriores = df[
        (df['fecha_cirugia'] == row['fecha_cirugia']) &
        (df['pabellon'] == row['pabellon']) &
        (df['hora_ingreso_quirofano_dt'] > row['hora_ingreso_quirofano_dt'])
    ]

    if not posteriores.empty:
        siguiente = posteriores.sort_values(by='hora_ingreso_quirofano_dt').iloc[0]
        hora_siguiente = siguiente['hora_ingreso_quirofano']
        hora_egreso = row['hora_egreso_quirofano']

        if pd.notnull(hora_egreso) and pd.notnull(hora_siguiente):
            try:
                egreso_dt = datetime.strptime(hora_egreso, "%H:%M:%S")
                ingreso_sig_dt = datetime.strptime(hora_siguiente, "%H:%M:%S")
                if ingreso_sig_dt > egreso_dt:
                    df.at[i, 'hora_ingreso_siguiente'] = hora_siguiente
            except Exception as e:
                logging.warning(f"Error al validar hora siguiente en episodio {row['episodio']}: {e}")
        else:
            df.at[i, 'hora_ingreso_siguiente'] = hora_siguiente

df.drop(columns=['hora_ingreso_quirofano_dt'], inplace=True)

columnas_finales = columnas_objetivo + ['hora_ingreso_siguiente']
df = df[columnas_finales]

df = df.where(pd.notnull(df), None)

# ============================================================
# TRUNCATE
# ============================================================
cursor.execute("TRUNCATE TABLE z_pabellon_uso_gestion_tiempo_transcurrido")
conn.commit()
logging.info("Tabla truncada: z_pabellon_uso_gestion_tiempo_transcurrido")

# ============================================================
# INSERT
# ============================================================
insert_query = """
    INSERT INTO z_pabellon_uso_gestion_tiempo_transcurrido
    (episodio, fecha_cirugia, fecha_ingreso_quirofano, hora_ingreso_quirofano,
     fecha_egreso_quirofano, hora_egreso_quirofano, pabellon,
     tipo_cirugia, estado_cirugia, hora_ingreso_siguiente)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

cursor.executemany(insert_query, df.values.tolist())
conn.commit()
logging.info(f"{cursor.rowcount} registros insertados correctamente.")

cursor.close()
conn.close()
