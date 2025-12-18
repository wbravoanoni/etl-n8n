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

# Conectar a MySQL
try:
    conn = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = conn.cursor()
    logging.info(" Conexión a MySQL exitosa")
except mysql.connector.Error as err:
    logging.error(f" Error de conexión a MySQL: {err}")
    exit(1)

# Consulta de origen
query = """
    SELECT * FROM z_pabellon_uso_gestion_pabellones_estado_agendamiento
"""

df = pd.read_sql(query, conn)

# Columnas objetivo incluyendo las nuevas
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

# Subset del DataFrame
df = df[columnas_objetivo]

# Convertir fecha_cirugia a YYYY-MM-DD
df['fecha_cirugia'] = pd.to_datetime(df['fecha_cirugia'], dayfirst=True, errors='coerce').dt.date

# Convertir fechas que pueden venir vacías
for col in ['fecha_ingreso_quirofano', 'fecha_egreso_quirofano']:
    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

# Convertir hora_ingreso_quirofano para cálculo posterior
df['hora_ingreso_quirofano_dt'] = pd.to_datetime(df['hora_ingreso_quirofano'], format='%H:%M:%S', errors='coerce')

# Calcular hora_ingreso_siguiente
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

        # Validar que la hora siguiente sea posterior a la hora de egreso (si ambas existen)
        if pd.notnull(hora_egreso) and pd.notnull(hora_siguiente):
            try:
                egreso_dt = datetime.strptime(hora_egreso, "%H:%M:%S")
                ingreso_sig_dt = datetime.strptime(hora_siguiente, "%H:%M:%S")

                if ingreso_sig_dt > egreso_dt:
                    df.at[i, 'hora_ingreso_siguiente'] = hora_siguiente
            except Exception as e:
                logging.warning(f" Error al validar hora siguiente en episodio {row['episodio']}: {e}")
        else:
            # Si no hay hora de egreso, se permite igual
            df.at[i, 'hora_ingreso_siguiente'] = hora_siguiente

# Eliminar columna auxiliar
df.drop(columns=['hora_ingreso_quirofano_dt'], inplace=True)

# Agregar la nueva columna a las columnas finales
columnas_finales = columnas_objetivo + ['hora_ingreso_siguiente']
df = df[columnas_finales]

# Reemplazar NaN o NaT por None
df = df.where(pd.notnull(df), None)

# Truncar tabla destino
try:
    cursor.execute("TRUNCATE TABLE z_pabellon_uso_gestion_tiempo_transcurrido")
    conn.commit()
    logging.info(" Tabla truncada: z_pabellon_uso_gestion_tiempo_transcurrido")
except Exception as e:
    logging.error(f" Error al truncar la tabla: {e}")
    conn.close()
    exit(1)

# Insertar en tabla destino
insert_query = """
    INSERT INTO z_pabellon_uso_gestion_tiempo_transcurrido 
    (episodio, fecha_cirugia, fecha_ingreso_quirofano, hora_ingreso_quirofano,
     fecha_egreso_quirofano, hora_egreso_quirofano, pabellon,
     tipo_cirugia, estado_cirugia, hora_ingreso_siguiente)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

valores = df.values.tolist()

try:
    cursor.executemany(insert_query, valores)
    conn.commit()
    logging.info(f" {cursor.rowcount} registros insertados correctamente.")
except mysql.connector.Error as err:
    logging.error(f" Error al insertar datos: {err}")
finally:
    cursor.close()
    conn.close()
