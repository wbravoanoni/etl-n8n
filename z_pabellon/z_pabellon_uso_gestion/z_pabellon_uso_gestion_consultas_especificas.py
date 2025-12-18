import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv(override=True)

# Leer variables de entorno
mysql_host = os.getenv("DB_MYSQL_HOST", "localhost")
mysql_port = int(os.getenv("DB_MYSQL_PORT", 3306))
mysql_user = os.getenv("DB_MYSQL_USER", "root")
mysql_password = os.getenv("DB_MYSQL_PASSWORD", "")
mysql_database = os.getenv('DB_MYSQL_DATABASE')

# Consulta SQL
query = """
SELECT * FROM z_pabellon_uso_gestion_pabellones_estado_agendamiento
WHERE fecha_cirugia = '01-12-2025' AND pabellon = 'Pabellón 02 HDS'
"""

try:
    # Establecer conexión
    connection = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )

    if connection.is_connected():
        print("✅ Conexión exitosa a MySQL")

        # Leer datos con pandas
        df = pd.read_sql(query, connection)

        # Mostrar resultados
        print("\n📋 Resultados de la consulta:")
        print(df)

except Error as e:
    print(f"❌ Error al conectar o ejecutar la consulta: {e}")

finally:
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("\n🔌 Conexión cerrada.")
