import os
import mysql.connector
from dotenv import load_dotenv
import logging

# ============================================================
# CARGAR VARIABLES DE ENTORNO (igual que tu script principal)
# ============================================================

load_dotenv(override=True)

mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = os.getenv('DB_MYSQL_PORT')
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():

    # ============================================================
    # VALIDACIONES
    # ============================================================
    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        logging.error("Faltan variables de entorno de MySQL")
        return

    logging.info("Conectando a MySQL...")

    try:
        conn = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
    except Exception as e:
        logging.error(f"Error conectando a MySQL: {e}")
        return

    cursor = conn.cursor()

    # ============================================================
    # OBTENER DATOS DE LA TABLA
    # ============================================================

    table_name = "z_usabilidad_hospitalizados_epicrisis"

    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
    except Exception as e:
        logging.error(f"Error ejecutando SELECT: {e}")
        conn.close()
        return

    logging.info(f"Se obtuvieron {len(rows)} filas para análisis.")

    # ============================================================
    # CALCULAR LARGO MÁXIMO POR COLUMNA
    # ============================================================

    max_lengths = {col: 0 for col in column_names}

    for row in rows:
        for col, value in zip(column_names, row):
            length = len(str(value)) if value is not None else 0
            if length > max_lengths[col]:
                max_lengths[col] = length

    # ============================================================
    # MOSTRAR RESULTADOS
    # ============================================================

    print("\n================ LARGO MÁXIMO POR COLUMNA ================")
    for col, length in max_lengths.items():
        print(f"{col:35} → {length}")
    print("===========================================================\n")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
