import pandas as pd
import logging
import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from datetime import datetime
from dotenv import load_dotenv


load_dotenv(override=True)

# ============================
# LOGGING
# ============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ============================
# RUTA BASE RESULTADOS
# ============================
BASE_RESULTADOS = 'z_usabilidad_5_salida_en_vivo/3_resultados'

# ============================
# FECHA DE EJECUCIÓN (GLOBAL)
# ============================
FECHA_EJECUCION = datetime.now()

# ============================
# VARIABLES DE ENTORNO MYSQL
# ============================
mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = int(os.getenv('DB_MYSQL_PORT', 3306))
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

# ============================
# CONEXIÓN SQLALCHEMY
# ============================
mysql_url = URL.create(
    drivername="mysql+mysqlconnector",
    username=mysql_user,
    password=mysql_password,
    host=mysql_host,
    port=mysql_port,
    database=mysql_database
)

engine = create_engine(mysql_url)

# ============================
# CONFIGURACIÓN DE TABLAS
# ============================
TABLAS = {

    'PROF': {
        'archivo': '1_profesionales_pro.xlsx',
        'tabla': 'hc_profesionales'
    },

    'ING_MED': {
        'archivo': '2_ingreso_medico_pro.xlsx',
        'tabla': 'hc_ingreso_medico'
    },

    'DIAG': {
        'archivo': '3_diagnosticos_pro.xlsx',
        'tabla': 'hc_diagnosticos'
    },

    'ALTA': {
        'archivo': '4_altas_medicas_pro.xlsx',
        'tabla': 'hc_altas_medicas'
    },

    'EPI': {
        'archivo': '5_epicrisis_pro.xlsx',
        'tabla': 'hc_epicrisis'
    },

    'EVOL': {
        'archivo': '6_evoluciones_pro.xlsx',
        'tabla': 'hc_evoluciones'
    },

    'HOSP': {
        'archivo': '7_pacientes_hospitalizados_pro.xlsx',
        'tabla': 'hc_pacientes_hospitalizados'
    },

    'QT_RIESGO': {
        'archivo': '8_cuestionario_QTCERIESGO_pro.xlsx',
        'tabla': 'hc_qt_ceri_riesgo'
    },

    'CLINICO': {
        'archivo': '9_df_clinico_FILTRADO_eventos_pro.xlsx',
        'tabla': 'hc_clinico_eventos'
    }
}

# ============================
# FUNCIÓN DE CARGA
# ============================
def cargar_excel_mysql(alias, archivo, tabla, modo='replace'):
    """
    modo:
        - 'replace'  -> borra y crea
        - 'append'   -> inserta
    """

    ruta = os.path.join(BASE_RESULTADOS, archivo)

    if not os.path.exists(ruta):
        logging.warning(f"[{alias}] Archivo no encontrado: {ruta}")
        return

    logging.info(f"[{alias}] Leyendo archivo {archivo}")
    df = pd.read_excel(ruta)

    if df.empty:
        logging.warning(f"[{alias}] DataFrame vacío, se omite carga")
        return

    # ============================
    # AGREGAR FECHA DE ACTUALIZACIÓN
    # ============================
    df['fecha_actualizacion'] = FECHA_EJECUCION

    logging.info(
        f"[{alias}] Insertando {len(df)} registros en {tabla} "
        f"(modo={modo}, fecha_actualizacion={FECHA_EJECUCION})"
    )

    df.to_sql(
        name=tabla,
        con=engine,
        if_exists=modo,
        index=False,
        chunksize=1000,
        method='multi'
    )

    logging.info(f"[{alias}] Carga finalizada correctamente")

# ============================
# EJECUCIÓN GENERAL
# ============================
if __name__ == "__main__":

    logging.info("=== INICIO CARGA MYSQL ===")
    logging.info(f"Fecha de ejecución: {FECHA_EJECUCION}")

    for alias, cfg in TABLAS.items():
        cargar_excel_mysql(
            alias=alias,
            archivo=cfg['archivo'],
            tabla=cfg['tabla'],
            modo='replace'   # cambia a 'append' si lo necesitas
        )

    logging.info("=== CARGA MYSQL FINALIZADA ===")
