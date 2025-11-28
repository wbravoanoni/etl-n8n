import jaydebeapi
import jpype
import mysql.connector
from dotenv import load_dotenv
import os
import logging

from datetime import datetime

load_dotenv(override=True)

# Configurar logging para que también imprima en la consola
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(threadName)s - %(processName)s %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("logs/z_carga_consultas_urgencias.log"),
                        logging.StreamHandler()])

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Leer variables de entorno para InterSystems IRIS
jdbc_driver_name = os.getenv('JDBC_DRIVER_NAME')
jdbc_driver_loc = os.getenv('JDBC_DRIVER_PATH')
iris_connection_string = os.getenv('CONEXION_STRING')

iris_user = os.getenv('DB_USER')
iris_password = os.getenv('DB_PASSWORD')

# Leer variables de entorno para MySQL
mysql_host = os.getenv('DB_MYSQL_HOST')
mysql_port = os.getenv('DB_MYSQL_PORT')
mysql_user = os.getenv('DB_MYSQL_USER')
mysql_password = os.getenv('DB_MYSQL_PASSWORD')
mysql_database = os.getenv('DB_MYSQL_DATABASE')

conn_mysql = None
conn_iris = None
cursor_iris = None
cursor_mysql = None

try:
    # Validar variables de entorno
    if not jdbc_driver_name or not jdbc_driver_loc:
        logging.error("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
        raise ValueError("El nombre o la ruta del controlador JDBC no están configurados correctamente.")
    if not iris_connection_string or not iris_user or not iris_password:
        logging.error("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
        raise ValueError("Las variables de entorno de InterSystems IRIS no están configuradas correctamente.")
    if not mysql_host or not mysql_port or not mysql_user or not mysql_password or not mysql_database:
        logging.error("Las variables de entorno de MySQL no están configuradas correctamente.")
        raise ValueError("Las variables de entorno de MySQL no están configuradas correctamente.")
    
    # Iniciar JVM si no está ya iniciada
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jdbc_driver_loc)

    # Crear conexión con InterSystems IRIS
    conn_iris = jaydebeapi.connect(
        jdbc_driver_name,
        iris_connection_string,
        {'user': iris_user, 'password': iris_password},
        jdbc_driver_loc
    )

    # Consulta SQL para obtener datos
    query = ''' 
            select ID,
            QUESCreateDate as "fecha_creacion",
            QUESCreateTime as "hora_creacion",
            QUESScore as "puntaje",
            QUESCreateUserDR,
            b.SSUSR_Name as "usuario_creador",
            c.PAADM_ADMNo AS "episodio",
            c.PAADM_CurrentWard_DR->WARD_Desc AS "local_actual",
            a.Q01,a.Q02,a.Q03,a.Q04,a.Q05,a.Q06,a.Q08,a.Q09,a.Q10,
            a.Q11,a.Q12,a.Q13,a.Q14,a.Q15,a.Q16,a.Q17,a.Q20,a.Q21,a.Q22,
            a.Q30,a.Q31,a.Q32,a.Q33,a.Q35,
            a.Q40,a.Q41,a.Q42,a.Q43,a.Q44,a.Q45,a.Q46
            from questionnaire.QTCEBRADEN AS a
            INNER JOIN SS_User b on a.QUESCreateUserDR=b.SSUSR_RowId
            LEFT JOIN PA_Adm c on a.QUESPAAdmDR = c.PAADM_RowID
            WHERE QUESCreateDate >= DATEADD(MONTH, -6, GETDATE()) AND PAADM_DepCode_DR->CTLOC_Hospital_DR = 10448;
        '''
    #QUESCreateDate >'2025-05-13' 
    # Ejecutar consulta en InterSystems IRIS
    cursor_iris = conn_iris.cursor()
    cursor_iris.execute(query)
    rows = cursor_iris.fetchall()

    # Convertir filas a formato adecuado para MySQL
    formatted_rows = []
    for row in rows:
        iD= '' if row[0] is None else str(row[0])
        fecha_creacion= '' if row[1] is None else str(row[1])
        hora_creacion= '' if row[2] is None else str(row[2])
        puntaje= '' if row[3] is None else str(row[3])
        QUESCreateUserDR= '' if row[4] is None else str(row[4])
        usuario_creador= '' if row[5] is None else str(row[5])
        episodio= '' if row[6] is None else str(row[6])
        local_actual= '' if row[7] is None else str(row[7])
        Q01= '' if row[8] is None else str(row[8])
        Q02= '' if row[9] is None else str(row[9])
        Q03= '' if row[10] is None else str(row[10])
        Q04= '' if row[11] is None else str(row[11])
        Q05= '' if row[12] is None else str(row[12])
        Q06= '' if row[13] is None else str(row[13])
        Q08= '' if row[14] is None else str(row[14])
        Q09= '' if row[15] is None else str(row[15])
        Q10= '' if row[16] is None else str(row[16])
        Q11= '' if row[17] is None else str(row[17])
        Q12= '' if row[18] is None else str(row[18])
        Q13= '' if row[19] is None else str(row[19])
        Q14= '' if row[20] is None else str(row[20])
        Q15= '' if row[21] is None else str(row[21])
        Q16= '' if row[22] is None else str(row[22])
        Q17= '' if row[23] is None else str(row[23])
        Q20= '' if row[24] is None else str(row[24])
        Q21= '' if row[25] is None else str(row[25])
        Q22= '' if row[26] is None else str(row[26])
        Q30= '' if row[27] is None else str(row[27])
        Q31= '' if row[28] is None else str(row[28])
        Q32= '' if row[29] is None else str(row[29])
        Q33= '' if row[30] is None else str(row[30])
        Q35= '' if row[31] is None else str(row[31])
        Q40= '' if row[32] is None else str(row[32])
        Q41= '' if row[33] is None else str(row[33])
        Q42= '' if row[34] is None else str(row[34])
        Q43= '' if row[35] is None else str(row[35])
        Q44= '' if row[36] is None else str(row[36])
        Q45= '' if row[37] is None else str(row[37])
        Q46= '' if row[38] is None else str(row[38])
        fechaActualizacion = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        formatted_rows.append(( 
            iD,
            fecha_creacion,
            hora_creacion,
            puntaje,
            QUESCreateUserDR,
            usuario_creador,
            episodio,
            local_actual,
            Q01,
            Q02,
            Q03,
            Q04,
            Q05,
            Q06,
            Q08,
            Q09,
            Q10,
            Q11,
            Q12,
            Q13,
            Q14,
            Q15,
            Q16,
            Q17,
            Q20,
            Q21,
            Q22,
            Q30,
            Q31,
            Q32,
            Q33,
            Q35,
            Q40,
            Q41,
            Q42,
            Q43,
            Q44,
            Q45,
            Q46,
            fechaActualizacion
        ))

    # Conectar a MySQL
    conn_mysql = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor_mysql = conn_mysql.cursor()

    # Truncar la tabla en MySQL
    try:
        cursor_mysql.execute("TRUNCATE TABLE z_cuestionario_braden")
        conn_mysql.commit()
        logging.info("Tabla 'z_cuestionario_braden' truncada exitosamente.")
    except mysql.connector.Error as e:
        logging.error(f"Error al truncar la tabla: {e}")
        raise

    # Insertar datos en MySQL en chunks
    insert_query = """
        INSERT INTO z_cuestionario_braden (
            ID,
            fecha_creacion,
            hora_creacion,
            puntaje,
            QUESCreateUserDR,
            usuario_creador,
            episodio,
            local_actual,
            Q01,
            Q02,
            Q03,
            Q04,
            Q05,
            Q06,
            Q08,
            Q09,
            Q10,
            Q11,
            Q12,
            Q13,
            Q14,
            Q15,
            Q16,
            Q17,
            Q20,
            Q21,
            Q22,
            Q30,
            Q31,
            Q32,
            Q33,
            Q35,
            Q40,
            Q41,
            Q42,
            Q43,
            Q44,
            Q45,
            Q46,
            fechaActualizacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    chunk_size = 1000  # Ajusta este tamaño según sea necesario
    for i in range(0, len(formatted_rows), chunk_size):
        chunk = formatted_rows[i:i + chunk_size]
        cursor_mysql.executemany(insert_query, chunk)
        conn_mysql.commit()
    logging.info("Datos transferidos exitosamente.")
except jaydebeapi.DatabaseError as e:
    logging.error(f"Error en InterSystems IRIS: {e}")
except mysql.connector.Error as e:
    logging.error(f"Error en MySQL: {e}")
except ValueError as e:
    logging.error(f"Error en la configuración: {e}")
except Exception as e:
    logging.error(f"Error: {e}")
finally:
    # Cerrar cursores y conexiones
    if cursor_iris:
        cursor_iris.close()
    if conn_iris:
        conn_iris.close()
    if cursor_mysql:
        cursor_mysql.close()
    if conn_mysql:
        conn_mysql.close()
    # Detener la JVM si la iniciamos
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
