import os
import jaydebeapi
import jpype
import jpype.imports
from dotenv import load_dotenv

load_dotenv()
CONEXION_STRING = os.getenv("CONEXION_STRING")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
JDBC_DRIVER_NAME = os.getenv("JDBC_DRIVER_NAME")
JDBC_DRIVER_PATH = os.getenv("JDBC_DRIVER_PATH")

print("==== Probando conexión IRIS ====")
print("URL:", CONEXION_STRING)
print("Driver:", JDBC_DRIVER_NAME)
print("JAR:", JDBC_DRIVER_PATH)

# ========= INICIAR JVM ==========
if not jpype.isJVMStarted():
    jpype.startJVM(classpath=[JDBC_DRIVER_PATH])

# ========= CONECTAR ===========
try:
    conn = jaydebeapi.connect(
        JDBC_DRIVER_NAME,
        CONEXION_STRING,
        [DB_USER, DB_PASSWORD],
    )
    print("Conectado correctamente a IRIS!")

    conn.close()

except Exception as e:
    print("ERROR al conectar:")
    print(str(e))
finally:
    if jpype.isJVMStarted():
        jpype.shutdownJVM()
