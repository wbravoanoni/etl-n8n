import pandas as pd
import os

import logging

# Configuración de logs
logging.basicConfig(level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/z_teamcoder_descargaReporte.log"),
        logging.StreamHandler()
    ])


# ========================
# CONFIGURACIÓN INICIAL
# ========================
os.makedirs("z_teamcoder/resultados", exist_ok=True)

archivo_original = "z_teamcoder/entrada/z_teamcoder_descargaReporte_original.xlsx"
archivo_ocupacion = "z_teamcoder/reglas_homologacion/OCUPACION.xlsx"
archivo_programa = "z_teamcoder/reglas_homologacion/REGCON_01.xlsx"
archivo_especialidad = "z_teamcoder/reglas_homologacion/ESPECIALIDADES.xlsx"
archivo_servalt = "z_teamcoder/reglas_homologacion/SERVALT.xlsx"
archivo_serving = "z_teamcoder/reglas_homologacion/SERVING.xlsx"
archivo_etnia = "z_teamcoder/reglas_homologacion/ETNIA.xlsx"
archivo_sexo = "z_teamcoder/reglas_homologacion/SEXO.xlsx"
archivo_salida_xlsx = "z_teamcoder/resultados/z_teamcoder_descargaReporte_homologado.xlsx"
archivo_salida_txt = "z_teamcoder/resultados/z_teamcoder_descargaReporte_homologado.txt"

if os.path.exists(archivo_salida_xlsx):
    try:
        os.remove(archivo_salida_xlsx)
        print(f"Archivo anterior eliminado: {archivo_salida_xlsx}")
        logging.info(f"Archivo anterior eliminado: {archivo_salida_xlsx}")
    except Exception as e:
        print(f"No se pudo eliminar el archivo {archivo_salida_xlsx}: {e}")
        logging.warning(f"No se pudo eliminar el archivo {archivo_salida_xlsx}: {e}")

if os.path.exists(archivo_salida_txt):
    try:
        os.remove(archivo_salida_txt)
        print(f"Archivo anterior eliminado: {archivo_salida_txt}")
        logging.info(f"Archivo anterior eliminado: {archivo_salida_txt}")
    except Exception as e:
        print(f"No se pudo eliminar el archivo {archivo_salida_txt}: {e}")
        logging.warning(f"No se pudo eliminar el archivo {archivo_salida_txt}: {e}")


# ========================
# LECTURA DE ARCHIVOS
# ========================
df = pd.read_excel(archivo_original)

for col in ["HISTORIA", "RUN_PACIENTE", "NRO_HISTORIA", "EPISODIO"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()


df_ocup = pd.read_excel(archivo_ocupacion, sheet_name="Hoja1")
df_prog = pd.read_excel(archivo_programa)
df_esp = pd.read_excel(archivo_especialidad)
df_servalt = pd.read_excel(archivo_servalt)
df_serving = pd.read_excel(archivo_serving)
df_etnia = pd.read_excel(archivo_etnia)
df_sexo = pd.read_excel(archivo_sexo)

# Normalizar nombres de columnas
for d in [df, df_ocup, df_prog, df_esp, df_servalt, df_serving, df_etnia, df_sexo]:
    d.rename(columns=lambda x: x.strip(), inplace=True)

# ========================
# 🔧 CORRECCIÓN PROGRAMA (mantener formato original '00')
# ========================
if "PROGRAMA" in df.columns:
    df["PROGRAMA"] = df["PROGRAMA"].astype(str).apply(
        lambda x: x.zfill(2) if x.replace('.', '', 1).isdigit() else x
    )

# ========================
# FUNCIONES AUXILIARES
# ========================
def normalizar_codigo(valor):
    if pd.isna(valor):
        return ""
    valor = str(valor).strip().upper()
    valor = valor.replace("–", "-").replace("—", "-").replace(" ", "")
    if valor.endswith(".0"):
        valor = valor[:-2]
    return valor

# ========================
# 1) HOMOLOGAR OCUPACION
# ========================
df["OCUPACION"] = df["OCUPACION"].astype(str).str.strip().str.upper()
df_ocup["Codigo"] = df_ocup["Codigo"].astype(str).str.strip().str.upper()

df = df.merge(df_ocup, left_on="OCUPACION", right_on="Codigo", how="left")

def asignar_categoria_ocup(row):
    if pd.isna(row["OCUPACION"]):
        return 99
    elif pd.isna(row["Categoria"]):
        return 0
    else:
        return int(row["Categoria"])

df["OCUPACION"] = df.apply(asignar_categoria_ocup, axis=1)
df.drop(columns=["Codigo", "Categoria"], inplace=True, errors="ignore")

# ========================
# 2) HOMOLOGAR REGCON_01
# ========================
df["REGCON_01"] = df["REGCON_01"].astype(str).str.strip().str.upper()
df_prog["AUXIT_Desc"] = df_prog["AUXIT_Desc"].astype(str).str.strip().str.upper()

df = df.merge(df_prog[["AUXIT_Desc", "Teamcode"]],
              left_on="REGCON_01",
              right_on="AUXIT_Desc",
              how="left")

def asignar_categoria_prog(row):
    if pd.isna(row["REGCON_01"]):
        return 99
    elif pd.isna(row["Teamcode"]):
        return 0
    else:
        return int(row["Teamcode"])

df["REGCON_01"] = df.apply(asignar_categoria_prog, axis=1)
df.drop(columns=["AUXIT_Desc", "Teamcode"], inplace=True, errors="ignore")

# ========================
# 3) HOMOLOGAR ESPECIALIDAD
# ========================
df["ESPECIALIDAD"] = df["ESPECIALIDAD"].astype(str).str.strip().str.upper()
df_esp.rename(columns=lambda x: x.strip().lower(), inplace=True)
df_esp["codigo_trakcare"] = df_esp["codigo_trakcare"].astype(str).str.strip().str.upper()

df["ESPECIALIDAD"] = df["ESPECIALIDAD"].apply(normalizar_codigo)
df_esp["codigo_trakcare"] = df_esp["codigo_trakcare"].apply(normalizar_codigo)

df = df.merge(df_esp[["codigo_trakcare", "codigo_teamcoder"]],
              left_on="ESPECIALIDAD",
              right_on="codigo_trakcare",
              how="left")

def asignar_categoria_esp(row):
    if pd.isna(row["ESPECIALIDAD"]):
        return pd.NA
    elif pd.isna(row["codigo_teamcoder"]):
        return pd.NA
    else:
        return int(row["codigo_teamcoder"])

df["ESPECIALIDAD"] = df.apply(asignar_categoria_esp, axis=1)
df.drop(columns=["codigo_trakcare", "codigo_teamcoder"], inplace=True, errors="ignore")

# ========================
# 4) HOMOLOGAR SERVALT
# ========================
df["SERVALT"] = df["SERVALT"].astype(str).str.strip().str.upper()
df_servalt.rename(columns=lambda x: x.strip().lower(), inplace=True)
df_servalt["codigo_hds"] = df_servalt["codigo_hds"].astype(str).str.strip().str.upper()

df["SERVALT"] = df["SERVALT"].apply(normalizar_codigo)
df_servalt["codigo_hds"] = df_servalt["codigo_hds"].apply(normalizar_codigo)

df = df.merge(df_servalt[["codigo_hds", "codigo"]],
              left_on="SERVALT",
              right_on="codigo_hds",
              how="left")

def asignar_categoria_servalt(row):
    if pd.isna(row["SERVALT"]):
        return pd.NA
    elif pd.isna(row["codigo"]):
        return pd.NA
    else:
        return int(row["codigo"])

df["SERVALT"] = df.apply(asignar_categoria_servalt, axis=1)
df.drop(columns=["codigo_hds", "codigo"], inplace=True, errors="ignore")

# ========================
# 5) HOMOLOGAR SERVING
# ========================
df["SERVING"] = df["SERVING"].astype(str).str.strip().str.upper()
df_serving.rename(columns=lambda x: x.strip().lower(), inplace=True)
df_serving["codigo_hds"] = df_serving["codigo_hds"].astype(str).str.strip().str.upper()

df["SERVING"] = df["SERVING"].apply(normalizar_codigo)
df_serving["codigo_hds"] = df_serving["codigo_hds"].apply(normalizar_codigo)

df = df.merge(df_serving[["codigo_hds", "codigo"]],
              left_on="SERVING",
              right_on="codigo_hds",
              how="left")

def asignar_categoria_serving(row):
    if pd.isna(row["SERVING"]):
        return pd.NA
    elif pd.isna(row["codigo"]):
        return pd.NA
    else:
        return int(row["codigo"])

df["SERVING"] = df.apply(asignar_categoria_serving, axis=1)
df.drop(columns=["codigo_hds", "codigo"], inplace=True, errors="ignore")

# ========================
# 6) HOMOLOGAR ETNIA
# ========================
df["ETNIA"] = df["ETNIA"].astype(str).str.strip().str.upper()
df_etnia.rename(columns=lambda x: x.strip().lower(), inplace=True)
df_etnia["codigo_trakcare"] = df_etnia["codigo_trakcare"].astype(str).str.strip().str.upper()

df["ETNIA"] = df["ETNIA"].apply(normalizar_codigo)
df_etnia["codigo_trakcare"] = df_etnia["codigo_trakcare"].apply(normalizar_codigo)

df = df.merge(df_etnia[["codigo_trakcare", "codigo"]],
              left_on="ETNIA",
              right_on="codigo_trakcare",
              how="left")

def asignar_categoria_etnia(row):
    if pd.isna(row["ETNIA"]):
        return pd.NA
    elif pd.isna(row["codigo"]):
        return pd.NA
    else:
        return int(row["codigo"])

df["ETNIA"] = df.apply(asignar_categoria_etnia, axis=1)
df.drop(columns=["codigo_trakcare", "codigo"], inplace=True, errors="ignore")

# ========================
# 7) HOMOLOGAR SEXO
# ========================
df["SEXO"] = df["SEXO"].astype(str).str.strip().str.upper()
df_sexo.rename(columns=lambda x: x.strip().lower(), inplace=True)
df_sexo["codigo_trakcare"] = df_sexo["codigo_trakcare"].astype(str).str.strip().str.upper()

df["SEXO"] = df["SEXO"].apply(normalizar_codigo)
df_sexo["codigo_trakcare"] = df_sexo["codigo_trakcare"].apply(normalizar_codigo)

df = df.merge(df_sexo[["codigo_trakcare", "codigo"]],
              left_on="SEXO",
              right_on="codigo_trakcare",
              how="left")

def asignar_categoria_sexo(row):
    if pd.isna(row["SEXO"]):
        return pd.NA
    elif pd.isna(row["codigo"]):
        return pd.NA
    else:
        return str(row["codigo"]).zfill(2)  # mantiene el 0 inicial

df["SEXO"] = df.apply(asignar_categoria_sexo, axis=1)
df.drop(columns=["codigo_trakcare", "codigo"], inplace=True, errors="ignore")


# ========================
# 7.1) FORMATEAR CAMPOS GEOGRÁFICOS
# ========================
if "DIST_PAC" in df.columns:
    df["DIST_PAC"] = df["DIST_PAC"].astype(str).str.strip().apply(
        lambda x: x.zfill(3) if x.isdigit() else x
    )

if "MRES" in df.columns:
    df["MRES"] = df["MRES"].astype(str).str.strip().apply(
        lambda x: x.zfill(5) if x.isdigit() else x
    )
    
# ========================
# 7.2) FORMATEAR RUT (MEDICOALT y CIP)
# ========================
for col in ["MEDICOALT", "CIP"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.upper()    


# ========================
# 8) EXPORTACIONES
# ========================
df.to_excel(archivo_salida_xlsx, index=False)
df.to_csv(archivo_salida_txt, sep="|", index=False, encoding="utf-8")

print("Archivo original:", len(df), "registros")
print("Exportado en formatos:")
print("Excel (.xlsx):", archivo_salida_xlsx)
print("Texto (.txt) delimitado por '|':", archivo_salida_txt)
print("Homologación completada con columnas: OCUPACION, REGCON_01, ESPECIALIDAD, SERVALT, SERVING, ETNIA y SEXO (manteniendo PROGRAMA original)")
