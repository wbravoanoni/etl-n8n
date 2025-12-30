from datetime import datetime
from zoneinfo import ZoneInfo

print("===== PRUEBA DE FECHAS PYTHON =====\n")

# Hora local (lo que Python considera 'local')
now_local = datetime.now()
print(f"datetime.now()                : {now_local}")
print(f"Fecha (local)                 : {now_local.date()}")
print()

# Hora UTC
now_utc = datetime.utcnow()
print(f"datetime.utcnow()             : {now_utc}")
print(f"Fecha (UTC)                   : {now_utc.date()}")
print()

# Hora explícita Chile
now_chile = datetime.now(ZoneInfo("America/Santiago"))
print(f"datetime.now(America/Santiago): {now_chile}")
print(f"Fecha (Chile)                 : {now_chile.date()}")
print()

print("Zona horaria explícita Chile  :", now_chile.tzinfo)
print("\n=================================")
