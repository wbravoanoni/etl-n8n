## Primeros Pasos

1. Version en desarrollo
    - Python 3.12.5
2. Version en produccion
    - Python 3.12.3
3. Instalación de dependencias
   - python -m pip install -r requirements.txt
4. Actualizar dependencias
   - pip freeze > requirements.txt 

5. Actualizar crontab
   - ssh dtd@10.68.96.120 "crontab -l > /tmp/crontab.txt"
   - scp dtd@10.68.96.120:/tmp/crontab.txt "C:\Users\Winston Bravo\Desktop\etl-n8n"

6. Respaldo de scripst 
