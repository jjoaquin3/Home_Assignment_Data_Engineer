# Usar Python como base
FROM python:3.9

# Establecer el directorio de trabajo
WORKDIR /workspace

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto 8000
EXPOSE 8000

# Comando para ejecutar la API
CMD ["uvicorn", "api.api:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
