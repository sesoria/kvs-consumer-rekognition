FROM python:3.10-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar dependencias del sistema que requiere OpenCV
RUN apt-get update && apt-get install -y \
    libglib2.0-0 libsm6 libxext6 libxrender-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar el código fuente
COPY . .

# Instalar dependencias de Python
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Comando por defecto (puedes cambiar el script si quieres otro)
CMD ["python3", "kvs_consumer_library_example.py"]
