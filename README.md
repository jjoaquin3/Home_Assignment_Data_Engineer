# **Sales Dashboard API & Data Pipeline**
🚀 **Procesamiento de datos de ventas con ETL y API REST usando FastAPI, SQLite y Spark.**

Este proyecto procesa datos de ventas desde un archivo CSV, los limpia, los almacena en SQLite y expone una API REST con FastAPI.

---

## ** 1. Estructura del Proyecto**
```
/Solution
│── /API                # Código del backend (FastAPI)
│   │── api.py          # API principal que une todas las versiones
│   │── api_simple.py   # API básica sin filtros
│   │── api_filter.py   # API con filtros y paginación
│── /Dataset
│   │── product_dataset.csv  # Dataset con datos de ventas
│── /Scripts
│   │── Solution_Spark.ipynb  # Notebook de procesamiento de datos
│   │── SQL_fetching_aggregated.ipynb  # Consultas SQL y validación de datos
│   │── /SQL
│   │   ├── aggregated_metrics.sql
│   │   ├── outliers.sql
│   │   ├── total_sales_day.sql
│   │   ├── total_sales_product.sql
/Stack
│── docker-compose.yaml  # Orquestación de contenedores
│── /FastAPI
│   │── Dockerfile  # Configuración del backend en FastAPI
│   │── requirements.txt  # Dependencias de la API
│── /Spark
│   │── Dockerfile  # Configuración de Spark
│   │── requirements.txt
│── /SQLite
│   │── Dockerfile  # Configuración de SQLite en Docker
│   │── /data
│   │   ├── sales_dashboard.db  # Base de datos SQLite
```

---

## ** 2. Cómo Levantar el Proyecto**
### **🔹 Clonar el repositorio**
```bash
git clone https://github.com/jjoaquin3/Home_Assignment_Data_Engineer
cd Home_Assignment_Data_Engineer/Stack
```

### **🔹 Construir y levantar los contenedores con Docker**
```bash
docker-compose up --build -d
```
 Esto levanta los servicios de **Spark, SQLite y FastAPI**.

### **🔹 Ejecutar el procesamiento de datos en Spark**
Abrir Jupyter Notebook desde Spark en `http://localhost:8888` y ejecutar:
- **`Solution/Scripts/Solution_Spark.ipynb`**

o bien abril usar como Remote Jupyter Server con http://127.0.0.1:8888/tree como kernel para ejecución
- **`Solution/Scripts/Solution_Spark.ipynb`**
  
Esto procesará los datos y los guardará en SQLite.

### **🔹 Probar la API en Swagger UI**
Acceder a **Swagger UI** en:
```
http://localhost:8000/docs
```

<a href="https://github.com/jjoaquin3/Home_Assignment_Data_Engineer/blob/main/Solution/Images/A1.JPG?raw=true" target="_blank">
    <img src="https://github.com/jjoaquin3/Home_Assignment_Data_Engineer/blob/main/Solution/Images/A1.JPG?raw=true" alt="Docs1" width="400px">
</a>


🔹 **Redoc UI (alternativa)**:  
```
http://localhost:8000/redoc
```
<a href="https://github.com/jjoaquin3/Home_Assignment_Data_Engineer/blob/main/Solution/Images/A2.JPG?raw=true" target="_blank">
    <img src="https://github.com/jjoaquin3/Home_Assignment_Data_Engineer/blob/main/Solution/Images/A2.JPG?raw=true" alt="Docs2" width="400px">
</a>

---

## ** 3. API Endpoints**
### **🔹 Version 1 (`/v1` - API Simple)**
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| `GET` | `/v1/sales/product` | Devuelve ventas por producto |
| `GET` | `/v1/sales/day` | Devuelve ventas por día |
| `GET` | `/v1/sales/category` | Devuelve métricas agregadas por categoría |
| `GET` | `/v1/sales/outliers` | Devuelve transacciones atípicas |

### **🔹 Version 2 (`/v2` - API con Filtros y Paginación)**
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| `POST` | `/v2/sales/product` | Filtra ventas por `product`, `category` |
| `POST` | `/v2/sales/day` | Filtra ventas por `start_date`, `end_date` |
| `POST` | `/v2/sales/category` | Devuelve métricas con paginación |
| `POST` | `/v2/sales/outliers` | Devuelve outliers con paginación |

---

## ** 4. Ejemplos de Uso con Postman**
### **🔹 Obtener ventas por producto con filtros**
 **Cuerpo JSON (Body -> raw -> JSON)**
```json
{
    "product": "Gadget-X",
    "category": "Gadget",
    "page": 1,
    "page_size": 5
}
```
 **Ejecutar en cURL**
```bash
curl -X 'POST' 'http://localhost:8000/v2/sales/product' \
-H 'Content-Type: application/json' \
-d '{
    "product": "Gadget-X",
    "category": "Gadget",
    "page": 1,
    "page_size": 5
}'
```

### **🔹 Obtener ventas por día en un rango de fechas**
```json
{
    "start_date": "2024-07-01",
    "end_date": "2024-07-10",
    "page": 2,
    "page_size": 10
}
```

---

## ** 5. Tecnologías Utilizadas**
🔹 **FastAPI** → Para exponer la API REST.  
🔹 **Spark + Pandas** → Para procesar los datos del CSV.  
🔹 **SQLite** → Para almacenar datos procesados y servir como base de datos.  
🔹 **Docker + Docker Compose** → Para contenerizar y orquestar la aplicación.  

---

## ** 6. Cómo Apagar los Contenedores**
Para detener los servicios, usa:
```bash
docker-compose down
```
Esto detendrá y eliminará los contenedores sin borrar los datos almacenados en SQLite.

---

🔥 **¡Listo! 
