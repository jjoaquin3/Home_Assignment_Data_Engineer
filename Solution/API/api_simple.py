from fastapi import APIRouter
import sqlite3
import pandas as pd
import os

# 📍 Rutas dentro del contenedor
DB_PATH = "/workspace/db/sales_dashboard.db"
SQL_FOLDER = "/workspace/sql/"

router = APIRouter(prefix="/v1", tags=["Versión Simple"])

# Función para ejecutar consultas SQL
def fetch_data_from_sql(sql_file):
    sql_path = os.path.join(SQL_FOLDER, sql_file)
    if not os.path.exists(sql_path):
        return {"error": f"Archivo {sql_file} no encontrado"}
    
    with open(sql_path, "r") as file:
        query = file.read()

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df.to_dict(orient="records")

# Endpoints de la versión simple
@router.get("/sales/product", summary="Obtener ventas por producto (Simple)")
def get_sales_by_product():
    return fetch_data_from_sql("total_sales_product.sql")

@router.get("/sales/day", summary="Obtener ventas por día (Simple)")
def get_sales_by_day():
    return fetch_data_from_sql("total_sales_day.sql")

@router.get("/sales/category", summary="Obtener métricas agregadas (Simple)")
def get_sales_by_category():
    return fetch_data_from_sql("aggregated_metrics.sql")

@router.get("/sales/outliers", summary="Obtener transacciones atípicas (Simple)")
def get_outliers():
    return fetch_data_from_sql("outliers.sql")
