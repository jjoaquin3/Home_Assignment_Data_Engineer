from fastapi import APIRouter
from pydantic import BaseModel
import sqlite3
import pandas as pd
import os
from typing import Optional, List

# 📍 Rutas dentro del contenedor
DB_PATH = "/workspace/db/sales_dashboard.db"
SQL_FOLDER = "/workspace/sql/"

router = APIRouter(prefix="/v2", tags=["Versión con Filtros"])

# Modelo de Filtros con Pydantic
class SalesFilter(BaseModel):
    product: Optional[str] = None
    category: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    page: Optional[int] = 1
    page_size: Optional[int] = 10

# Función para obtener todos los datos sin filtros
def fetch_full_data(sql_file) -> pd.DataFrame:
    sql_path = os.path.join(SQL_FOLDER, sql_file)
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"Archivo {sql_file} no encontrado")
    
    with open(sql_path, "r") as file:
        query = file.read().strip()

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df

# Función para aplicar filtros a un DataFrame
def apply_filters(df: pd.DataFrame, filters: SalesFilter) -> pd.DataFrame:
    available_columns = df.columns.tolist()  # ✅ Listar columnas disponibles

    if "category" in available_columns and filters.category:
        df = df[df["category"] == filters.category]
    if "product" in available_columns and filters.product:
        df = df[df["product"] == filters.product]
    if "date" in available_columns and filters.start_date:
        df = df[df["date"] >= filters.start_date]
    if "date" in available_columns and filters.end_date:
        df = df[df["date"] <= filters.end_date]

    return df


# Función para aplicar paginación
def apply_pagination(df: pd.DataFrame, page: int, page_size: int) -> dict:
    total_records = len(df)
    page = max(1, page)  # Asegurar que la página sea al menos 1
    page_size = max(1, page_size)  # Asegurar que el tamaño de página sea válido

    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    paginated_data = df.iloc[start_idx:end_idx].to_dict(orient="records")

    return {
        "total_records": total_records,
        "page": page,
        "page_size": page_size,
        "total_pages": (total_records + page_size - 1) // page_size,
        "data": paginated_data
    }

# Endpoints usando funciones modulares
@router.post("/sales/product", summary="Obtener ventas por producto con filtros y paginación")
def get_sales_by_product(filters: SalesFilter):
    df = fetch_full_data("total_sales_product.sql")
    df = apply_filters(df, filters)
    return apply_pagination(df, filters.page, filters.page_size)

@router.post("/sales/day", summary="Obtener ventas por día con filtros y paginación")
def get_sales_by_day(filters: SalesFilter):
    df = fetch_full_data("total_sales_day.sql")
    df = apply_filters(df, filters)
    return apply_pagination(df, filters.page, filters.page_size)

@router.post("/sales/category", summary="Obtener métricas agregadas con paginación")
def get_sales_by_category(filters: SalesFilter):
    df = fetch_full_data("aggregated_metrics.sql")
    return apply_pagination(df, filters.page, filters.page_size)

@router.post("/sales/outliers", summary="Obtener transacciones atípicas con paginación")
def get_outliers(filters: SalesFilter):
    df = fetch_full_data("outliers.sql")
    return apply_pagination(df, filters.page, filters.page_size)
