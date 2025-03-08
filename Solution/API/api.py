from fastapi import FastAPI
from .api_simple import router as router_simple
from .api_filter import router as router_filter

app = FastAPI(
    title="Sales Dashboard API",
    description="API para consultar métricas de ventas procesadas desde SQLite.",
    version="2.0",
)

# 🔹 Cargar las versiones de la API
app.include_router(router_simple)
app.include_router(router_filter)
