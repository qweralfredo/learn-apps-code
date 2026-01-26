from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import duckdb
import pyarrow as pa
import time
from datetime import datetime
import logging
from prometheus_client import Counter, Histogram, generate_latest
from fastapi.responses import Response

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Métricas Prometheus
query_counter = Counter('duckdb_queries_total', 'Total de queries executadas')
query_duration = Histogram('duckdb_query_duration_seconds', 'Duração das queries')

# Aplicação FastAPI
app = FastAPI(
    title="DuckDB Micro-Lakehouse API",
    description="API para consultas OLAP em tempo real com DuckDB",
    version="1.0.0"
)

# Conexão DuckDB
con = None

class QueryRequest(BaseModel):
    sql: str
    params: Optional[dict] = None

class QueryResponse(BaseModel):
    data: List[dict]
    rows: int
    execution_time_ms: float

class InsertRequest(BaseModel):
    table: str
    data: List[dict]

@app.on_event("startup")
async def startup_event():
    global con
    con = duckdb.connect('/data/lakehouse.db')
    logger.info("DuckDB conectado com sucesso")
    
    # Criar tabelas de exemplo
    con.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DECIMAL(10,2),
            timestamp BIGINT,
            status VARCHAR
        )
    """)
    logger.info("Tabelas inicializadas")

@app.on_event("shutdown")
async def shutdown_event():
    if con:
        con.close()
        logger.info("DuckDB desconectado")

@app.get("/")
async def root():
    return {
        "service": "DuckDB Micro-Lakehouse",
        "status": "running",
        "version": "1.0.0"
    }

@app.get("/health")
async def health():
    try:
        con.execute("SELECT 1").fetchone()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")

@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    query_counter.inc()
    
    try:
        start = time.perf_counter()
        
        # Executar query
        result = con.execute(request.sql).fetchdf()
        
        elapsed = time.perf_counter() - start
        query_duration.observe(elapsed)
        
        # Converter para lista de dicionários
        data = result.to_dict('records')
        
        logger.info(f"Query executada em {elapsed*1000:.1f}ms, {len(data)} linhas")
        
        return QueryResponse(
            data=data,
            rows=len(data),
            execution_time_ms=elapsed * 1000
        )
        
    except Exception as e:
        logger.error(f"Erro na query: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/insert")
async def insert_data(request: InsertRequest):
    try:
        import pandas as pd
        
        start = time.perf_counter()
        
        # Converter para DataFrame
        df = pd.DataFrame(request.data)
        
        # Inserir no DuckDB
        con.execute(f"INSERT INTO {request.table} SELECT * FROM df")
        
        elapsed = time.perf_counter() - start
        
        logger.info(f"{len(request.data)} linhas inseridas em {elapsed*1000:.1f}ms")
        
        return {
            "status": "success",
            "rows_inserted": len(request.data),
            "execution_time_ms": elapsed * 1000
        }
        
    except Exception as e:
        logger.error(f"Erro ao inserir: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/tables")
async def list_tables():
    try:
        result = con.execute("""
            SELECT table_name, estimated_size 
            FROM duckdb_tables()
        """).fetchdf()
        
        return {
            "tables": result.to_dict('records')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/table/{table_name}/info")
async def table_info(table_name: str):
    try:
        # Schema
        schema = con.execute(f"DESCRIBE {table_name}").fetchdf()
        
        # Estatísticas
        stats = con.execute(f"""
            SELECT 
                COUNT(*) as row_count,
                pg_size_pretty(pg_total_relation_size('{table_name}')) as total_size
            FROM {table_name}
        """).fetchdf()
        
        return {
            "table": table_name,
            "schema": schema.to_dict('records'),
            "statistics": stats.to_dict('records')[0]
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type="text/plain")

@app.get("/analytics/summary")
async def analytics_summary(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None)
):
    """Resumo de analytics"""
    try:
        where_clause = ""
        if start_date and end_date:
            where_clause = f"WHERE timestamp >= {start_date} AND timestamp <= {end_date}"
        
        result = con.execute(f"""
            SELECT 
                COUNT(*) as total_transactions,
                COUNT(DISTINCT customer_id) as unique_customers,
                ROUND(SUM(amount), 2) as total_revenue,
                ROUND(AVG(amount), 2) as avg_transaction,
                status,
                COUNT(*) as count_by_status
            FROM transactions
            {where_clause}
            GROUP BY status
        """).fetchdf()
        
        return {
            "summary": result.to_dict('records')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
