from fastapi import FastAPI, Request
import duckdb

app = FastAPI()

@app.post("/query")
async def run_query(params: dict):
    condition = params.get("condition", "TRUE")
    query = f"SELECT * FROM read_parquet('agar-malwa.parquet') WHERE {condition}"
    result = duckdb.query(query).fetchall()
    return {"data": result}

