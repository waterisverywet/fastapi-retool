from fastapi import FastAPI
from pydantic import BaseModel
import duckdb

app = FastAPI()

# Define the request body schema
class QueryRequest(BaseModel):
    condition: str = "TRUE"  # Default: no filter

@app.post("/query")
async def run_query(request: QueryRequest):
    # Compose the SQL query
    query = f"SELECT * FROM read_parquet('agar-malwa.parquet') WHERE {request.condition}"
    try:
        # Execute the query
        result = duckdb.query(query).fetchall()
        # Optionally, get column names for better output
        columns = [desc[0] for desc in duckdb.query(query).description]
        # Return as list of dicts for easy use in Retool/Table
        data = [dict(zip(columns, row)) for row in result]
        return {"data": data}
    except Exception as e:
        return {"error": str(e)}
