from fastapi import FastAPI, HTTPException, Query
import duckdb
from contextlib import closing
from dotenv import load_dotenv
import os

load_dotenv()
app = FastAPI()

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

@app.on_event("startup")
async def startup():
    duckdb.execute("INSTALL httpfs;")
    duckdb.execute("LOAD httpfs;")

from typing import Optional

@app.get("/query-parquet")
async def query_parquet(
    statename: str,
    dname: str,
    page: Optional[int] = Query(0, ge=0),  # Allow Optional
    page_size: Optional[int] = Query(10, ge=1)
):
    # Convert None/undefined to defaults
    page = page or 0
    page_size = page_size or 10
    # Rest of your code

    if not statename.islower() or not dname.islower():
        raise HTTPException(400, "Names must be lowercase")

    s3_path = f"s3://{S3_BUCKET}/state={statename}/dname={dname}/{dname}.parquet"
    
    try:
        with closing(duckdb.connect()) as con:
            con.execute(f"SET s3_region='{AWS_REGION}';")
            con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';")
            con.execute(f"SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';")
            
            # Get total row count
            count_query = f"SELECT COUNT(*) FROM read_parquet('{s3_path}')"
            total_rows = con.execute(count_query).fetchone()[0]
            
            # Get paginated data (convert to 0-based offset)
            data_query = f"""
                SELECT * 
                FROM read_parquet('{s3_path}') 
                ORDER BY (SELECT 0)  -- Maintain consistent order
                LIMIT {page_size} 
                OFFSET {(page - 1) * page_size}
            """
            result = con.execute(data_query)
            columns = [desc[0] for desc in result.description]
            data = [dict(zip(columns, row)) for row in result.fetchall()]

            return {
                "data": data,
                "total_rows": total_rows,
                "page": page,
                "page_size": page_size
            }
    except duckdb.IOException as e:
        raise HTTPException(404, f"File not found: {str(e)}")
    except Exception as e:
        raise HTTPException(500, f"Query failed: {str(e)}")
