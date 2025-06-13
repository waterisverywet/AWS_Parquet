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

@app.get("/query-parquet")
async def query_parquet(
    statename: str,
    dname: str,
    page: int = Query(0, ge=0),
    page_size: int = Query(10, ge=1)
):
    if not statename.islower() or not dname.islower():
        raise HTTPException(400, "Names must be lowercase")

    s3_path = f"s3://{S3_BUCKET}/state={statename}/dname={dname}/{dname}.parquet"
    
    try:
        with closing(duckdb.connect()) as con:
            # Set S3 credentials
            con.execute(f"SET s3_region='{AWS_REGION}';")
            con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';")
            con.execute(f"SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';")
            
            # Get total row count
            count_query = f"SELECT COUNT(*) FROM read_parquet('{s3_path}')"
            total_rows = con.execute(count_query).fetchone()[0]
            
            # Get paginated data
            data_query = f"""
                SELECT * 
                FROM read_parquet('{s3_path}') 
                LIMIT {page_size} 
                OFFSET {page * page_size}
            """
            result = con.execute(data_query)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            data = [dict(zip(columns, row)) for row in rows]

            return {
                "data": data,
                "columns": columns,
                "total_rows": total_rows,
                "page": page,
                "page_size": page_size
            }
    except duckdb.IOException as e:
        raise HTTPException(404, f"File not found: {str(e)}")
    except Exception as e:
        raise HTTPException(500, f"Query failed: {str(e)}")
