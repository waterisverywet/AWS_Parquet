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
    
):
    if not statename.islower() or not dname.islower():
        raise HTTPException(400, "Names must be lowercase")

    s3_path = f"s3://{S3_BUCKET}/state={statename}/dname={dname}/{dname}.parquet"

    try:
        with closing(duckdb.connect()) as con:
            con.execute(f"SET s3_region='{AWS_REGION}';")
            con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';")
            con.execute(f"SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';")
            query = f"SELECT * FROM read_parquet('{s3_path}')"
            result = con.execute(query)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            data = [dict(zip(columns, row)) for row in rows]
            query2= f"SELECT * FROM read_parquet('{s3_path}')"
            result2 = con.execute(query2)
            columns2 = [desc[0] for desc in result2.description]
            rows2 = result2.fetchall()
            data2 = [dict(zip(columns2, row)) for row in rows2]

            return {
                "data": data,
                "columns": columns,
                "row_count": len(data2)
            }
    except duckdb.IOException as e:
        raise HTTPException(404, f"File not found: {str(e)}")
    except Exception as e:
        raise HTTPException(500, f"Query failed: {str(e)}")
