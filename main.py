from fastapi import FastAPI, HTTPException
import duckdb
from contextlib import closing
from dotenv import load_dotenv
load_dotenv()
import os
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
async def query_parquet(statename: str, dname: str):
    """
    Query Parquet files from S3 with parameters:
    - statename: State directory name (e.g. 'madhyapradesh')
    - dname: District directory name (e.g. 'agar-malwa')
    """
    
    if not statename.islower() or not dname.islower():
        raise HTTPException(400, "Names must be lowercase")

    
    s3_path = f"s3://{S3_BUCKET}/state={statename}/dname={dname}/{dname}.parquet"

    try:
        with closing(duckdb.connect()) as con:
            # Set AWS credentials and region
            con.execute(f"SET s3_region='{AWS_REGION}';")
            con.execute(f"SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';")
            con.execute(f"SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';")

            # Query the parquet file
            result = con.execute("""
                SELECT * FROM read_parquet(?)
            """, [s3_path])

            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()

            return {
                "data": [dict(zip(columns, row)) for row in rows],
                "columns": columns,
                "row_count": len(rows)
            }
    except duckdb.IOException as e:
        raise HTTPException(404, f"File not found: {str(e)}")
    except Exception as e:
        raise HTTPException(500, f"Query failed: {str(e)}")
