# AWS Parquet Query Engine

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104.0-green)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow)](LICENSE)

> A high-performance REST API for querying Parquet files stored in AWS S3 using DuckDB

## Overview

AWS Parquet Query Engine is a lightweight FastAPI application that provides an efficient interface for querying Parquet files hosted on Amazon S3. It leverages DuckDB's columnar query engine to deliver fast, memory-efficient data retrieval with built-in pagination support.

### Key Features

- **Fast Queries**: Utilizes DuckDB's high-performance columnar database engine
- **S3 Integration**: Seamless integration with AWS S3 for distributed data storage
- **Pagination Support**: Built-in pagination for handling large datasets
- **RESTful API**: Clean, intuitive REST API endpoints
- **Environment Configuration**: Secure credential management via environment variables
- **Error Handling**: Comprehensive error handling with detailed HTTP responses

## Tech Stack

- **Framework**: FastAPI (modern async Python web framework)
- **Query Engine**: DuckDB (in-process analytical database)
- **Web Server**: Uvicorn (ASGI server)
- **AWS Integration**: Python boto3-compatible S3 access
- **Configuration**: python-dotenv for environment management

## Prerequisites

- Python 3.8 or higher
- AWS S3 bucket with Parquet files
- AWS credentials (Access Key ID and Secret Access Key)
- Internet connection for S3 access

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/waterisverywet/AWS_Parquet.git
cd AWS_Parquet
```

### 2. Create Virtual Environment

```bash
# Using venv
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\\Scripts\\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the project root:

```env
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
S3_BUCKET=your-bucket-name
```

Alternatively, copy from the template:

```bash
cp .env.example .env
# Edit .env with your credentials
```

## Usage

### Running the Server

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`

### API Documentation

Interactive API documentation is available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### Query Parquet Endpoint

**Endpoint**: `GET /query-parquet`

**Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `statename` | string | Yes | State name (must be lowercase) |
| `dname` | string | Yes | Data name/table identifier (must be lowercase) |
| `page` | integer | No | Page number (0-indexed, default: 0) |
| `page_size` | integer | No | Records per page (default: 10, min: 1) |

**Example Request**:

```bash
curl -X GET "http://localhost:8000/query-parquet?statename=california&dname=sales&page=0&page_size=10"
```

**Success Response** (HTTP 200):

```json
{
  "data": [
    {
      "id": 1,
      "name": "Product A",
      "sales": 1000
    },
    {
      "id": 2,
      "name": "Product B",
      "sales": 1500
    }
  ],
  "total_rows": 1000,
  "page": 0,
  "page_size": 10
}
```

**Error Responses**:

- **400 Bad Request**: Names must be lowercase
- **404 Not Found**: Parquet file not found in S3
- **500 Internal Server Error**: Query execution failed

### S3 Path Structure

The application constructs S3 paths as follows:

```
s3://{S3_BUCKET}/state={statename}/dname={dname}/{dname}.parquet
```

**Example**: For `statename=california` and `dname=sales`:
```
s3://my-bucket/state=california/dname=sales/sales.parquet
```

## Project Structure

```
AWS_Parquet/
├── main.py                 # Main FastAPI application
├── requirements.txt        # Python dependencies
├── .env.example           # Environment variables template
├── .gitignore             # Git ignore rules
└── README.md              # This file
```

## Dependencies

- **fastapi** (0.104.0+): Modern Python web framework
- **uvicorn**: ASGI web server
- **duckdb**: In-process analytical database
- **python-dotenv**: Environment variable management
- **pytz**: Timezone support
- **fastapi-pagination**: Pagination utilities

## Code Overview

### main.py

The core application file containing:

- **Startup Event Handler**: Initializes DuckDB HTTP filesystem support
- **Query Endpoint**: Processes S3 parquet queries with pagination
- **Error Handling**: Comprehensive exception management
- **Configuration**: Loads AWS credentials from environment

## Performance Considerations

1. **DuckDB Optimization**: DuckDB's columnar format is optimized for analytical queries
2. **Pagination**: Limits data transfer for large datasets
3. **Connection Pooling**: Uses context managers for efficient resource management
4. **S3 Direct Access**: Queries Parquet files directly from S3 without downloading

## Deployment

### Docker Deployment

Create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

Build and run:

```bash
docker build -t aws-parquet-api .
docker run -p 8000:8000 --env-file .env aws-parquet-api
```

### AWS Lambda Deployment

Use AWS Lambda with API Gateway for serverless deployment. Requires `mangum` wrapper:

```bash
pip install mangum
```

Create `handler.py`:

```python
from mangum import Mangum
from main import app

handler = Mangum(app)
```

## Troubleshooting

### File Not Found Error

**Problem**: HTTP 404 when querying a file

**Solution**:
- Verify S3 path structure matches expected format
- Confirm AWS credentials have S3 read permissions
- Check bucket name and region configuration

### Authentication Errors

**Problem**: AWS credential errors

**Solution**:
- Verify `.env` file contains correct credentials
- Check AWS IAM permissions for S3 access
- Ensure credentials are not expired

### Timeout Issues

**Problem**: Queries timing out on large files

**Solution**:
- Reduce `page_size` parameter
- Use smaller parquet files or partitions
- Increase timeout settings in production deployment

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Author

[@waterisverywet](https://github.com/waterisverywet)

## Support

For issues, questions, or suggestions, please open an issue on the [GitHub repository](https://github.com/waterisverywet/AWS_Parquet/issues).

---

**Last Updated**: January 2026
