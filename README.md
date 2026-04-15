# Online Art Supply Store — Azure Practice 2

Three local FastAPI services connecting to Azure SQL (MS SQL Server).

## Structure
```
art-supply-store/
├── order-service/
├── product-service/
└── return-service/
```

## Setup (each service)
```bash
cd <service-folder>
pip install -r requirements.txt
cp .env.example .env        # paste your connection string
uvicorn main:app --reload --port <port>
```

| Service         | Port |
|-----------------|------|
| order-service   | 8001 |
| product-service | 8002 |
| return-service  | 8003 |

## Swagger UI
- http://localhost:8001/docs
- http://localhost:8002/docs
- http://localhost:8003/docs
