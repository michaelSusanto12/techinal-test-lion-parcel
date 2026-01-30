# Lion Parcel Data Engineering Assessment

## Quick Start (3 Steps)

```bash
# 1. Create .env file
cat > .env << EOF
OPENAI_API_KEY=your_api_key_here
OPENAI_API_BASE=https://api.apifree.ai/v1
OPENAI_MODEL=openai/gpt-5.2
EOF

# 2. Build & run all containers
docker compose up -d --build

# 3. Verify all services are running
docker ps
```

**Done!** Akses services:
- Airflow: http://localhost:8080 (admin/admin)
- Image API: http://localhost:8000/docs
- MinIO: http://localhost:9001 (minioadmin/minioadmin)

---

## Soal 1: ETL Pipeline

### Run ETL

1. Buka Airflow: http://localhost:8080
2. Login: `admin` / `admin`
3. Aktifkan DAG `retail_transactions_etl`
4. Trigger manual atau tunggu schedule

### Verifikasi Data

```bash
# Connect ke Data Warehouse
docker compose exec postgres psql -U postgres -d lionparcel_dwh -c "SELECT * FROM dwh_retail_transactions LIMIT 5;"
```

---

## Soal 2: Image Analysis API

### Upload & Test

```bash
# 1. Upload gambar ke MinIO
docker compose exec image-api python app/upload_to_minio.py

# 2. Test API
curl -X POST http://localhost:8000/analyze \
  -H "Content-Type: application/json" \
  -d '{"image_url": "http://minio:9000/lionparcel/Gambar1.jpg"}'

# 3. Batch process semua gambar
docker compose exec image-api python app/summarize_images.py
```

### Output
- Hasil tersimpan di `number_2/output/summary.csv`

---

## Services

| Service | Port | URL |
|---------|------|-----|
| Airflow | 8080 | http://localhost:8080 |
| Image API | 8000 | http://localhost:8000/docs |
| MinIO Console | 9001 | http://localhost:9001 |
| PostgreSQL | 5436 | localhost:5436 |

---

## Stop & Cleanup

```bash
# Stop all containers
docker compose down

# Stop and remove all data
docker compose down -v
```

---

## Troubleshooting

### Containers not starting
```bash
docker compose logs <service-name>
```

### Rebuild after changes
```bash
docker compose up -d --force-recreate --build
```
