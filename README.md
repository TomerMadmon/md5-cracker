# MD5 Hash Cracker System

A distributed system for looking up phone numbers from MD5 hashes using Angular UI, Spring Boot (Master/Minion), RabbitMQ, and PostgreSQL.

## Solution Explanation

### Overview

This system solves the problem of efficiently looking up phone numbers from MD5 hashes at scale. The solution uses a **distributed architecture** to handle large-scale hash lookups by:

1. **Precomputing MD5 hashes**: All possible phone number MD5 hashes are precomputed and stored in PostgreSQL
2. **Distributed processing**: Work is distributed across multiple worker nodes (minions) using RabbitMQ
3. **Batch processing**: Large input files are split into manageable batches for parallel processing
4. **Real-time monitoring**: Progress updates are streamed via Server-Sent Events (SSE)

### How It Works

#### Data Flow

```
1. User uploads file with MD5 hashes
   ↓
2. Master service validates and splits into batches (1000 hashes each)
   ↓
3. Batches published to RabbitMQ queue
   ↓
4. Minion workers consume batches from queue
   ↓
5. Each minion performs database lookup (batch IN query)
   ↓
6. Found results stored in PostgreSQL results table
   ↓
7. Result batches sent back to Master via RabbitMQ
   ↓
8. Master aggregates results and updates job progress
   ↓
9. User receives real-time progress updates via SSE
   ↓
10. User downloads final results CSV
```

#### Key Design Decisions

1. **Precomputed Hash Table**: Instead of computing MD5 hashes on-the-fly, we precompute all possible phone number MD5 hashes and store them in a PostgreSQL table. This allows for O(1) lookup time per hash.

2. **Binary Storage**: MD5 hashes are stored as `BYTEA` (16 bytes) instead of hex strings (32 characters), saving ~50% storage space and improving query performance.

3. **Batch Processing**: Hashes are processed in batches of 1000 to:
   - Reduce database round-trips
   - Enable parallel processing across multiple minions
   - Improve throughput

4. **Message Queue**: RabbitMQ ensures:
   - Reliable delivery (messages persist if minion crashes)
   - Load balancing across minions
   - Horizontal scalability (add more minions as needed)

5. **Asynchronous Processing**: Jobs are processed asynchronously, allowing the system to handle large files without blocking the API.

6. **Idempotency**: All database writes use `ON CONFLICT DO NOTHING` to handle duplicate messages safely.

### Architecture

- **Angular UI**: Upload MD5 files and monitor job progress with real-time updates (SSE)
- **Spring Boot Master**: Accepts file uploads, splits into batches, publishes to RabbitMQ, aggregates results
- **Spring Boot Minion**: Consumes batches from RabbitMQ, performs database lookups, publishes results
- **RabbitMQ**: Message queue for distributing work batches
- **PostgreSQL**: Stores precomputed MD5 hash map (100M+ rows) and job results

## Prerequisites

- Docker & Docker Compose
- Java 21 (for local development)
- Node 18+ / Angular CLI (for UI development)
- PostgreSQL client (psql) for data loading

## Quick Start

### 1. Start All Services

```bash
docker compose up --build
```

This will start:
- PostgreSQL on port 5432
- Adminer (Database Admin UI) on port 8081
- RabbitMQ on port 5672 (Management UI on 15672)
- Master service on port 8080
- Minion service (2 replicas)
- UI on port 4200

### 2. Load Precomputed Data (One-Time)

#### Generate Precomputed CSV Files

```bash
cd db/load

# Generate 10M rows for prefix 050 (single file)
python3 generate_precomp.py --prefix 050 --start 0 --count 10000000 --out precomp_050.csv

# Generate in parallel (recommended for large datasets)
python3 generate_precomp.py --prefix 050 --parallel 10 --count 10000000 --out-dir ./precomp_data

# Generate for multiple prefixes
for prefix in 050 051 052 053 054; do
  python3 generate_precomp.py --prefix $prefix --parallel 10 --count 10000000 --out-dir ./precomp_data
done
```

#### Load Data into PostgreSQL

```bash
# Make scripts executable (Linux/Mac)
chmod +x db/load/*.sh

# Load a single file
./db/load/load_data.sh precomp_050.csv md5db md5

# Load all files from a directory
./db/load/load_all.sh ./precomp_data md5db md5

# Or manually using psql
psql -d md5db -U md5 -c "\COPY staging_md5(md5_hex, phone_number) FROM 'precomp_050.csv' WITH CSV"
psql -d md5db -U md5 -c "INSERT INTO md5_phone_map_bin (md5_hash, phone_number) SELECT decode(md5_hex, 'hex'), phone_number FROM staging_md5 ON CONFLICT DO NOTHING;"
psql -d md5db -U md5 -c "TRUNCATE staging_md5;"
psql -d md5db -U md5 -c "ANALYZE md5_phone_map_bin;"
```

### 3. Use the System

1. **Access UI**: Open http://localhost:4200
2. **Access Database Admin (Adminer)**: Open http://localhost:8081
   - System: PostgreSQL
   - Server: postgres
   - Username: md5
   - Password: md5pass
   - Database: md5db
3. **Access RabbitMQ Management**: Open http://localhost:15672 (user: md5, pass: md5pass)
4. **Upload File**: Upload a text file with MD5 hashes (one per line, 32 hex characters)
5. **Monitor Progress**: View real-time progress with SSE updates
6. **Download Results**: When complete, download the CSV with results

### 4. API Usage

The Master service provides a REST API for job management. All endpoints are prefixed with `/api/jobs`.

#### 4.1. Upload Hash File (Create Job)

**Endpoint**: `POST /api/jobs`  
**Content-Type**: `multipart/form-data`

Upload a text file containing MD5 hashes (one per line, 32 hex characters).

**Request Example**:
```bash
curl -X POST \
  -F "file=@test_hashes.txt" \
  http://localhost:8080/api/jobs
```

**Request with verbose output**:
```bash
curl -v -X POST \
  -F "file=@test_hashes.txt" \
  http://localhost:8080/api/jobs
```

**Example File Content** (`test_hashes.txt`):
```
d41d8cd98f00b204e9800998ecf8427e
5d41402abc4b2a76b9719d911017c592
098f6bcd4621d373cade4e832627b4f6
a1b2c3d4e5f6789012345678901234ab
```

**Response** (HTTP 202 Accepted):
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Error Response** (HTTP 400 Bad Request):
```json
{
  "error": "Invalid file format",
  "message": "File must contain valid MD5 hashes (32 hex characters per line)"
}
```

#### 4.2. Get Job Status

**Endpoint**: `GET /api/jobs/{jobId}`

Retrieve the current status and progress of a job.

**Request Example**:
```bash
curl http://localhost:8080/api/jobs/550e8400-e29b-41d4-a716-446655440000
```

**Response** (HTTP 200 OK):
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "RUNNING",
  "totalHashes": 4,
  "batchesExpected": 1,
  "batchesCompleted": 0,
  "foundCount": 0,
  "createdAt": "2024-01-15T10:30:00Z"
}
```

**Status Values**:
- `RUNNING`: Job is being processed
- `COMPLETED`: All batches have been processed
- `FAILED`: Job encountered an error (rare)

**Response for Completed Job**:
```json
{
  "jobId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "COMPLETED",
  "totalHashes": 4,
  "batchesExpected": 1,
  "batchesCompleted": 1,
  "foundCount": 2,
  "createdAt": "2024-01-15T10:30:00Z"
}
```

#### 4.3. Stream Progress Events (SSE)

**Endpoint**: `GET /api/jobs/{jobId}/events`  
**Content-Type**: `text/event-stream`

Stream real-time progress updates using Server-Sent Events (SSE). The connection remains open until the job completes.

**Request Example**:
```bash
curl -N http://localhost:8080/api/jobs/550e8400-e29b-41d4-a716-446655440000/events
```

**Response Stream**:
```
event: progress
data: {"batchesCompleted":1,"batchesExpected":1,"foundCount":2}

event: completed
data: {"jobId":"550e8400-e29b-41d4-a716-446655440000"}

```

**Event Types**:
- `job_created`: Job was created (emitted immediately after upload)
- `progress`: Progress update with current batch completion and found count
- `completed`: Job has finished processing all batches

**Using in JavaScript** (for reference):
```javascript
const eventSource = new EventSource(`http://localhost:8080/api/jobs/${jobId}/events`);
eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Progress:', data);
};
eventSource.addEventListener('completed', () => {
  eventSource.close();
});
```

#### 4.4. Download Results

**Endpoint**: `GET /api/jobs/{jobId}/results`  
**Content-Type**: `text/csv`

Download the complete results as a CSV file. Each row contains the original hash and the corresponding phone number (or "NOT FOUND").

**Request Example**:
```bash
curl http://localhost:8080/api/jobs/550e8400-e29b-41d4-a716-446655440000/results \
  -o results.csv
```

**Request with headers shown**:
```bash
curl -v http://localhost:8080/api/jobs/550e8400-e29b-41d4-a716-446655440000/results \
  -o results.csv
```

**Response** (HTTP 200 OK):
```
hash,phone
d41d8cd98f00b204e9800998ecf8427e,NOT FOUND
5d41402abc4b2a76b9719d911017c592,0501234567
098f6bcd4621d373cade4e832627b4f6,NOT FOUND
a1b2c3d4e5f6789012345678901234ab,0519876543
```

**CSV Format**:
- Header row: `hash,phone`
- Each row: `<32-char-hex-hash>,<11-digit-phone-or-NOT-FOUND>`
- Results are ordered by hash (alphabetically)

**Error Response** (HTTP 404 Not Found):
```json
{
  "error": "Job not found",
  "message": "Job ID 550e8400-e29b-41d4-a716-446655440000 does not exist"
}
```

#### 4.5. List All Jobs (Optional)

**Endpoint**: `GET /api/jobs`

Currently returns an empty list. Can be extended for job listing functionality.

**Request Example**:
```bash
curl http://localhost:8080/api/jobs
```

**Response**:
```json
[]
```

#### Complete Workflow Example

Here's a complete example of using the API to process a hash file:

```bash
# 1. Create a test file
cat > test_hashes.txt << EOF
d41d8cd98f00b204e9800998ecf8427e
5d41402abc4b2a76b9719d911017c592
098f6bcd4621d373cade4e832627b4f6
EOF

# 2. Upload the file and capture job ID
JOB_ID=$(curl -s -X POST -F "file=@test_hashes.txt" \
  http://localhost:8080/api/jobs | jq -r '.jobId')

echo "Job ID: $JOB_ID"

# 3. Monitor progress (in another terminal or background)
curl -N http://localhost:8080/api/jobs/$JOB_ID/events &

# 4. Poll job status until complete
while true; do
  STATUS=$(curl -s http://localhost:8080/api/jobs/$JOB_ID | jq -r '.status')
  echo "Status: $STATUS"
  
  if [ "$STATUS" = "COMPLETED" ]; then
    break
  fi
  
  sleep 2
done

# 5. Download results
curl http://localhost:8080/api/jobs/$JOB_ID/results -o results.csv

# 6. View results
cat results.csv
```

#### API Response Times

- **Upload**: Typically < 1 second for files up to 10MB
- **Status Check**: < 100ms
- **Results Download**: Depends on result size (typically < 1 second for < 10MB CSV)
- **SSE Events**: Real-time (emitted as batches complete)

#### Error Handling

All endpoints return appropriate HTTP status codes:
- `200 OK`: Successful request
- `202 Accepted`: Job created (upload endpoint)
- `400 Bad Request`: Invalid input (e.g., malformed file)
- `404 Not Found`: Job ID doesn't exist
- `500 Internal Server Error`: Server error (check logs)

## Project Structure

```
md5-hash/
├── docker-compose.yml          # Full stack orchestration
├── db/
│   ├── ddl/
│   │   └── create_tables.sql   # Database schema
│   └── load/
│       ├── generate_precomp.py # Generate precomputed CSV files
│       ├── load_data.sh        # Load single CSV file
│       └── load_all.sh         # Load all CSVs from directory
├── master/                      # Spring Boot master service
│   ├── src/main/java/...
│   ├── Dockerfile
│   └── pom.xml
├── minion/                      # Spring Boot minion service
│   ├── src/main/java/...
│   ├── Dockerfile
│   └── pom.xml
├── ui/                          # Angular application
│   ├── src/
│   ├── Dockerfile
│   └── package.json
└── README.md
```

## Database Schema

### Main Tables

- **md5_phone_map_bin**: Precomputed MD5 hashes (BYTEA) → phone numbers
- **jobs**: Job tracking (status, progress, counts)
- **targets**: Original input hashes per job
- **results**: Lookup results (found phone numbers)

See `db/ddl/create_tables.sql` for complete schema.

## Configuration

### Environment Variables

#### Master Service
- `SPRING_DATASOURCE_URL`: PostgreSQL connection URL
- `SPRING_DATASOURCE_USERNAME`: Database username
- `SPRING_DATASOURCE_PASSWORD`: Database password
- `SPRING_RABBITMQ_HOST`: RabbitMQ host
- `SPRING_RABBITMQ_PORT`: RabbitMQ port (default: 5672)
- `SPRING_RABBITMQ_USERNAME`: RabbitMQ username
- `SPRING_RABBITMQ_PASSWORD`: RabbitMQ password

#### Minion Service
- Same as master, plus:
- `SPRING_RABBITMQ_LISTENER_SIMPLE_CONCURRENCY`: Number of concurrent consumers (default: 4)

### Batch Size

Default batch size is 1000 hashes per batch. Adjust in `master/src/main/java/com/md5cracker/service/JobService.java`:

```java
private static final int BATCH_SIZE = 1000; // Change as needed
```

## Performance Tuning

### PostgreSQL

Edit `postgresql.conf` or use environment variables:

```sql
shared_buffers = 4GB              # 25% of RAM
effective_cache_size = 12GB       # 70-80% of RAM
random_page_cost = 1.1            # For SSD
work_mem = 64MB                   # Per operation
max_connections = 100
```

### RabbitMQ

- Increase prefetch for faster processing: `spring.rabbitmq.listener.simple.prefetch=50`
- Adjust concurrency: `spring.rabbitmq.listener.simple.concurrency=8`

### Minion Scaling

Scale minions in docker-compose:

```bash
docker compose up --scale minion=4
```

Or edit `docker-compose.yml`:

```yaml
minion:
  deploy:
    replicas: 4
```

## Testing

### Small-Scale Test

1. Generate test data (10k rows):
```bash
python3 db/load/generate_precomp.py --prefix 050 --start 0 --count 10000 --out test_data.csv
./db/load/load_data.sh test_data.csv
```

2. Create test input file `test_hashes.txt`:
```
a1b2c3d4e5f6789012345678901234ab
05012345678  # (MD5 this, or use existing hash from test_data.csv)
```

3. Upload via UI or API and verify results

### Failure Testing

- Kill a minion during processing: RabbitMQ will requeue the message
- Results use `ON CONFLICT DO NOTHING` to handle duplicates safely

## Monitoring

### Health Checks

- Master: http://localhost:8080/actuator/health
- Minion: http://localhost:8081/actuator/health
- RabbitMQ: http://localhost:15672 (user: md5, pass: md5pass)

### Metrics

- Prometheus: http://localhost:8080/actuator/prometheus

### Database Monitoring

```sql
-- Check index usage
SELECT * FROM pg_stat_user_indexes WHERE tablename = 'md5_phone_map_bin';

-- Slow queries
SELECT * FROM pg_stat_statements ORDER BY total_time DESC LIMIT 10;

-- Table size
SELECT pg_size_pretty(pg_total_relation_size('md5_phone_map_bin'));
```

## Production Considerations

### Security

1. **API Authentication**: Add JWT or API key authentication to master endpoints
2. **RabbitMQ**: Use TLS, secure credentials, vhosts
3. **PostgreSQL**: Strong passwords, network restrictions, SSL
4. **Rate Limiting**: Add rate limiting on upload endpoint
5. **CORS**: Configure CORS properly for production domains

### Backup & Recovery

- Regular PostgreSQL backups (pg_dump)
- RabbitMQ queue persistence
- Consider job result archival

### High Availability

- PostgreSQL: Master-replica setup
- RabbitMQ: Cluster mode
- Load balancer for master/minion services
- Kubernetes deployment for better scaling

## Troubleshooting

### Minion not processing batches

- Check RabbitMQ queues: http://localhost:15672
- Verify database connection
- Check minion logs: `docker compose logs minion`

### Slow lookups

- Verify indexes exist: `\d md5_phone_map_bin` in psql
- Check PostgreSQL connection pool size
- Monitor database CPU/memory
- Consider increasing batch size (if DB can handle)

### SSE not working

- Check browser console for errors
- Verify master service is running
- Check nginx proxy configuration (if using)

## Development

### Local Development (without Docker)

#### Master/Minion

```bash
cd master
mvn spring-boot:run -Dspring-boot.run.profiles=master

cd ../minion
mvn spring-boot:run -Dspring-boot.run.profiles=minion
```

#### UI

```bash
cd ui
npm install
npm start  # Runs on http://localhost:4200
```

Note: Update `ui/src/app/api.service.ts` base URL to `http://localhost:8080/api` for local dev.

## License

MIT

## Support

For issues or questions, please check:
- Database connection issues → Verify PostgreSQL is running and credentials
- RabbitMQ connection issues → Check RabbitMQ management UI
- Build errors → Ensure Java 21 and Node 18+ are installed

