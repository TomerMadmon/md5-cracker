FROM python:3.11-slim

WORKDIR /app

# Install PostgreSQL client and psycopg2 dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir psycopg2-binary

# Copy scripts
COPY generate_precomp.py generate_all_prefixes.py load_all_to_db.py load_all_numbers.py ./

# Set as entrypoint
ENTRYPOINT ["python", "load_all_numbers.py"]

