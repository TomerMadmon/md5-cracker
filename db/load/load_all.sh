#!/bin/bash
# Load all CSV files from a directory
# Usage: ./load_all.sh <directory> [db_name] [db_user]

DIR=$1
DB_NAME=${2:-md5db}
DB_USER=${3:-md5}

if [ -z "$DIR" ]; then
    echo "Usage: ./load_all.sh <directory> [db_name] [db_user]"
    exit 1
fi

echo "Loading all CSV files from $DIR..."

for file in "$DIR"/*.csv; do
    if [ -f "$file" ]; then
        echo "Processing $file..."
        ./load_data.sh "$file" "$DB_NAME" "$DB_USER"
    fi
done

echo "All files loaded. Running ANALYZE..."
psql -d "$DB_NAME" -U "$DB_USER" -c "ANALYZE md5_phone_map_bin;"

echo "Done!"

