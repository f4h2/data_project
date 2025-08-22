#!/bin/bash
set -e

# Kiểm tra và tạo database airflow nếu chưa tồn tại
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "\l" | grep -q airflow || \
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
EOSQL

# Kiểm tra và tạo database warehouse nếu chưa tồn tại
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -c "\l" | grep -q warehouse || \
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE warehouse;
    GRANT ALL PRIVILEGES ON DATABASE warehouse TO $POSTGRES_USER;
EOSQL

# Tạo bảng hot_products_stream trong database warehouse
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname warehouse <<-EOSQL
    CREATE TABLE IF NOT EXISTS hot_products_stream (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        product_id INTEGER,
        click_count BIGINT,
        unique_users BIGINT,
        PRIMARY KEY (window_start, window_end, product_id)
    );

    CREATE TABLE IF NOT EXISTS aggregated_clicks (
        product_id INTEGER PRIMARY KEY,
        click_count BIGINT,
        unique_users BIGINT,
        price DOUBLE PRECISION,
        potential_revenue DOUBLE PRECISION
    );
EOSQL