#!/bin/bash
# Setup Airflow Variables for Domain Ingestion Pipeline
# Run this after starting the docker services

set -e

echo "🔧 Setting up Airflow Variables..."

# Wait for Airflow to be ready
echo "⏳ Waiting for Airflow webserver to be ready..."
sleep 5

# Set Kafka configuration
echo "📡 Setting Kafka configuration..."
docker exec airflow-webserver airflow variables set kafka_servers "kafka:29092" 2>/dev/null || true

# Set Spark configuration
echo "⚡ Setting Spark configuration..."
docker exec airflow-webserver airflow variables set spark_master "spark://spark-master:7077" 2>/dev/null || true

# Set Delta Lake paths
echo "💾 Setting Delta Lake paths..."
docker exec airflow-webserver airflow variables set bronze_path "/opt/data/delta/bronze" 2>/dev/null || true
docker exec airflow-webserver airflow variables set silver_path "/opt/data/delta/silver" 2>/dev/null || true
docker exec airflow-webserver airflow variables set gold_path "/opt/data/delta/gold" 2>/dev/null || true

echo ""
echo "✅ Airflow Variables configured successfully!"
echo ""
echo "📋 Current variables:"
docker exec airflow-webserver airflow variables list 2>/dev/null | grep -v "FutureWarning" | grep -v "WARNING"
echo ""
echo "🚀 You can now access Airflow at: http://localhost:8082"
echo "   Username: admin"
echo "   Password: admin"
