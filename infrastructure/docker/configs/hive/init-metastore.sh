#!/bin/bash
set -e

echo "Installing dependencies..."
apt-get update -qq && apt-get install -y -qq wget postgresql-client

echo "Installing PostgreSQL JDBC driver..."
wget -q https://jdbc.postgresql.org/download/postgresql-42.5.1.jar -O /opt/hive/lib/postgresql-42.5.1.jar

echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD=hive psql -h postgres-metastore -U hive -d metastore -c '\q' 2>/dev/null; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done
echo "PostgreSQL is ready!"

# Create hive-site.xml
cat > /opt/hive/conf/hive-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://postgres-metastore:5432/metastore</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hive</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/data/warehouse</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://0.0.0.0:9083</value>
  </property>
</configuration>
EOF

echo "Checking if metastore schema exists..."
TABLE_COUNT=$(PGPASSWORD=hive psql -h postgres-metastore -U hive -d metastore -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name='DBS';" 2>/dev/null || echo "0")

if [ "$TABLE_COUNT" -eq "0" ] || [ -z "$TABLE_COUNT" ]; then
  echo "Initializing metastore schema..."
  /opt/hive/bin/schematool -dbType postgres -initSchema
else
  echo "Schema already exists, skipping initialization"
fi

echo "Starting Hive Metastore service..."
/opt/hive/bin/hive --service metastore
