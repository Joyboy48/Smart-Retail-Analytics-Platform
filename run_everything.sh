#!/bin/bash

GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}====================================================${NC}"
echo -e "${GREEN}🚀 STARTING SMART RETAIL ANALYTICS MASTER PIPELINE 🚀${NC}"
echo -e "${BLUE}====================================================${NC}"

# Phase 1: Start Services
echo -e "\n${GREEN}📦 Phase 1: Starting Big Data Services (Hadoop, HBase, ZooKeeper)...${NC}"
bash setup/start_all_services.sh start
sleep 5 # Give cluster a moment to warm up

# Phase 2: HDFS Operations
echo -e "\n${GREEN}📤 Phase 2: Uploading Dataset to HDFS...${NC}"
bash hdfs_ops/hdfs_upload.sh

# Phase 3: Pig ETL
echo -e "\n${GREEN}🐷 Phase 3: Running Apache Pig ETL MapReduce Tasks...${NC}"
bash pig_scripts/run_pig_etl.sh

# Phase 4: Hive Analytics
echo -e "\n${GREEN}🐝 Phase 4: Executing Apache Hive Data Warehouse Queries...${NC}"
hive -f hive_scripts/01_create_tables.hql
hive -f hive_scripts/02_analytics_queries.hql

# Phase 5: HBase Python Operations (Wait for Thrift)
echo -e "\n${GREEN}🛢️ Phase 5: Pushing data to HBase & fetching real-time stats...${NC}"
# Depending on how thrift starts, if it's already a service we just run operations:
python3 hbase_scripts/hbase_operations.py || echo "Warning: HBase Python operations encountered an error, continuing to charts..."

# Phase 6: Generate Visualizations
echo -e "\n${GREEN}🐍 Phase 6: Generating Python Charts for Dashboard...${NC}"
python3 python_viz/generate_charts.py

# Phase 7: Launch Dashboard
echo -e "\n${GREEN}🌐 Phase 7: Launching UI Dashboard...${NC}"
echo "Serving Dashboard on http://localhost:8000/dashboard/"
python3 -m http.server 8000 &
SERVER_PID=$!

sleep 2
xdg-open "http://localhost:8000/dashboard/index.html" || \
  echo -e "\n${BLUE}Please open your browser and manually visit: http://localhost:8000/dashboard/index.html${NC}"

echo -e "\n${GREEN}✅ PIPELINE COMPLETE!${NC}"
echo -e "Press [CTRL+C] in this terminal when you want to stop the UI dashboard server."
wait $SERVER_PID
