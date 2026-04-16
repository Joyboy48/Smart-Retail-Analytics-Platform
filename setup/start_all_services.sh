#!/bin/bash
# ═══════════════════════════════════════════════════════════════
#   Start / Stop All Big Data Services
# ═══════════════════════════════════════════════════════════════

INSTALL_DIR="/opt/bigdata"
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

start_all() {
    echo -e "${GREEN}[START] Starting all services...${NC}"

    # 1. Start HDFS (NameNode + DataNode)
    echo -e "${YELLOW}→ Starting HDFS...${NC}"
    $INSTALL_DIR/hadoop/sbin/start-dfs.sh

    # 2. Start YARN (ResourceManager + NodeManager)
    echo -e "${YELLOW}→ Starting YARN...${NC}"
    $INSTALL_DIR/hadoop/sbin/start-yarn.sh

    # 3. Start ZooKeeper
    echo -e "${YELLOW}→ Starting ZooKeeper...${NC}"
    $INSTALL_DIR/zookeeper/bin/zkServer.sh start

    # 4. Start HBase (uses its own ZooKeeper unless external)
    echo -e "${YELLOW}→ Starting HBase...${NC}"
    $INSTALL_DIR/hbase/bin/start-hbase.sh

    echo ""
    echo -e "${GREEN}✅ All services started! Verification:${NC}"
    $INSTALL_DIR/hadoop/bin/hdfs dfsadmin -report 2>/dev/null | head -20
    jps
}

stop_all() {
    echo -e "${RED}[STOP] Stopping all services...${NC}"
    $INSTALL_DIR/hbase/bin/stop-hbase.sh
    $INSTALL_DIR/zookeeper/bin/zkServer.sh stop
    $INSTALL_DIR/hadoop/sbin/stop-yarn.sh
    $INSTALL_DIR/hadoop/sbin/stop-dfs.sh
    echo -e "${GREEN}✅ All services stopped.${NC}"
}

status_all() {
    echo -e "${GREEN}[STATUS] Running Java processes:${NC}"
    jps
    echo ""
    echo -e "${GREEN}[STATUS] ZooKeeper:${NC}"
    $INSTALL_DIR/zookeeper/bin/zkServer.sh status 2>/dev/null || echo "ZooKeeper not running"
    echo ""
    echo -e "${GREEN}[STATUS] HDFS:${NC}"
    $INSTALL_DIR/hadoop/bin/hdfs dfsadmin -report 2>/dev/null | head -15 || echo "HDFS not running"
}

case "$1" in
    start)  start_all  ;;
    stop)   stop_all   ;;
    status) status_all ;;
    restart) stop_all; sleep 3; start_all ;;
    *)
        echo "Usage: $0 {start|stop|status|restart}"
        echo ""
        echo "Expected JVM processes after start:"
        echo "  NameNode, DataNode, SecondaryNameNode"
        echo "  ResourceManager, NodeManager"
        echo "  QuorumPeerMain (ZooKeeper)"
        echo "  HMaster (HBase)"
        ;;
esac
