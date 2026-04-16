#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════
#   Smart Retail Analytics Platform – Hadoop Ecosystem Setup Script
#   Installs: Java 8, Hadoop 3.3.6, Pig 0.17, Hive 3.1.3,
#             HBase 2.5.6, ZooKeeper 3.8.3
#   OS: Ubuntu 20.04 / 22.04 LTS (also works on Debian-based systems)
# ═══════════════════════════════════════════════════════════════════════

set -e   # Exit on any error

# ─────────────────── Colour Helpers ────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
section() { echo -e "\n${GREEN}═══════════════════════════════════════════════${NC}"; \
            echo -e "${GREEN}  $*${NC}"; \
            echo -e "${GREEN}═══════════════════════════════════════════════${NC}\n"; }

HADOOP_VERSION="3.3.6"
PIG_VERSION="0.17.0"
HIVE_VERSION="3.1.3"
HBASE_VERSION="2.5.6"
ZK_VERSION="3.8.3"
INSTALL_DIR="/opt/bigdata"

# ─────────────────── Step 1: Prerequisites ───────────────────
section "Step 1: Installing Prerequisites"
sudo apt-get update -y
sudo apt-get install -y \
    openjdk-8-jdk \
    ssh \
    rsync \
    wget \
    curl \
    python3 \
    python3-pip \
    net-tools \
    vim \
    unzip

info "Java version:"
java -version 2>&1

# Set JAVA_HOME globally
JAVA_HOME_PATH=$(readlink -f /usr/bin/java | sed 's:/bin/java::')
if ! grep -q "JAVA_HOME" /etc/environment 2>/dev/null; then
    echo "JAVA_HOME=$JAVA_HOME_PATH" | sudo tee -a /etc/environment
fi

# ─────────────────── Step 2: SSH Passwordless Setup ──────────
section "Step 2: Configuring Passwordless SSH (localhost)"
if [ ! -f ~/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
fi
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null
ssh-keyscan -H 0.0.0.0   >> ~/.ssh/known_hosts 2>/dev/null
info "SSH configured ✓"

# ─────────────────── Step 3: Create Install Directory ────────
sudo mkdir -p $INSTALL_DIR
sudo chown $USER:$USER $INSTALL_DIR

# ─────────────────── Step 4: Hadoop ──────────────────────────
section "Step 3: Installing Hadoop $HADOOP_VERSION"
HADOOP_TAR="hadoop-${HADOOP_VERSION}.tar.gz"
HADOOP_URL="https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_TAR}"

if [ ! -d "$INSTALL_DIR/hadoop" ]; then
    wget -q --show-progress -O /tmp/${HADOOP_TAR} ${HADOOP_URL}
    tar -xzf /tmp/${HADOOP_TAR} -C $INSTALL_DIR
    mv $INSTALL_DIR/hadoop-${HADOOP_VERSION} $INSTALL_DIR/hadoop
    info "Hadoop extracted to $INSTALL_DIR/hadoop"
else
    warn "Hadoop already installed – skipping"
fi

# ─────────────────── Step 5: Apache Pig ──────────────────────
section "Step 4: Installing Apache Pig $PIG_VERSION"
PIG_TAR="pig-${PIG_VERSION}.tar.gz"
if [ ! -d "$INSTALL_DIR/pig" ]; then
    wget -q --show-progress \
        -O /tmp/${PIG_TAR} \
        "https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/${PIG_TAR}"
    tar -xzf /tmp/${PIG_TAR} -C $INSTALL_DIR
    mv $INSTALL_DIR/pig-${PIG_VERSION} $INSTALL_DIR/pig
    info "Pig extracted ✓"
else
    warn "Pig already installed – skipping"
fi

# ─────────────────── Step 6: Apache Hive ─────────────────────
section "Step 5: Installing Apache Hive $HIVE_VERSION"
HIVE_TAR="apache-hive-${HIVE_VERSION}-bin.tar.gz"
if [ ! -d "$INSTALL_DIR/hive" ]; then
    wget -q --show-progress \
        -O /tmp/${HIVE_TAR} \
        "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/${HIVE_TAR}"
    tar -xzf /tmp/${HIVE_TAR} -C $INSTALL_DIR
    mv $INSTALL_DIR/apache-hive-${HIVE_VERSION}-bin $INSTALL_DIR/hive
    info "Hive extracted ✓"
else
    warn "Hive already installed – skipping"
fi

# ─────────────────── Step 7: ZooKeeper ───────────────────────
section "Step 6: Installing ZooKeeper $ZK_VERSION"
ZK_TAR="apache-zookeeper-${ZK_VERSION}-bin.tar.gz"
if [ ! -d "$INSTALL_DIR/zookeeper" ]; then
    wget -q --show-progress \
        -O /tmp/${ZK_TAR} \
        "https://archive.apache.org/dist/zookeeper/zookeeper-${ZK_VERSION}/${ZK_TAR}"
    tar -xzf /tmp/${ZK_TAR} -C $INSTALL_DIR
    mv $INSTALL_DIR/apache-zookeeper-${ZK_VERSION}-bin $INSTALL_DIR/zookeeper
    cp $INSTALL_DIR/zookeeper/conf/zoo_sample.cfg \
       $INSTALL_DIR/zookeeper/conf/zoo.cfg
    info "ZooKeeper extracted ✓"
else
    warn "ZooKeeper already installed – skipping"
fi

# ─────────────────── Step 8: HBase ───────────────────────────
section "Step 7: Installing HBase $HBASE_VERSION"
HBASE_TAR="hbase-${HBASE_VERSION}-bin.tar.gz"
if [ ! -d "$INSTALL_DIR/hbase" ]; then
    wget -q --show-progress \
        -O /tmp/${HBASE_TAR} \
        "https://archive.apache.org/dist/hbase/${HBASE_VERSION}/${HBASE_TAR}"
    tar -xzf /tmp/${HBASE_TAR} -C $INSTALL_DIR
    mv $INSTALL_DIR/hbase-${HBASE_VERSION} $INSTALL_DIR/hbase
    info "HBase extracted ✓"
else
    warn "HBase already installed – skipping"
fi

# ─────────────────── Step 9: Environment Variables ───────────
section "Step 8: Configuring Environment Variables"

ENV_BLOCK="
# ─── Big Data Platform ───────────────────────────────
export JAVA_HOME=${JAVA_HOME_PATH}
export HADOOP_HOME=${INSTALL_DIR}/hadoop
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PIG_HOME=${INSTALL_DIR}/pig
export HIVE_HOME=${INSTALL_DIR}/hive
export HBASE_HOME=${INSTALL_DIR}/hbase
export ZOOKEEPER_HOME=${INSTALL_DIR}/zookeeper

export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
export PATH=\$PATH:\$PIG_HOME/bin:\$HIVE_HOME/bin:\$HBASE_HOME/bin:\$ZOOKEEPER_HOME/bin

export HADOOP_MAPRED_HOME=\$HADOOP_HOME
export HADOOP_COMMON_HOME=\$HADOOP_HOME
export HADOOP_HDFS_HOME=\$HADOOP_HOME
export YARN_HOME=\$HADOOP_HOME
# ─────────────────────────────────────────────────────
"

if ! grep -q "Big Data Platform" ~/.bashrc; then
    echo "$ENV_BLOCK" >> ~/.bashrc
    info "Environment variables added to ~/.bashrc"
fi

# Source for current shell
eval "$ENV_BLOCK"

# ─────────────────── Step 10: Hadoop Config Files ────────────
section "Step 9: Configuring Hadoop (core-site, hdfs-site, mapred-site, yarn-site)"

HADOOP_ETC=$INSTALL_DIR/hadoop/etc/hadoop

# Set JAVA_HOME inside Hadoop's env script
sed -i "s|# export JAVA_HOME=.*|export JAVA_HOME=${JAVA_HOME_PATH}|" \
    $HADOOP_ETC/hadoop-env.sh

# core-site.xml
cat > $HADOOP_ETC/core-site.xml <<'XMLEOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
    <description>NameNode URI</description>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/tmp/hadoop-${user.name}</value>
  </property>
</configuration>
XMLEOF

# hdfs-site.xml
cat > $HADOOP_ETC/hdfs-site.xml <<'XMLEOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
    <description>Single-node: 1 replica</description>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///opt/bigdata/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///opt/bigdata/hdfs/datanode</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
XMLEOF

# mapred-site.xml
cat > $HADOOP_ETC/mapred-site.xml <<'XMLEOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
</configuration>
XMLEOF

# yarn-site.xml
cat > $HADOOP_ETC/yarn-site.xml <<'XMLEOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
  </property>
</configuration>
XMLEOF

# ─────────────────── Step 11: HBase Config ───────────────────
HBASE_CONF=$INSTALL_DIR/hbase/conf

cat > $HBASE_CONF/hbase-site.xml <<'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hbase.rootdir</name>
    <value>hdfs://localhost:9000/hbase</value>
  </property>
  <property>
    <name>hbase.cluster.distributed</name>
    <value>false</value>
  </property>
  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>localhost</value>
  </property>
  <property>
    <name>hbase.zookeeper.property.dataDir</name>
    <value>/opt/bigdata/zookeeper/data</value>
  </property>
  <property>
    <name>hbase.unsafe.stream.capability.enforce</name>
    <value>false</value>
  </property>
</configuration>
XMLEOF

sed -i "s|# export JAVA_HOME.*|export JAVA_HOME=${JAVA_HOME_PATH}|" \
    $HBASE_CONF/hbase-env.sh

# ─────────────────── Step 12: Format NameNode ────────────────
section "Step 10: Formatting HDFS NameNode"
mkdir -p /opt/bigdata/hdfs/namenode /opt/bigdata/hdfs/datanode /opt/bigdata/zookeeper/data
$INSTALL_DIR/hadoop/bin/hdfs namenode -format -force

# ─────────────────── Step 13: Python Deps ────────────────────
section "Step 11: Installing Python Analytics Libraries"
pip3 install --quiet pandas matplotlib seaborn plotly happybase

# ─────────────────── Done ────────────────────────────────────
section "✅ Installation Complete!"
echo -e "Run: ${YELLOW}source ~/.bashrc${NC}  to load environment variables"
echo -e "Then: ${YELLOW}bash setup/start_all_services.sh${NC}  to start all services"
