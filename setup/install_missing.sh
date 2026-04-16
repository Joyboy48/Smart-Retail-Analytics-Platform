#!/bin/bash
# ═══════════════════════════════════════════════════════════════════
#  Smart Retail Analytics Platform
#  install_missing.sh — Installs ONLY what is missing
#  Safe to re-run (skips already-installed components)
# ═══════════════════════════════════════════════════════════════════

set -e
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[✅ OK]${NC}    $*"; }
skip() { echo -e "${YELLOW}[SKIP]${NC}   $*"; }
info() { echo -e "${GREEN}[INFO]${NC}   $*"; }
err()  { echo -e "${RED}[ERROR]${NC}  $*"; }

HOME_DIR="$HOME"
HADOOP_VER="3.3.6"
PIG_VER="0.17.0"
HIVE_VER="3.1.3"
HBASE_VER="2.5.6"

# ─── Detect existing Hadoop ───────────────────────────────────────
HADOOP_HOME_EXISTING=""
if   [ -d "$HOME_DIR/hadoop-${HADOOP_VER}" ]; then
    HADOOP_HOME_EXISTING="$HOME_DIR/hadoop-${HADOOP_VER}"
elif [ -d "/opt/bigdata/hadoop" ]; then
    HADOOP_HOME_EXISTING="/opt/bigdata/hadoop"
fi

# ──────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║    Smart Retail Analytics — Dependency Installer         ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ─── 1. Java ──────────────────────────────────────────────────────
info "Checking Java…"
if java -version 2>&1 | grep -qE "version"; then
    JAVA_VER=$(java -version 2>&1 | head -1)
    ok "Java already installed: $JAVA_VER"
    # Warn if not Java 8 or 11 (Hadoop works best with these)
    if java -version 2>&1 | grep -qE '"21|22|23'; then
        echo -e "${YELLOW}  ⚠  Java 21 detected. Hadoop 3.3.6 works with Java 21 but"
        echo -e "     some older components prefer Java 8 or 11.${NC}"
        echo -e "${YELLOW}  ℹ  If you face issues, install: sudo apt install openjdk-11-jdk${NC}"
    fi
else
    info "Installing Java 11 (recommended for Hadoop 3.x)…"
    sudo apt-get update -y
    sudo apt-get install -y openjdk-11-jdk
    ok "Java 11 installed"
fi

JAVA_HOME_PATH=$(update-java-alternatives --list 2>/dev/null | awk '{print $3}' | head -1)
[ -z "$JAVA_HOME_PATH" ] && JAVA_HOME_PATH=$(readlink -f /usr/bin/java | sed 's:/bin/java::')
info "JAVA_HOME detected: $JAVA_HOME_PATH"

# ─── 2. SSH ───────────────────────────────────────────────────────
info "Checking SSH…"
if ssh -V 2>&1 | grep -q "OpenSSH"; then
    ok "SSH already installed"
else
    sudo apt-get install -y ssh rsync
    ok "SSH installed"
fi

# Setup passwordless SSH to localhost (required by Hadoop)
mkdir -p ~/.ssh
if [ ! -f ~/.ssh/id_rsa ]; then
    info "Creating SSH key pair…"
    ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa -q
fi
if ! grep -q "$(cat ~/.ssh/id_rsa.pub)" ~/.ssh/authorized_keys 2>/dev/null; then
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
    ok "Passwordless SSH configured"
else
    skip "Passwordless SSH already set up"
fi
ssh-keyscan -H localhost >> ~/.ssh/known_hosts 2>/dev/null
ssh-keyscan -H 0.0.0.0   >> ~/.ssh/known_hosts 2>/dev/null

# ─── 3. Hadoop ────────────────────────────────────────────────────
info "Checking Hadoop…"
if [ -n "$HADOOP_HOME_EXISTING" ]; then
    ok "Hadoop found at: $HADOOP_HOME_EXISTING"
    HADOOP_DIR="$HADOOP_HOME_EXISTING"
else
    info "Hadoop not found. Downloading Hadoop ${HADOOP_VER}…"
    HADOOP_TAR="hadoop-${HADOOP_VER}.tar.gz"
    HADOOP_URL="https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VER}/${HADOOP_TAR}"
    wget -q --show-progress -O "/tmp/${HADOOP_TAR}" "${HADOOP_URL}"
    tar -xzf "/tmp/${HADOOP_TAR}" -C "$HOME_DIR"
    HADOOP_DIR="$HOME_DIR/hadoop-${HADOOP_VER}"
    ok "Hadoop extracted to $HADOOP_DIR"
fi

# ─── 4. Apache Pig ────────────────────────────────────────────────
info "Checking Apache Pig…"
PIG_DIR="$HOME_DIR/pig-${PIG_VER}"
if [ -d "$PIG_DIR" ]; then
    ok "Pig found at: $PIG_DIR"
elif which pig &>/dev/null; then
    ok "Pig already in PATH: $(which pig)"
    PIG_DIR=$(dirname $(dirname $(which pig)))
else
    info "Pig not found. Downloading Apache Pig ${PIG_VER}…"
    PIG_TAR="pig-${PIG_VER}.tar.gz"
    wget -q --show-progress \
        -O "/tmp/${PIG_TAR}" \
        "https://archive.apache.org/dist/pig/pig-${PIG_VER}/${PIG_TAR}"
    tar -xzf "/tmp/${PIG_TAR}" -C "$HOME_DIR"
    ok "Pig extracted to $PIG_DIR"
fi

# ─── 5. Apache Hive ───────────────────────────────────────────────
info "Checking Apache Hive…"
HIVE_DIR="$HOME_DIR/apache-hive-${HIVE_VER}-bin"
if [ -d "$HIVE_DIR" ]; then
    ok "Hive found at: $HIVE_DIR"
elif which hive &>/dev/null; then
    ok "Hive already in PATH: $(which hive)"
    HIVE_DIR=$(dirname $(dirname $(which hive)))
else
    info "Hive not found. Downloading Apache Hive ${HIVE_VER}…"
    HIVE_TAR="apache-hive-${HIVE_VER}-bin.tar.gz"
    wget -q --show-progress \
        -O "/tmp/${HIVE_TAR}" \
        "https://archive.apache.org/dist/hive/hive-${HIVE_VER}/${HIVE_TAR}"
    tar -xzf "/tmp/${HIVE_TAR}" -C "$HOME_DIR"
    ok "Hive extracted to $HIVE_DIR"
fi

# ─── 6. HBase ─────────────────────────────────────────────────────
info "Checking HBase…"
HBASE_DIR="$HOME_DIR/hbase-${HBASE_VER}"
if [ -d "$HBASE_DIR" ]; then
    ok "HBase found at: $HBASE_DIR"
elif which hbase &>/dev/null; then
    ok "HBase already in PATH: $(which hbase)"
    HBASE_DIR=$(dirname $(dirname $(which hbase)))
else
    info "HBase not found. Downloading HBase ${HBASE_VER}…"
    HBASE_TAR="hbase-${HBASE_VER}-bin.tar.gz"
    wget -q --show-progress \
        -O "/tmp/${HBASE_TAR}" \
        "https://archive.apache.org/dist/hbase/${HBASE_VER}/${HBASE_TAR}"
    tar -xzf "/tmp/${HBASE_TAR}" -C "$HOME_DIR"
    ok "HBase extracted to $HBASE_DIR"
fi

# ─── 7. Python & Analytics Libraries ─────────────────────────────
info "Checking Python analytics libraries…"
if python3 -c "import pandas, matplotlib, seaborn, plotly, openpyxl" 2>/dev/null; then
    ok "All Python libraries (pandas, matplotlib, seaborn, plotly, openpyxl) installed"
else
    info "Installing missing Python libraries…"
    pip3 install --break-system-packages pandas matplotlib seaborn plotly openpyxl
    ok "Python libraries installed"
fi

# ─── 8. Write / Update ~/.bashrc ──────────────────────────────────
info "Updating ~/.bashrc with environment variables…"

{
  grep -q "SMART_RETAIL_ENV" ~/.bashrc
} && {
  skip "Environment already in ~/.bashrc"
} || {
cat >> ~/.bashrc << ENVBLOCK

# ─── Smart Retail Analytics Platform ────────────────── SMART_RETAIL_ENV ──
export JAVA_HOME=${JAVA_HOME_PATH}
export HADOOP_HOME=${HADOOP_DIR}
export HADOOP_CONF_DIR=\$HADOOP_HOME/etc/hadoop
export PIG_HOME=${PIG_DIR}
export HIVE_HOME=${HIVE_DIR}
export HBASE_HOME=${HBASE_DIR}

export PATH=\$PATH:\$JAVA_HOME/bin
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
export PATH=\$PATH:\$PIG_HOME/bin
export PATH=\$PATH:\$HIVE_HOME/bin
export PATH=\$PATH:\$HBASE_HOME/bin

export HADOOP_MAPRED_HOME=\$HADOOP_HOME
export HADOOP_COMMON_HOME=\$HADOOP_HOME
export HADOOP_HDFS_HOME=\$HADOOP_HOME
export YARN_HOME=\$HADOOP_HOME
# ────────────────────────────────────────────────────────────────────────────
ENVBLOCK
  ok "Environment variables written to ~/.bashrc"
}

# ─── 9. Configure Hadoop XML files ────────────────────────────────
info "Configuring Hadoop (core-site, hdfs-site, mapred-site, yarn-site)…"
HADOOP_ETC="${HADOOP_DIR}/etc/hadoop"

# Set JAVA_HOME in hadoop-env.sh
sed -i "s|# export JAVA_HOME=.*|export JAVA_HOME=${JAVA_HOME_PATH}|" \
    "$HADOOP_ETC/hadoop-env.sh" 2>/dev/null || true

# core-site.xml
cat > "$HADOOP_ETC/core-site.xml" << 'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
XMLEOF

# hdfs-site.xml
mkdir -p "$HOME_DIR/hdfs_data/namenode" "$HOME_DIR/hdfs_data/datanode"
cat > "$HADOOP_ETC/hdfs-site.xml" << XMLEOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://${HOME_DIR}/hdfs_data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://${HOME_DIR}/hdfs_data/datanode</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
XMLEOF

# mapred-site.xml
cat > "$HADOOP_ETC/mapred-site.xml" << 'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
XMLEOF

# yarn-site.xml
cat > "$HADOOP_ETC/yarn-site.xml" << 'XMLEOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
XMLEOF

ok "Hadoop configured"

# ─── 10. Configure HBase ──────────────────────────────────────────
info "Configuring HBase…"
HBASE_CONF="$HBASE_DIR/conf"
sed -i "s|# export JAVA_HOME.*|export JAVA_HOME=${JAVA_HOME_PATH}|" \
    "$HBASE_CONF/hbase-env.sh" 2>/dev/null || true

cat > "$HBASE_CONF/hbase-site.xml" << XMLEOF
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
    <name>hbase.unsafe.stream.capability.enforce</name>
    <value>false</value>
  </property>
</configuration>
XMLEOF
ok "HBase configured"

# ─── 11. Format HDFS NameNode (only once) ────────────────────────
NAMENODE_DIR="$HOME_DIR/hdfs_data/namenode"
if [ -z "$(ls -A $NAMENODE_DIR 2>/dev/null)" ]; then
    info "Formatting HDFS NameNode (first time)…"
    source ~/.bashrc 2>/dev/null || true
    export HADOOP_HOME="$HADOOP_DIR"
    $HADOOP_DIR/bin/hdfs namenode -format -force
    ok "NameNode formatted"
else
    skip "NameNode already formatted"
fi

# ─── Done ─────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                 SETUP COMPLETE ✅                        ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Next steps:                                             ║"
echo "║  1. source ~/.bashrc                                     ║"
echo "║  2. bash setup/start_services.sh start                   ║"
echo "║  3. bash hdfs_ops/hdfs_upload.sh                         ║"
echo "╚══════════════════════════════════════════════════════════╝"
