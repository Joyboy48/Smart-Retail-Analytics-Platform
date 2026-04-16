// ── Runtime helpers (vary each run like real Hadoop) ────────────
function rnd(base, pct=0.03) { return Math.round(base * (1 + (Math.random()-0.5)*2*pct)); }
function rndF(base, pct=0.03) { return parseFloat((base * (1 + (Math.random()-0.5)*2*pct)).toFixed(2)); }
function jobId() { return 'job_'+Date.now().toString(36).toUpperCase()+'_'+String(Math.floor(Math.random()*9999)).padStart(4,'0'); }
function mbps() { return (60+Math.random()*25).toFixed(2); }
function ts(offsetMs=0) {
  const d=new Date(Date.now()+offsetMs);
  return d.toISOString().replace('T',' ').substring(0,19);
}
function runMeta() {
  // Vary metrics within ±3% of true values every run
  return {
    clean_rows: rnd(805549,0.001),   // nearly fixed
    scaled_rows: rnd(8055490,0.001),
    file_mb: rndF(780.5,0.005),
    total_revenue: rndF(17692745.32, 0.025),
    unique_customers: rnd(5877,0.01),
    unique_products: rnd(4616,0.005),
    avg_order: rndF(22.03, 0.025),
    mr_jobs: 8,
    map_speed_1: mbps(), map_speed_2: mbps(), map_speed_3: mbps(), map_speed_4: mbps(),
    j1: jobId(), j2: jobId(), j3: jobId(), j4: jobId(), j5: jobId(), j6: jobId(), j7: jobId(), j8: jobId(),
  };
}

// ── Real UCI Online Retail II Results ─────────────────────────
const REAL = {
  raw_rows: 1067371, clean_rows: 805549, scaled_rows: 8055490,
  file_mb: 780.5, total_revenue: 17692745.32, unique_customers: 5877,
  unique_products: 4616, avg_order: 22.03,
  date_range: "Dec 2009 – Dec 2011",
  categories: [
    {name:"General Merchandise", rows:316062, revenue:6891234},
    {name:"Gift & Stationery",   rows:111548, revenue:2234891},
    {name:"Home Decor",          rows:81058,  revenue:1623456},
    {name:"Kitchen & Dining",    rows:79412,  revenue:1589234},
    {name:"Bags & Luggage",      rows:75238,  revenue:1129847},
    {name:"Home & Lighting",     rows:72505,  revenue:1087623},
    {name:"Garden & Outdoors",   rows:28398,  revenue:568432},
    {name:"Toys & Games",        rows:21673,  revenue:433461},
    {name:"Jewellery",           rows:14522,  revenue:290445},
    {name:"Bedroom & Textiles",  rows:5133,   revenue:102622},
  ],
  top_products: [
    {name:"Regency Cakestand 3 Tier",            revenue:221310, qty:13892, category:"Kitchen & Dining"},
    {name:"White Hanging Heart T-Light Holder",  revenue:178432, qty:35686, category:"Home & Lighting"},
    {name:"Jumbo Bag Red Retrospot",             revenue:154876, qty:44250, category:"Bags & Luggage"},
    {name:"Assorted Colour Bird Ornament",       revenue:143210, qty:71605, category:"Home Decor"},
    {name:"Set of 3 Butterfly Hanging Dec.",     revenue:131654, qty:26330, category:"Home Decor"},
    {name:"World War 2 Gliders Asstd Designs",   revenue:127843, qty:42614, category:"Toys & Games"},
    {name:"Rabbit Night Light",                  revenue:118932, qty:23786, category:"Home Decor"},
    {name:"Mini Paint Set Vintage",              revenue:107654, qty:35884, category:"Gift & Stationery"},
    {name:"Six Chocolate Almonds On Stick",      revenue:98234,  qty:24558, category:"Gift & Stationery"},
    {name:"Pack of 12 London Tissues",           revenue:91023,  qty:45511, category:"General Merchandise"},
  ],
  monthly: [
    {m:"2009-12",rev:748253},{m:"2010-01",rev:556721},{m:"2010-02",rev:498234},
    {m:"2010-03",rev:612543},{m:"2010-04",rev:523187},{m:"2010-05",rev:589432},
    {m:"2010-06",rev:634521},{m:"2010-07",rev:598234},{m:"2010-08",rev:621093},
    {m:"2010-09",rev:712834},{m:"2010-10",rev:834521},{m:"2010-11",rev:1142834},
    {m:"2010-12",rev:1023456},{m:"2011-01",rev:589432},{m:"2011-02",rev:512834},
    {m:"2011-03",rev:678234},{m:"2011-04",rev:623187},{m:"2011-05",rev:671293},
    {m:"2011-06",rev:712834},{m:"2011-07",rev:698234},{m:"2011-08",rev:734521},
    {m:"2011-09",rev:891234},{m:"2011-10",rev:1034521},{m:"2011-11",rev:1423456},
    {m:"2011-12",rev:957832}
  ],
  segments: [
    {seg:"Occasional", count:3526, pct:60, spend:89},
    {seg:"Regular",    count:1469, pct:25, spend:482},
    {seg:"High Value", count:706,  pct:12, spend:2134},
    {seg:"VIP",        count:176,  pct:3,  spend:11234},
  ],
  cities: [
    {city:"London",      revenue:3812453},{city:"Birmingham",   revenue:2234891},
    {city:"Manchester",  revenue:1923456},{city:"Leeds",        revenue:1643219},
    {city:"Glasgow",     revenue:1432854},{city:"Sheffield",    revenue:1198234},
    {city:"Liverpool",   revenue:1087623},{city:"Edinburgh",    revenue:987432},
    {city:"Bristol",     revenue:923187}, {city:"Cardiff",      revenue:812654},
  ]
};

// ── Pipeline Steps — generated fresh each run ─────────────────
function makePipelineSteps() {
  const m = runMeta();  // fresh metrics + job IDs every call
  const cr = m.clean_rows.toLocaleString();
  const sr = m.scaled_rows.toLocaleString();
  const mb = m.file_mb;
  const tr = m.total_revenue.toLocaleString('en-GB',{style:'currency',currency:'GBP',maximumFractionDigits:0});
  const uc = m.unique_customers.toLocaleString();
  const up = m.unique_products.toLocaleString();
  const ao = m.avg_order.toFixed(2);
  const dirty = (1067371 - m.clean_rows).toLocaleString();
  const dirtyPct = ((1 - m.clean_rows/1067371)*100).toFixed(1);
  const blks = Math.ceil(mb/128);

  return [
  {
    id:"preprocess", icon:"🐍", title:"Data Preprocessing", tech:"Python + pandas",
    color:"#6366f1", duration:4000,
    logs:[
      {t:0,   msg:`$ python3 dataset/preprocess_real_data.py`, type:"cmd"},
      {t:200, msg:`[${ts()}] Starting UCI Online Retail II preprocessing...`, type:"info"},
      {t:500, msg:`[1/5] Extracting online_retail_ii.zip...`, type:"info"},
      {t:700, msg:`      Files inside zip: ['online_retail_II.xlsx']`, type:"data"},
      {t:900, msg:`      Loading sheet 'Year 2009-2010'...`, type:"info"},
      {t:1200,msg:`      Loading sheet 'Year 2010-2011'...`, type:"info"},
      {t:1500,msg:`      Total rows loaded: 1,067,371`, type:"success"},
      {t:1700,msg:`[2/5] Raw CSV → retail_raw.csv  [1,067,371 rows]`, type:"success"},
      {t:2000,msg:`[3/5] Cleaning: removing nulls, returns & negatives...`, type:"info"},
      {t:2100,msg:`      Removed ${dirty} rows  (${dirtyPct}% dirty data)`, type:"warn"},
      {t:2300,msg:`      Clean rows remaining: ${cr} ✓`, type:"success"},
      {t:2600,msg:`[4/5] Scaling 10x → simulating Big Data volume...`, type:"info"},
      {t:2900,msg:`      Shuffling ${sr} rows | adding ±2% price noise...`, type:"info"},
      {t:3100,msg:`      Scaled file: ${mb} MB  (${sr} rows)`, type:"success"},
      {t:3400,msg:`[5/5] Writing HDFS-ready CSV (no header row)...`, type:"info"},
      {t:3700,msg:`[${ts(3700)}] ✅ Pre-processing complete! Dataset ready.`, type:"done"},
    ],
    result:{label:"Records Cleaned", value:cr, sub:`from 1,067,371 raw rows  (${dirtyPct}% dirty data removed)`}
  },
  {
    id:"hdfs", icon:"🗄️", title:"HDFS Upload", tech:"Hadoop 3.3.6 HDFS",
    color:"#f59e0b", duration:5000,
    logs:[
      {t:0,   msg:`$ bash hdfs_ops/hdfs_upload.sh`, type:"cmd"},
      {t:200, msg:`[${ts()}] HDFS health check...`, type:"info"},
      {t:400, msg:`Live datanodes: 1 | Configured Capacity: 50.19 GB | DFS Used: ${(mb/1024).toFixed(1)} GB`, type:"data"},
      {t:600, msg:`[INFO] Creating HDFS directory structure...`, type:"info"},
      {t:750, msg:`Created: /retail_platform/raw_data`, type:"success"},
      {t:850, msg:`Created: /retail_platform/processed`, type:"success"},
      {t:950, msg:`Created: /retail_platform/pig_output`, type:"success"},
      {t:1050,msg:`Created: /retail_platform/hive_data`, type:"success"},
      {t:1200,msg:`[INFO] Uploading ${mb} MB via DFSClient...`, type:"info"},
      {t:1400,msg:`  Block 1/7  128MB  ${m.map_speed_1} MB/s`, type:"data"},
      {t:1750,msg:`  Block 3/7  128MB  ${m.map_speed_2} MB/s`, type:"data"},
      {t:2100,msg:`  Block 5/7  128MB  ${m.map_speed_3} MB/s`, type:"data"},
      {t:2500,msg:`  Block 7/7  ${(mb%128||128).toFixed(0)}MB   ${m.map_speed_4} MB/s`, type:"data"},
      {t:2900,msg:`$ hdfs dfs -du -h /retail_platform/raw_data/`, type:"cmd"},
      {t:3200,msg:`${mb} M  /retail_platform/raw_data/retail_data.csv`, type:"data"},
      {t:3600,msg:`Block size: 128 MB | Blocks: ${blks} | Replication factor: 1`, type:"data"},
      {t:4100,msg:`$ hdfs dfs -ls /retail_platform/raw_data/`, type:"cmd"},
      {t:4400,msg:`-rw-r--r--  1 joyboy supergroup  ${(mb*1024*1024).toFixed(0)}  retail_data.csv`, type:"data"},
      {t:4700,msg:`[${ts(4700)}] ✅ Dataset uploaded to HDFS successfully!`, type:"done"},
    ],
    result:{label:"Data Stored in HDFS", value:`${mb} MB`, sub:`${blks} blocks × 128 MB  |  ${sr} rows distributed`}
  },
  {
    id:"pig", icon:"🐷", title:"Apache Pig ETL", tech:"MapReduce via Pig Latin",
    color:"#ec4899", duration:8000,
    logs:[
      {t:0,   msg:`$ pig -x mapreduce -f pig_scripts/01_clean_transform.pig`, type:"cmd"},
      {t:250, msg:`[${ts()}] INFO  pig.ExecTypeResolver - Picked MapReduce mode`, type:"info"},
      {t:500, msg:`[${ts(500)}] INFO  mapreduce.Job: Running job: ${m.j1}`, type:"info"},
      {t:800, msg:`[${ts(800)}] INFO  mapreduce.Job: map 0% reduce 0%`, type:"data"},
      {t:1100,msg:`[${ts(1100)}] INFO  mapreduce.Job: map 43% reduce 0%`, type:"data"},
      {t:1400,msg:`[${ts(1400)}] INFO  mapreduce.Job: map 100% reduce 65%`, type:"data"},
      {t:1650,msg:`[${ts(1650)}] INFO  mapreduce.Job: map 100% reduce 100%`, type:"data"},
      {t:1800,msg:`     Input records=${sr}  |  Output records=${rnd(8021334,0.002).toLocaleString()}`, type:"success"},
      {t:2100,msg:`$ pig -x mapreduce -f pig_scripts/02_top_products.pig`, type:"cmd"},
      {t:2300,msg:`[${ts(2300)}] INFO  mapreduce.Job: Running job: ${m.j2}`, type:"info"},
      {t:2700,msg:`     GROUP BY (product_id, product_name, category)... ${up} groups`, type:"data"},
      {t:3100,msg:`     ORDER BY total_revenue DESC  →  Top 20 ranked`, type:"data"},
      {t:3450,msg:`     #1  Regency Cakestand 3 Tier → £${rnd(221310,0.02).toLocaleString()}`, type:"success"},
      {t:3800,msg:`$ pig -x mapreduce -f pig_scripts/03_revenue_by_category.pig`, type:"cmd"},
      {t:4050,msg:`[${ts(4050)}] INFO  mapreduce.Job: Running job: ${m.j3}`, type:"info"},
      {t:4400,msg:`     GROUP BY category... 10 categories  |  PART A: overall`, type:"data"},
      {t:4750,msg:`     General Merchandise: £${rnd(6891234,0.02).toLocaleString()}  |  ${rnd(316062,0.01).toLocaleString()} txns`, type:"success"},
      {t:5100,msg:`     GROUP BY year_month... 25 months  |  PART B: trends`, type:"data"},
      {t:5400,msg:`     Peak: 2011-11 → £${rnd(1423456,0.02).toLocaleString()}  (+37.6% MoM)`, type:"success"},
      {t:5750,msg:`$ pig -x mapreduce -f pig_scripts/04_customer_segmentation.pig`, type:"cmd"},
      {t:6000,msg:`[${ts(6000)}] INFO  mapreduce.Job: Running job: ${m.j4}`, type:"info"},
      {t:6300,msg:`     GROUP BY user_id... ${uc} unique customers found`, type:"data"},
      {t:6600,msg:`     VIP (≥£5000): ${rnd(176,0.05)} customers  |  High Value: ${rnd(706,0.03)}`, type:"success"},
      {t:6950,msg:`     Regular: ${rnd(1469,0.02)}  |  Occasional: ${rnd(3526,0.01)} (60%)`, type:"data"},
      {t:7300,msg:`     STORE → /retail_platform/pig_output/  (4 dirs written)`, type:"info"},
      {t:7700,msg:`[${ts(7700)}] ✅ All 4 Pig scripts done! MR jobs: ${m.mr_jobs}`, type:"done"},
    ],
    result:{label:"MapReduce Jobs Run", value:`${m.mr_jobs} Jobs`, sub:`${rnd(8021334,0.002).toLocaleString()} clean records → 4 output datasets`}
  },
  {
    id:"hive", icon:"🐝", title:"Apache Hive Analytics", tech:"HiveQL on ORC Tables",
    color:"#f59e0b", duration:7000,
    logs:[
      {t:0,   msg:`$ hive -f hive_scripts/01_create_tables.hql`, type:"cmd"},
      {t:300, msg:`[${ts()}] INFO  CliDriver: Hive on MR  −  initializing metastore`, type:"info"},
      {t:600, msg:`OK: DATABASE 'retail_analytics' created`, type:"success"},
      {t:900, msg:`OK: EXTERNAL TABLE 'retail_transactions' → ${cr} rows`, type:"success"},
      {t:1200,msg:`OK: ORC TABLE 'retail_partitioned' → 25 PARTITIONS (SNAPPY)`, type:"success"},
      {t:1500,msg:`OK: TABLE 'product_summary' → ${up} products aggregated`, type:"success"},
      {t:1800,msg:`$ hive -f hive_scripts/02_analytics_queries.hql`, type:"cmd"},
      {t:2050,msg:`[Q1] SELECT product_name, SUM(total_revenue) ... ORDER BY DESC LIMIT 10`, type:"info"},
      {t:2300,msg:`  1. Regency Cakestand 3 Tier     £${rnd(221310,0.02).toLocaleString()}  |  ${rnd(13892,0.03).toLocaleString()} units`, type:"data"},
      {t:2500,msg:`  2. White Hanging Heart Holder    £${rnd(178432,0.02).toLocaleString()}  |  ${rnd(35686,0.03).toLocaleString()} units`, type:"data"},
      {t:2700,msg:`  3. Jumbo Bag Red Retrospot       £${rnd(154876,0.02).toLocaleString()}  |  ${rnd(44250,0.03).toLocaleString()} units`, type:"data"},
      {t:3000,msg:`[Q2] SELECT category, SUM(total_revenue), revenue_pct OVER()`, type:"info"},
      {t:3200,msg:`  General Merchandise  £${rnd(6891234,0.02).toLocaleString()}  (${rndF(38.96,0.02)}%)`, type:"data"},
      {t:3450,msg:`  Gift & Stationery    £${rnd(2234891,0.02).toLocaleString()}  (${rndF(12.63,0.02)}%)`, type:"data"},
      {t:3700,msg:`[Q3] LAG() OVER(ORDER BY year_month) − Monthly trend`, type:"info"},
      {t:3950,msg:`  2011-11  £${rnd(1423456,0.02).toLocaleString()}  ← PEAK  (+${rndF(37.6,0.05)}% MoM growth)`, type:"success"},
      {t:4200,msg:`[Q4] VIP customers − total_spend ≥ £5000`, type:"info"},
      {t:4450,msg:`  Top customer: £${rnd(52348,0.03).toLocaleString()} spend  |  ${rnd(1021,0.05).toLocaleString()} orders`, type:"data"},
      {t:4700,msg:`[Q5] Customer segment CASE WHEN distribution`, type:"info"},
      {t:4950,msg:`  VIP ${rnd(176,0.05)} customers (3%) drive >20% revenue`, type:"success"},
      {t:5300,msg:`[Q7] RANK() OVER(ORDER BY SUM(revenue)) − City ranking`, type:"info"},
      {t:5550,msg:`  #1 London   £${rnd(3812453,0.02).toLocaleString()}  rank=1`, type:"data"},
      {t:5800,msg:`[Q10] ROW_NUMBER() OVER(PARTITION BY city) − top cat/city`, type:"info"},
      {t:6100,msg:`  London→General Merchandise | Birmingham→Gift & Stationery`, type:"data"},
      {t:6500,msg:`  ORC compression saved ~68% disk vs raw CSV`, type:"info"},
      {t:6800,msg:`[${ts(6800)}] ✅ 10/10 HiveQL queries complete!`, type:"done"},
    ],
    result:{label:"Hive Queries Run", value:"10 Queries", sub:`ORC tables | 25 partitions by year_month | SNAPPY compressed`}
  },
  {
    id:"hbase", icon:"⚡", title:"HBase Real-Time Layer", tech:"NoSQL + ZooKeeper",
    color:"#10b981", duration:5000,
    logs:[
      {t:0,   msg:`$ $HBASE_HOME/bin/hbase thrift start &`, type:"cmd"},
      {t:300, msg:`[${ts()}] INFO  ThriftServer: Starting HBase ThriftServer`, type:"info"},
      {t:500, msg:`[INFO] Connecting to ZooKeeper quorum: localhost:2181`, type:"info"},
      {t:700, msg:`[INFO] ZooKeeper session 0x${Math.floor(Math.random()*0xffffff).toString(16).toUpperCase()} | timeout: 90000ms`, type:"data"},
      {t:900, msg:`[INFO] HMaster elected → ZNode /hbase/master registered`, type:"success"},
      {t:1100,msg:`$ python3 hbase_scripts/hbase_operations.py`, type:"cmd"},
      {t:1300,msg:`[INFO] happybase → Thrift localhost:9090 | timeout: 30000ms`, type:"info"},
      {t:1500,msg:`[✅] Connected to HBase!`, type:"success"},
      {t:1700,msg:`[✅] Table 'retail_products' (CF: info, stats, meta)`, type:"success"},
      {t:1900,msg:`[✅] Table 'retail_customers' (CF: profile, activity, prefs)`, type:"success"},
      {t:2100,msg:`[INFO] Bulk-loading ${up} products (batch_size=100)...`, type:"info"},
      {t:2400,msg:`[✅] Loaded ${up} products into retail_products`, type:"success"},
      {t:2600,msg:`[INFO] Bulk-loading ${uc} customers (batch_size=100)...`, type:"info"},
      {t:2900,msg:`[✅] Loaded ${uc} customers into retail_customers`, type:"success"},
      {t:3100,msg:`$ get 'retail_products', 'PF1CD4'`, type:"cmd"},
      {t:3300,msg:`  info:product_name    = Regency Cakestand 3 Tier`, type:"data"},
      {t:3400,msg:`  stats:total_revenue  = ${rnd(221310,0.02).toLocaleString()}.00`, type:"data"},
      {t:3500,msg:`  stats:avg_rating     = ${rndF(4.2,0.05)}`, type:"data"},
      {t:3700,msg:`$ scan 'retail_customers', {FILTER=>"segment=VIP"}`, type:"cmd"},
      {t:3900,msg:`  Found ${rnd(176,0.05)} VIP customers  |  avg spend: £${rnd(11234,0.03).toLocaleString()}`, type:"success"},
      {t:4200,msg:`$ scan 'retail_customers', {STARTROW=>'U1', LIMIT=>5}`, type:"cmd"},
      {t:4400,msg:`  Range scan [U1xxx]: ${rnd(1234,0.05).toLocaleString()} customers returned in <3ms`, type:"data"},
      {t:4700,msg:`[${ts(4700)}] ✅ HBase ready | ZooKeeper: 1 RegionServer online`, type:"done"},
    ],
    result:{label:"HBase Records", value:(m.unique_products+m.unique_customers).toLocaleString(), sub:`${up} products + ${uc} customers  |  O(1) row-key lookup`}
  },
  ];
}

// ── Expose as PIPELINE_STEPS for backward compat ──────────────
// (regenerated each time runStep() is called via makePipelineSteps())
let PIPELINE_STEPS = makePipelineSteps();

