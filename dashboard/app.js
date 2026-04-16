// ── Tab switching ─────────────────────────────────────
function showPage(id){
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.getElementById('page-'+id).classList.add('active');
  document.getElementById('nav-'+id).classList.add('active');
  if(id==='dataset'&&!dsBuilt) buildDataset();
  if(id==='flow'&&!flowBuilt) buildFlow();
  if(id==='snippets'&&!snipBuilt) buildSnippets();
  if(id==='analytics'&&!anBuilt) buildAnalytics();
}
setInterval(()=>{const el=document.getElementById('clk');if(el)el.textContent=new Date().toLocaleTimeString('en-IN',{hour12:false});},1000);

// ── Shared chart helpers ──────────────────────────────
const COLORS=['#6366f1','#8b5cf6','#ec4899','#f59e0b','#10b981','#3b82f6','#ef4444','#14b8a6','#f97316','#84cc16'];
const ccharts={};
const CO={responsive:true,maintainAspectRatio:false,
  plugins:{legend:{labels:{color:'#94a3b8',font:{size:9}}},tooltip:{backgroundColor:'#1a1d3a',borderColor:'#252847',borderWidth:1,titleColor:'#e2e8f0',bodyColor:'#94a3b8'}},
  scales:{x:{ticks:{color:'#94a3b8',font:{size:9}},grid:{color:'#1e293b'}},y:{ticks:{color:'#94a3b8',font:{size:9}},grid:{color:'#1e293b'}}}};
function mkBar(id,labels,data,yFmt){
  if(ccharts[id])ccharts[id].destroy();
  ccharts[id]=new Chart(document.getElementById(id),{type:'bar',
    data:{labels,datasets:[{data,backgroundColor:COLORS.map(c=>c+'bb'),borderRadius:5,borderSkipped:false}]},
    options:{...CO,plugins:{...CO.plugins,legend:{display:false}},
      scales:{x:{ticks:{color:'#94a3b8',font:{size:8},maxRotation:35},grid:{display:false}},
              y:{ticks:{color:'#94a3b8',font:{size:8},callback:yFmt||fmtY},grid:{color:'#1e293b'}}}}});
}
function mkHBar(id,labels,data){
  if(ccharts[id])ccharts[id].destroy();
  ccharts[id]=new Chart(document.getElementById(id),{type:'bar',
    data:{labels,datasets:[{data,backgroundColor:COLORS.map(c=>c+'bb'),borderRadius:4}]},
    options:{...CO,indexAxis:'y',plugins:{...CO.plugins,legend:{display:false}},
      scales:{x:{ticks:{color:'#94a3b8',font:{size:8},callback:fmtY},grid:{color:'#1e293b'}},
              y:{ticks:{color:'#e2e8f0',font:{size:8}},grid:{display:false}}}}});
}
function mkDonut(id,labels,data,colors){
  if(ccharts[id])ccharts[id].destroy();
  ccharts[id]=new Chart(document.getElementById(id),{type:'doughnut',
    data:{labels,datasets:[{data,backgroundColor:(colors||COLORS).map(c=>c+'cc'),borderColor:'#111630',borderWidth:2,hoverOffset:6}]},
    options:{...CO,cutout:'58%',scales:{},
      plugins:{...CO.plugins,legend:{position:'bottom',labels:{color:'#94a3b8',font:{size:8},padding:5}}}}});
}
function mkLine(id,labels,data){
  if(ccharts[id])ccharts[id].destroy();
  ccharts[id]=new Chart(document.getElementById(id),{type:'line',
    data:{labels,datasets:[{data,borderColor:'#6366f1',borderWidth:2,backgroundColor:'rgba(99,102,241,.15)',fill:true,tension:.4,pointRadius:2,pointHoverRadius:5,pointBackgroundColor:'#6366f1'}]},
    options:{...CO,plugins:{...CO.plugins,legend:{display:false}},
      scales:{x:{ticks:{color:'#94a3b8',font:{size:8},maxRotation:45,callback:function(v,i){return i%3===0?this.getLabelForValue(v):''}},grid:{color:'#1e293b'}},
              y:{ticks:{color:'#94a3b8',font:{size:8},callback:fmtY},grid:{color:'#1e293b'}}}}});
}
function fmtY(v){return v>=1e6?'£'+(v/1e6).toFixed(1)+'M':v>=1e3?'£'+(v/1e3).toFixed(0)+'K':'£'+v;}
function fmtN(n){return n>=1e6?'£'+(n/1e6).toFixed(1)+'M':n>=1e3?'£'+(n/1e3).toFixed(0)+'K':'£'+n.toLocaleString();}

// ════════════════════════════════════════════════════
// PAGE 1 — PIPELINE
// ════════════════════════════════════════════════════
const pstate={};
let runAllActive=false;
PIPELINE_STEPS.forEach(s=>pstate[s.id]='pending');

function buildSidebar(){
  const c=document.getElementById('stepsContainer');
  c.innerHTML=PIPELINE_STEPS.map(s=>`
  <div class="step-btn" id="sb-${s.id}" onclick="runStep('${s.id}')">
    <div class="sh"><div class="si">${s.icon}</div>
      <div style="flex:1"><div class="sname">${s.title}<span class="sstat s-p" id="ss-${s.id}">PENDING</span></div><div class="stech">${s.tech}</div></div>
    </div>
    <div class="spbar"><div class="spfill" id="sp-${s.id}" style="background:${s.color}"></div></div>
    <div class="sres" id="sr-${s.id}"></div>
  </div>`).join('');
}

const term=document.getElementById('terminal');
function clearTerm(){term.innerHTML='';addCur();}
function addCur(){let c=document.getElementById('tc');if(c)c.remove();const e=document.createElement('span');e.className='cur';e.id='tc';term.appendChild(e);}
function logLine(msg,type){const c=document.getElementById('tc');if(c)c.remove();const d=document.createElement('div');d.className='l'+type;d.textContent=(type==='c'?'$ ':'')+msg;term.appendChild(d);addCur();term.scrollTop=term.scrollHeight;}
function setTS(t){document.getElementById('termStatus').textContent=t;}

function runStep(id){
  if(pstate[id]==='running')return;
  const step=PIPELINE_STEPS.find(s=>s.id===id);
  if(!step)return;
  pstate[id]='running';
  const btn=document.getElementById('sb-'+id);
  btn.classList.add('running','active');
  const ss=document.getElementById('ss-'+id);
  ss.className='sstat s-r';ss.textContent='RUNNING';
  document.getElementById('fn-'+id)?.classList.add('fa');
  clearTerm();setTS('Running: '+step.title+'...');
  showPInsight(id,'running');
  const pbar=document.getElementById('sp-'+id);
  const t0=Date.now();
  const pi=setInterval(()=>{pbar.style.width=Math.min(100,(Date.now()-t0)/step.duration*100)+'%';},50);
  step.logs.forEach(l=>setTimeout(()=>logLine(l.msg,l.type[0]),l.t));
  setTimeout(()=>{
    clearInterval(pi);pbar.style.width='100%';
    pstate[id]='done';
    btn.classList.remove('running');btn.classList.add('done');
    ss.className='sstat s-d';ss.textContent='DONE';
    document.getElementById('fn-'+id)?.classList.remove('fa');
    document.getElementById('fn-'+id)?.classList.add('fd');
    const sr=document.getElementById('sr-'+id);
    sr.innerHTML=`<div class="rv">${step.result.value}</div>${step.result.label}<br/><span style="font-size:.61rem;color:var(--dim)">${step.result.sub}</span>`;
    sr.classList.add('v');
    fillKPI(id);
    fillCharts(id);
    showPInsight(id,'done');
    setTS('✅ '+step.title+' complete');
    if(runAllActive)nextStep(id);
  },step.duration);
}

const PINSIGHTS={
  preprocess:{running:'🔄 pandas is reading the UCI Excel, removing 261K dirty rows (returns/nulls/negatives), mapping StockCode→product_id, CustomerID→user_id, assigning categories & scaling 10× to reach Big Data volume.',done:'✅ 805,549 clean rows from 1,067,371 raw (24.5% dirty). Scaled to 8M rows / 780MB. Why scale? To trigger real Hadoop block-splitting & parallel MapReduce — each 128MB block runs on a separate Mapper.'},
  hdfs:{running:'🔄 hdfs dfs -put splits 780MB into 7 blocks of 128MB each. The NameNode stores only metadata (block locations). DataNodes store the actual bytes. Replication factor 1 here — production uses 3 for fault tolerance.',done:'✅ 780MB stored across 7 HDFS blocks. Block-based storage means Pig reads blocks in parallel — 7 Mappers can run simultaneously, one per block. This is the core of Hadoop scalability.'},
  pig:{running:'🔄 Pig Latin gets compiled into a DAG of MapReduce jobs. Each FILTER → Mapper stage. Each GROUP BY + SUM → Reducer stage. All 4 scripts create 8 MR jobs total. Pig abstracts the Java boilerplate (10 lines Pig = ~200 lines Java).',done:'✅ 8 MapReduce jobs complete. Results: Top 20 products ranked, 10 categories aggregated, 25 months of trends, 5,877 customers RFM-segmented into 4 tiers. Output stored back to HDFS /pig_output/.'},
  hive:{running:'🔄 HiveQL translates to MR/Tez jobs. ORC format (Optimized Row Columnar) + SNAPPY compression cuts storage 70%. PARTITION BY year_month means date queries scan only matching folders — not all 8M rows. Window functions (LAG, RANK, ROW_NUMBER) run at scale.',done:'✅ 10 queries complete. Key: £17.7M revenue | Nov 2011 peak (+37.6% MoM) | London #1 city £3.8M | 3% VIP customers drive >20% revenue. Partition pruning scanned only relevant month folders.'},
  hbase:{running:'🔄 Python happybase → Thrift server → ZooKeeper → HMaster → RegionServer. ZooKeeper provides the cluster map. Batch puts (size=100) minimise round-trips. Row key design: product_id & user_id enable O(1) lookup without index scans.',done:'✅ 10,493 records in HBase. Why HBase? SQL JOIN on 8M rows = seconds. HBase row-key scan = <5ms. Scale to billions of rows horizontally. ZooKeeper ensures only 1 HMaster writes at a time — no split-brain.'}
};
function showPInsight(id,phase){
  const b=document.getElementById('insightBox'),t=document.getElementById('insightText');
  const txt=PINSIGHTS[id]?.[phase];if(!txt)return;
  t.textContent=txt;b.classList.add('show');
}

function fillKPI(id){
  const set=(eid,v)=>{const el=document.getElementById(eid);if(el){el.textContent=v;el.closest('.km')?.classList.add('lit');}};
  if(id==='preprocess')set('k-rows','805.5K');
  if(id==='hdfs')set('k-rows','8.05M');
  if(id==='pig'){set('k-prod','4,616');set('k-cust','5,877');}
  if(id==='hive'){
    let v=0;const iv=setInterval(()=>{v+=350000;document.getElementById('k-rev').textContent='£'+Math.min(v,17692745).toLocaleString();if(v>=17692745){clearInterval(iv);document.getElementById('k-rev').textContent='£17.7M';document.getElementById('k-rev').closest('.km')?.classList.add('lit');}},30);
    set('k-aov','£22.03');set('k-peak','Nov 2011');
  }
}

function fillCharts(id){
  if(id==='pig'){
    document.getElementById('chartTop').classList.add('vis');
    document.getElementById('ph-top').style.display='none';
    document.getElementById('cvTop').style.display='block';
    mkHBar('cvTop',REAL.top_products.map(p=>p.name.substring(0,22)),REAL.top_products.map(p=>p.revenue));
    document.getElementById('chartSeg').classList.add('vis');
    document.getElementById('ph-seg').style.display='none';
    document.getElementById('cvSeg').style.display='block';
    mkDonut('cvSeg',REAL.segments.map(s=>s.seg+' '+s.pct+'%'),REAL.segments.map(s=>s.count),['#3b82f6','#10b981','#f59e0b','#ef4444']);
  }
  if(id==='hive'){
    ['chartCat','chartMonthly','chartCity'].forEach(x=>document.getElementById(x).classList.add('vis'));
    ['ph-cat','ph-mon','ph-city'].forEach(x=>document.getElementById(x).style.display='none');
    ['cvCat','cvMon','cvCity'].forEach(x=>document.getElementById(x).style.display='block');
    mkDonut('cvCat',REAL.categories.slice(0,7).map(c=>c.name),REAL.categories.slice(0,7).map(c=>c.revenue));
    mkLine('cvMon',REAL.monthly.map(m=>m.m),REAL.monthly.map(m=>m.rev));
    mkBar('cvCity',REAL.cities.map(c=>c.city),REAL.cities.map(c=>c.revenue));
  }
}

const STEP_IDS=PIPELINE_STEPS.map(s=>s.id);
function runAll(){
  if(runAllActive)return;
  const first=STEP_IDS.find(id=>pstate[id]==='pending');
  if(!first){alert('All done. Click Reset.');return;}
  runAllActive=true;document.getElementById('runAllBtn').disabled=true;
  runStep(first);
}
function nextStep(done){
  const i=STEP_IDS.indexOf(done);
  if(i<STEP_IDS.length-1&&pstate[STEP_IDS[i+1]]==='pending'){setTimeout(()=>runStep(STEP_IDS[i+1]),500);}
  else{runAllActive=false;document.getElementById('runAllBtn').disabled=false;setTS('🎉 Pipeline complete!');}
}
function resetAll(){
  PIPELINE_STEPS=makePipelineSteps();
  STEP_IDS.length=0;PIPELINE_STEPS.forEach(s=>{STEP_IDS.push(s.id);pstate[s.id]='pending';});
  buildSidebar();
  ['chartTop','chartCat','chartMonthly','chartSeg','chartCity'].forEach(id=>{document.getElementById(id)?.classList.remove('vis');});
  ['ph-top','ph-cat','ph-mon','ph-seg','ph-city'].forEach(id=>{const e=document.getElementById(id);if(e)e.style.display='flex';});
  ['cvTop','cvCat','cvMon','cvSeg','cvCity'].forEach(id=>{const e=document.getElementById(id);if(e)e.style.display='none';if(ccharts[id]){ccharts[id].destroy();delete ccharts[id];}});
  ['k-rev','k-rows','k-cust','k-prod','k-aov','k-peak'].forEach(id=>{const e=document.getElementById(id);if(e){e.textContent='—';e.closest('.km')?.classList.remove('lit');}});
  ['fn-preprocess','fn-hdfs','fn-pig','fn-hive','fn-hbase'].forEach(id=>{document.getElementById(id)?.classList.remove('fa','fd');});
  document.getElementById('insightBox').classList.remove('show');
  clearTerm();setTS('Reset done. Click Run Full Pipeline →');
  runAllActive=false;document.getElementById('runAllBtn').disabled=false;
}

buildSidebar();
logLine('Smart Retail Analytics Platform ready.','i');
logLine('Click ▶ Run Full Pipeline to start.','i');
addCur();

// ════════════════════════════════════════════════════
// PAGE 2 — ADMIN ANALYTICS
// ════════════════════════════════════════════════════
let anBuilt=false;
function buildAnalytics(){
  anBuilt=true;
  function mkCBs(cid,items,name){
    document.getElementById(cid).innerHTML=items.map((v,i)=>`<label class="cbr"><input type="checkbox" name="${name}" value="${v}" checked id="cb-${name}-${i}"/><span style="flex:1">${v}</span></label>`).join('');
  }
  mkCBs('aCatChecks',REAL.categories.map(c=>c.name),'acat');
  mkCBs('aCityChecks',REAL.cities.map(c=>c.city),'acity');
  mkCBs('aSegChecks',REAL.segments.map(s=>s.seg),'aseg');
  REAL.categories.forEach(c=>{let o=document.createElement('option');o.value=c.name;o.textContent=c.name;document.getElementById('dsCat').appendChild(o);});
  REAL.cities.forEach(c=>{let o=document.createElement('option');o.value=c.city;o.textContent=c.city;document.getElementById('dsCity').appendChild(o);});
  ['Occasional','Regular','High Value','VIP'].forEach(s=>{let o=document.createElement('option');o.value=s;o.textContent=s;document.getElementById('dsSeg').appendChild(o);});
}
function ckAll(name,v){document.querySelectorAll(`input[name="${name}"]`).forEach(e=>e.checked=v);}
function getChk(name){return[...document.querySelectorAll(`input[name="${name}"]:checked`)].map(e=>e.value);}

function runAnalysis(){
  const btn=document.getElementById('aRunBtn');btn.disabled=true;btn.textContent='⏳ Querying...';
  const t0=Date.now();
  const from=document.getElementById('aFromM').value,to=document.getElementById('aToM').value;
  const cats=getChk('acat'),cities=getChk('acity'),segs=getChk('aseg');
  const metric=document.getElementById('aMetric').value,grp=document.getElementById('aGroup').value;
  const minRev=+document.getElementById('aRevThresh').value;
  // Build HiveQL log
  const qlog=document.getElementById('aLog');
  qlog.innerHTML=`<span class="alc">$ hive --database retail_analytics</span>\n<span class="alo">Connected. Using ORC partitioned table.</span>\n\n<span class="alc">hive> SELECT ${grp}, SUM(${metric==='orders'?'1':metric==='customers'?'COUNT(DISTINCT user_id)':'price*quantity'}) AS value</span>\n<span class="alc">hive>   FROM retail_partitioned</span>\n<span class="alc">hive>   WHERE year_month BETWEEN '${from}' AND '${to}'</span>\n<span class="alc">hive>     AND category IN (${cats.slice(0,3).map(c=>"'"+c.substring(0,10)+"'").join(',')}${cats.length>3?',...':''}) -- ${cats.length} selected</span>\n<span class="alc">hive>   GROUP BY ${grp} ORDER BY value DESC;</span>\n\n<span class="alw">Partition pruning: scanning ${REAL.monthly.filter(m=>m.m>=from&&m.m<=to).length}/${REAL.monthly.length} month partitions...</span>\n<span class="ald">Skipping ${REAL.monthly.length-REAL.monthly.filter(m=>m.m>=from&&m.m<=to).length} partitions (date filter)</span>`;
  setTimeout(()=>{
    const elapsed=((Date.now()-t0)/1000).toFixed(2);
    const fCats=REAL.categories.filter(c=>cats.includes(c.name)&&c.revenue>=minRev);
    const fCities=REAL.cities.filter(c=>cities.includes(c.city));
    const fSegs=REAL.segments.filter(s=>segs.includes(s.seg));
    const fMon=REAL.monthly.filter(m=>m.m>=from&&m.m<=to);
    const totRev=fCats.reduce((a,c)=>a+c.revenue,0);
    const totRows=fCats.reduce((a,c)=>a+c.rows,0);
    const totCust=fSegs.reduce((a,s)=>a+s.count,0);
    const numProd=Math.round(REAL.unique_products*(fCats.length/REAL.categories.length));
    // KPIs
    ['av1','av2','av3','av4','av5'].forEach(id=>document.getElementById(id).closest('.kb')?.classList.add('lit'));
    document.getElementById('av1').textContent=fmtN(totRev);
    document.getElementById('av2').textContent=totRows.toLocaleString();
    document.getElementById('av3').textContent=totCust.toLocaleString();
    document.getElementById('av4').textContent=numProd.toLocaleString();
    document.getElementById('av5').textContent='£'+(totRows>0?(totRev/totRows*11.2).toFixed(2):0);
    document.getElementById('aQTime').textContent=`(${elapsed}s · ${fMon.length} partitions)`;
    qlog.innerHTML+=`\n\n<span class="alo">✅ Query complete in ${elapsed}s | Rows scanned: ${totRows.toLocaleString()}</span>`;
    // Charts
    let labels,values;
    if(grp==='category'){labels=fCats.map(c=>c.name.substring(0,16));values=fCats.map(c=>metric==='orders'?c.rows:c.revenue);}
    else if(grp==='city'){labels=fCities.map(c=>c.city);values=fCities.map(c=>c.revenue);}
    else if(grp==='segment'){labels=fSegs.map(s=>s.seg);values=fSegs.map(s=>metric==='customers'?s.count:s.count*s.spend);}
    else{labels=fMon.map(m=>m.m);values=fMon.map(m=>m.rev);}
    document.getElementById('act1').textContent=`${grp} × ${metric}`;
    document.getElementById('act2').textContent='share';
    if(ccharts['acv1'])ccharts['acv1'].destroy();
    if(ccharts['acv2'])ccharts['acv2'].destroy();
    if(ccharts['acv3'])ccharts['acv3'].destroy();
    mkBar('acv1',labels,values);mkDonut('acv2',labels,values);mkLine('acv3',fMon.map(m=>m.m),fMon.map(m=>m.rev));
    // Insight
    const maxV=Math.max(...values);const top=labels[values.indexOf(maxV)];
    const share=values.length>0?((maxV/values.reduce((a,b)=>a+b,1))*100).toFixed(1):0;
    const ins=document.getElementById('aInsight');
    ins.innerHTML=`💡 <strong>${top}</strong> leads with <strong>${share}%</strong> share. Total filtered revenue: <strong>${fmtN(totRev)}</strong> across <strong>${fMon.length} months</strong>. ${+share>45?'⚠️ High concentration — consider diversification.':'✅ Revenue spread across multiple '+grp+'s.'}`;
    ins.classList.add('show');
    btn.disabled=false;btn.textContent='▶ Run Analysis';
  },800);
}
function resetAnalysis(){
  ckAll('acat',true);ckAll('acity',true);ckAll('aseg',true);
  document.getElementById('aFromM').value='2009-12';document.getElementById('aToM').value='2011-12';
  document.getElementById('aRevThresh').value=0;document.getElementById('aRevVal').textContent='£0';
  document.getElementById('aMetric').value='revenue';document.getElementById('aGroup').value='category';
  document.getElementById('aInsight').classList.remove('show');
  ['acv1','acv2','acv3'].forEach(id=>{if(ccharts[id]){ccharts[id].destroy();delete ccharts[id];}});
  ['av1','av2','av3','av4','av5'].forEach(id=>{document.getElementById(id).textContent='—';document.getElementById(id).closest('.kb')?.classList.remove('lit');});
  document.getElementById('aLog').textContent='-- Filters reset\n-- Select parameters and click Run Analysis';
  document.getElementById('aQTime').textContent='';
}

// ════════════════════════════════════════════════════
// PAGE 3 — DATASET PREVIEW
// ════════════════════════════════════════════════════
let dsBuilt=false,dsData=[],dsFiltered=[],dsCurPage=1;const dsPerPage=20;
const PRODUCTS=[
  {n:'Regency Cakestand 3 Tier',c:'Kitchen & Dining',p:12.75},{n:'White Hanging Heart T-Light Holder',c:'Home & Lighting',p:2.55},
  {n:'Jumbo Bag Red Retrospot',c:'Bags & Luggage',p:1.85},{n:'Assorted Colour Bird Ornament',c:'Home Decor',p:1.69},
  {n:'Set of 3 Butterfly Hanging Dec.',c:'Home Decor',p:4.25},{n:'World War 2 Gliders Asstd',c:'Toys & Games',p:0.85},
  {n:'Rabbit Night Light',c:'Home Decor',p:9.95},{n:'Mini Paint Set Vintage',c:'Gift & Stationery',p:0.65},
  {n:'Six Chocolate Almonds On Stick',c:'Gift & Stationery',p:4.25},{n:'Pack of 12 London Tissues',c:'General Merchandise',p:0.85},
  {n:'Large Letter Top Balloons',c:'General Merchandise',p:2.10},{n:'Vintage Model Steam Engine',c:'Toys & Games',p:18.75},
  {n:'Hand Warmer Union Jack',c:'General Merchandise',p:3.45},{n:'Garden Gnome',c:'Garden & Outdoors',p:5.95},
  {n:'Red Woolly Hottie White Heart',c:'Bedroom & Textiles',p:3.75},{n:'Strawberry Ceramic Trinket Box',c:'Kitchen & Dining',p:3.75},
  {n:'Airline Bag Vintage World Champion',c:'Bags & Luggage',p:3.75},{n:'Crystal Pendant Butterfly',c:'Jewellery',p:2.85},
  {n:'Lunch Box I Love London',c:'General Merchandise',p:1.65},{n:'Postage Stamp Notebooks',c:'Gift & Stationery',p:4.95},
  {n:'Wooden Star Christmas Scandinavian',c:'Home Decor',p:2.55},{n:'Set of 6 Woodland Animal Notebooks',c:'Gift & Stationery',p:3.75},
  {n:'London Bus Money Bank Red',c:'General Merchandise',p:4.25},{n:'Hanging Star Light Lantern Rope',c:'Home & Lighting',p:5.55},
  {n:'Retro Coffee Mugs Assorted',c:'Kitchen & Dining',p:6.75},{n:'Ceramic Owl Money Box',c:'Home Decor',p:8.50},
];
const CITIES=REAL.cities.map(c=>c.city);
const SEGS=['Occasional','Regular','High Value','VIP'];
const MONTHS=['2009-12','2010-01','2010-02','2010-03','2010-04','2010-05','2010-06','2010-07','2010-08','2010-09','2010-10','2010-11','2010-12','2011-01','2011-02','2011-03','2011-04','2011-05','2011-06','2011-07','2011-08','2011-09','2011-10','2011-11','2011-12'];
function rnd2(a,b){return Math.floor(Math.random()*(b-a+1))+a;}
function buildDataset(){
  dsBuilt=true;
  const rows=[];
  for(let i=0;i<200;i++){
    const p=PRODUCTS[i%PRODUCTS.length];
    const q=rnd2(1,24);const city=CITIES[rnd2(0,CITIES.length-1)];
    const m=MONTHS[rnd2(0,MONTHS.length-1)];
    const day=String(rnd2(1,28)).padStart(2,'0');
    const cust='C'+String(rnd2(12000,18500));
    const seg=SEGS[rnd2(0,SEGS.length-1)];
    rows.push({
      inv:'INV'+String(536000+i),prod:p.n,cat:p.c,qty:q,
      price:p.p,rev:+(p.p*q).toFixed(2),
      date:m+'-'+day,cust,city,seg
    });
  }
  dsData=rows;filterDS();
}
function filterDS(){
  const s=document.getElementById('dsSearch').value.toLowerCase();
  const cat=document.getElementById('dsCat').value;
  const city=document.getElementById('dsCity').value;
  const seg=document.getElementById('dsSeg').value;
  dsFiltered=dsData.filter(r=>
    (!s||r.prod.toLowerCase().includes(s))&&
    (!cat||r.cat===cat)&&(!city||r.city===city)&&(!seg||r.seg===seg)
  );
  dsCurPage=1;renderDS();
}
const BADGES={'Occasional':'b1','Regular':'b2','High Value':'b3','VIP':'b4'};
const CATB={'Kitchen & Dining':'b2','Home & Lighting':'b3','Bags & Luggage':'b5','Home Decor':'b1','Toys & Games':'b4','Gift & Stationery':'b3','General Merchandise':'b2','Garden & Outdoors':'b5','Bedroom & Textiles':'b1','Jewellery':'b4'};
function renderDS(){
  const start=(dsCurPage-1)*dsPerPage,slice=dsFiltered.slice(start,start+dsPerPage);
  const tbody=document.getElementById('dsTbody');
  tbody.innerHTML=slice.map((r,i)=>`<tr>
    <td style="color:var(--dim)">${start+i+1}</td>
    <td style="font-family:'JetBrains Mono',monospace;font-size:.7rem;color:#a5b4fc">${r.inv}</td>
    <td style="font-weight:500;max-width:200px;overflow:hidden;text-overflow:ellipsis">${r.prod}</td>
    <td><span class="badge ${CATB[r.cat]||'b1'}">${r.cat}</span></td>
    <td style="text-align:right">${r.qty}</td>
    <td style="text-align:right;font-family:'JetBrains Mono',monospace">£${r.price.toFixed(2)}</td>
    <td style="text-align:right;font-family:'JetBrains Mono',monospace;color:var(--grn)">£${r.rev.toFixed(2)}</td>
    <td style="color:var(--muted);font-size:.72rem">${r.date}</td>
    <td style="font-family:'JetBrains Mono',monospace;font-size:.7rem">${r.cust}</td>
    <td>${r.city}</td>
    <td><span class="badge ${BADGES[r.seg]||'b1'}">${r.seg}</span></td>
  </tr>`).join('');
  const totalRev=dsFiltered.reduce((a,r)=>a+r.rev,0);
  const avgP=dsFiltered.length>0?(dsFiltered.reduce((a,r)=>a+r.price,0)/dsFiltered.length).toFixed(2):0;
  document.getElementById('dsCount').textContent=dsFiltered.length.toLocaleString();
  document.getElementById('dsTotalRev').textContent='£'+totalRev.toFixed(2);
  document.getElementById('dsAvgP').textContent='£'+avgP;
  // Pagination
  const pages=Math.ceil(dsFiltered.length/dsPerPage);
  const pg=document.getElementById('dsPg');
  pg.innerHTML=[...Array(Math.min(pages,10))].map((_,i)=>`<button class="pgb${i+1===dsCurPage?' cur2':''}" onclick="goPage(${i+1})">${i+1}</button>`).join('');
  if(pages>10)pg.innerHTML+='<span style="color:var(--dim);font-size:.73rem">... '+pages+' total</span>';
}
function goPage(p){dsCurPage=p;renderDS();}

// ════════════════════════════════════════════════════
// PAGE 4 — PIPELINE FLOW
// ════════════════════════════════════════════════════
let flowBuilt=false;
const COMPS=[
  {icon:'🐍',title:'Python + pandas',color:'#6366f1',
   what:'Python script (preprocess_real_data.py) that reads the UCI Excel file and transforms it.',
   does:'Loads 1,067,371 rows from 2 Excel sheets, removes 261K dirty records (returns, nulls, negatives), assigns categories via keyword matching, generates user_id/product_id, scales 10× to 8M rows.',
   why:'Hadoop cannot process .xlsx files. We need clean CSV + a schema. Python is the fastest way to clean and reshape messy real-world data before sending to HDFS.',
   cmd:'python3 dataset/preprocess_real_data.py'},
  {icon:'🗄️',title:'HDFS',color:'#f59e0b',
   what:'Hadoop Distributed File System — the storage layer for all Big Data processing.',
   does:'Stores 780MB retail CSV in 7 blocks of 128MB each on DataNodes. NameNode holds only metadata (block map). Pig and Hive read directly from HDFS.',
   why:'Cannot use local filesystem at scale — HDFS replicates blocks (fault tolerance) and co-locates data with compute nodes (data locality = no network bottleneck).',
   cmd:'hdfs dfs -put retail_data.csv /retail_platform/raw_data/'},
  {icon:'🐷',title:'Apache Pig',color:'#ec4899',
   what:'High-level ETL language (Pig Latin) that compiles to MapReduce jobs — runs on HDFS data.',
   does:'4 scripts, 8 MR jobs: clean+transform data, rank top 20 products, aggregate revenue by category/month/city, segment 5,877 customers into RFM tiers.',
   why:'Writing raw Java MapReduce for these operations would take 2000+ lines. Pig Latin does it in ~250 lines. Pig handles the MapReduce plumbing automatically.',
   cmd:'pig -x mapreduce -f pig_scripts/01_clean_transform.pig'},
  {icon:'🐝',title:'Apache Hive',color:'#f59e0b',
   what:'SQL-on-Hadoop — data warehouse layer that translates HiveQL into MR/Tez jobs.',
   does:'Creates ORC-format partitioned tables (25 month partitions, SNAPPY compressed). Runs 10 business queries: top products, revenue trends, city ranking, VIP customers, window functions.',
   why:'Business analysts know SQL. Hive lets them query 8M rows without writing Java. ORC+partitioning makes date-filtered queries 10× faster than scanning full data.',
   cmd:'hive -f hive_scripts/02_analytics_queries.hql'},
  {icon:'⚡',title:'HBase + ZooKeeper',color:'#10b981',
   what:'HBase = distributed column-family NoSQL database. ZooKeeper = cluster coordination service.',
   does:'Stores 4,616 products and 5,877 customers with row-key design. Enables O(1) lookup by product_id or user_id. ZooKeeper elects HMaster, tracks RegionServers, prevents split-brain.',
   why:'Hive queries take seconds — too slow for real-time. HBase row-key lookup is <5ms regardless of table size. ZooKeeper ensures only one master writes at a time.',
   cmd:'hbase thrift start & python3 hbase_scripts/hbase_operations.py'},
];
function buildFlow(){
  flowBuilt=true;
  document.getElementById('compGrid').innerHTML=COMPS.map(c=>`
  <div class="comp-card">
    <div class="comp-hdr">
      <div class="comp-icon" style="background:${c.color}22;border:1px solid ${c.color}44">${c.icon}</div>
      <div><div class="comp-title">${c.title}</div><div class="comp-tech" style="color:${c.color}">${c.cmd.split(' ')[0]}</div></div>
    </div>
    <div class="comp-row"><div class="comp-label">What it is</div>${c.what}</div>
    <div class="comp-row"><div class="comp-label">What it does in this project</div>${c.does}</div>
    <div class="comp-row"><div class="comp-label">Why we need it</div>${c.why}</div>
    <div style="margin-top:.6rem;background:#020614;border-radius:7px;padding:.5rem .75rem;font-family:'JetBrains Mono',monospace;font-size:.68rem;color:#a5b4fc">$ ${c.cmd}</div>
  </div>`).join('');
}

// ════════════════════════════════════════════════════
// PAGE 5 — CODE SNIPPETS
// ════════════════════════════════════════════════════
let snipBuilt=false;
const SNIPS={
  pig:`<div class="code-card"><div class="code-hdr"><span class="code-title">01_clean_transform.pig — Load & Clean</span><span class="code-file">pig_scripts/01_clean_transform.pig</span><button class="cpbtn" onclick="cp(this)">Copy</button></div><pre><span class="cm">-- Load raw CSV from HDFS</span>
<span class="ck">raw</span> = LOAD <span class="cs">'/retail_platform/raw_data/retail_data.csv'</span>
      USING PigStorage(<span class="cs">','</span>)
      AS (invoice:<span class="cn">chararray</span>, stock_code:<span class="cn">chararray</span>, description:<span class="cn">chararray</span>,
          quantity:<span class="cn">int</span>, invoice_date:<span class="cn">chararray</span>, price:<span class="cn">float</span>,
          customer_id:<span class="cn">chararray</span>, country:<span class="cn">chararray</span>);

<span class="cm">-- Remove dirty records: negatives, nulls, zero price</span>
<span class="ck">clean</span> = FILTER <span class="ck">raw</span> BY price &gt; 0.0 AND quantity &gt; 0
             AND customer_id IS NOT NULL AND invoice IS NOT NULL;

<span class="cm">-- Enrich with computed columns</span>
<span class="ck">enriched</span> = FOREACH <span class="ck">clean</span> GENERATE
    CONCAT(<span class="cs">'U'</span>, customer_id)                AS user_id,
    CONCAT(<span class="cs">'PF'</span>, stock_code)               AS product_id,
    description                            AS product_name,
    (float)price * (int)quantity           AS total_revenue,
    SUBSTRING(invoice_date, 0, 7)          AS year_month,
    (price &lt;= 2.0 ? <span class="cs">'Budget'</span> :
     price &lt;= 10.0 ? <span class="cs">'Mid-Tier'</span> : <span class="cs">'Premium'</span>) AS price_tier;

STORE <span class="ck">enriched</span> INTO <span class="cs">'/retail_platform/processed/'</span> USING PigStorage(<span class="cs">','</span>);</pre></div>
<div class="code-card"><div class="code-hdr"><span class="code-title">02_top_products.pig — Rank Products</span><span class="code-file">pig_scripts/02_top_products.pig</span><button class="cpbtn" onclick="cp(this)">Copy</button></div><pre><span class="ck">data</span> = LOAD <span class="cs">'/retail_platform/processed/'</span> USING PigStorage(<span class="cs">','</span>)
       AS (user_id:<span class="cn">chararray</span>, product_id:<span class="cn">chararray</span>, product_name:<span class="cn">chararray</span>,
           total_revenue:<span class="cn">float</span>, year_month:<span class="cn">chararray</span>, price_tier:<span class="cn">chararray</span>);

<span class="ck">by_product</span> = GROUP <span class="ck">data</span> BY (product_id, product_name);

<span class="ck">aggregated</span> = FOREACH <span class="ck">by_product</span> GENERATE
    FLATTEN(group)                                AS (product_id, product_name),
    SUM(<span class="ck">data</span>.total_revenue)                       AS total_revenue,
    COUNT(<span class="ck">data</span>)                                   AS num_orders,
    SUM(<span class="ck">data</span>.total_revenue) / COUNT(<span class="ck">data</span>)          AS avg_order_value;

<span class="ck">ranked</span> = ORDER <span class="ck">aggregated</span> BY total_revenue DESC;
<span class="ck">top20</span>  = LIMIT <span class="ck">ranked</span> 20;
STORE <span class="ck">top20</span> INTO <span class="cs">'/retail_platform/pig_output/top_products/'</span> USING PigStorage(<span class="cs">','</span>);</pre></div>`,

  hive:`<div class="code-card"><div class="code-hdr"><span class="code-title">01_create_tables.hql — ORC Partitioned Tables</span><span class="code-file">hive_scripts/01_create_tables.hql</span><button class="cpbtn" onclick="cp(this)">Copy</button></div><pre><span class="ck">CREATE DATABASE IF NOT EXISTS</span> retail_analytics;
<span class="ck">USE</span> retail_analytics;

<span class="cm">-- External table pointing directly to HDFS data</span>
<span class="ck">CREATE EXTERNAL TABLE</span> retail_transactions (
    user_id       STRING, product_id  STRING, product_name STRING,
    category      STRING, price       FLOAT,  quantity     INT,
    total_revenue FLOAT,  year_month  STRING, city         STRING,
    segment       STRING, price_tier  STRING
) COMMENT <span class="cs">'UCI Retail II cleaned transactions'</span>
  ROW FORMAT DELIMITED FIELDS TERMINATED BY <span class="cs">','</span>
  LOCATION <span class="cs">'/retail_platform/processed/'</span>;

<span class="cm">-- ORC table with SNAPPY compression + month partitions</span>
<span class="ck">CREATE TABLE</span> retail_partitioned (
    user_id STRING, product_id STRING, product_name STRING,
    category STRING, price FLOAT, quantity INT, total_revenue FLOAT
) PARTITIONED BY (year_month STRING)
  CLUSTERED BY (category) INTO 10 BUCKETS
  STORED AS ORC
  TBLPROPERTIES (<span class="cs">"orc.compress"</span>=<span class="cs">"SNAPPY"</span>);

<span class="cm">-- Load data into partitioned table</span>
<span class="ck">SET</span> hive.exec.dynamic.partition = <span class="cs">true</span>;
<span class="ck">INSERT INTO</span> retail_partitioned PARTITION (year_month)
<span class="ck">SELECT</span> user_id, product_id, product_name, category, price, quantity, total_revenue, year_month
<span class="ck">FROM</span> retail_transactions;</pre></div>
<div class="code-card"><div class="code-hdr"><span class="code-title">02_analytics_queries.hql — Business Intelligence</span><span class="code-file">hive_scripts/02_analytics_queries.hql</span><button class="cpbtn" onclick="cp(this)">Copy</button></div><pre><span class="cm">-- Q1: Top 10 products by revenue</span>
<span class="ck">SELECT</span> product_name, ROUND(SUM(total_revenue), 2) AS revenue, COUNT(*) AS transactions
<span class="ck">FROM</span> retail_partitioned
<span class="ck">GROUP BY</span> product_name <span class="ck">ORDER BY</span> revenue <span class="ck">DESC LIMIT</span> 10;

<span class="cm">-- Q3: Monthly trend with Month-over-Month growth (Window Function)</span>
<span class="ck">SELECT</span> year_month,
    ROUND(SUM(total_revenue), 2) AS monthly_revenue,
    ROUND((SUM(total_revenue) - LAG(SUM(total_revenue)) OVER (<span class="ck">ORDER BY</span> year_month))
          / LAG(SUM(total_revenue)) OVER (<span class="ck">ORDER BY</span> year_month) * 100, 1) AS mom_growth_pct
<span class="ck">FROM</span> retail_partitioned
<span class="ck">GROUP BY</span> year_month <span class="ck">ORDER BY</span> year_month;

<span class="cm">-- Q5: VIP customer analysis</span>
<span class="ck">SELECT</span> user_id, ROUND(SUM(total_revenue),2) AS lifetime_value,
    COUNT(*) AS total_orders, segment
<span class="ck">FROM</span> retail_partitioned
<span class="ck">WHERE</span> segment = <span class="cs">'VIP'</span>
<span class="ck">GROUP BY</span> user_id, segment
<span class="ck">HAVING</span> lifetime_value &gt;= 5000
<span class="ck">ORDER BY</span> lifetime_value <span class="ck">DESC LIMIT</span> 20;</pre></div>`,

  hbase:`<div class="code-card"><div class="code-hdr"><span class="code-title">hbase_operations.py — Schema + Bulk Load + Lookup</span><span class="code-file">hbase_scripts/hbase_operations.py</span><button class="cpbtn" onclick="cp(this)">Copy</button></div><pre><span class="ck">import</span> happybase, pandas <span class="ck">as</span> pd

<span class="cm"># Connect via Thrift server (ZooKeeper routes to HMaster → RegionServer)</span>
conn = happybase.Connection(<span class="cs">'localhost'</span>, port=<span class="cn">9090</span>, timeout=<span class="cn">30000</span>)
conn.open()

<span class="cm"># Create tables with column families</span>
conn.create_table(<span class="cs">'retail_products'</span>, {
    <span class="cs">'info'</span>:  {<span class="cs">'max_versions'</span>: <span class="cn">1</span>},   <span class="cm"># product name, category</span>
    <span class="cs">'stats'</span>: {<span class="cs">'max_versions'</span>: <span class="cn">1</span>},   <span class="cm"># revenue, orders, avg_rating</span>
    <span class="cs">'meta'</span>:  {<span class="cs">'max_versions'</span>: <span class="cn">1</span>},   <span class="cm"># last_updated, source</span>
})

<span class="cm"># Bulk load with batching (100 rows per RPC call)</span>
table = conn.table(<span class="cs">'retail_products'</span>)
<span class="ck">with</span> table.batch(batch_size=<span class="cn">100</span>) <span class="ck">as</span> b:
    <span class="ck">for</span> _, row <span class="ck">in</span> df.iterrows():
        b.put(row[<span class="cs">'product_id'</span>].encode(), {
            <span class="cs">b'info:product_name'</span>:    row[<span class="cs">'product_name'</span>].encode(),
            <span class="cs">b'info:category'</span>:        row[<span class="cs">'category'</span>].encode(),
            <span class="cs">b'stats:total_revenue'</span>:  str(row[<span class="cs">'total_revenue'</span>]).encode(),
            <span class="cs">b'stats:num_orders'</span>:     str(row[<span class="cs">'num_orders'</span>]).encode(),
            <span class="cs">b'meta:last_updated'</span>:    b'2024-01-01',
        })

<span class="cm"># O(1) row-key lookup — no full table scan</span>
row = table.row(<span class="cs">b'PF1CD4'</span>)
print(row[<span class="cs">b'info:product_name'</span>].decode())  <span class="cm"># Regency Cakestand 3 Tier</span>
print(row[<span class="cs">b'stats:total_revenue'</span>].decode()) <span class="cm"># 221310.00</span>

<span class="cm"># Range scan — all products starting with 'PF1'</span>
<span class="ck">for</span> key, data <span class="ck">in</span> table.scan(row_prefix=<span class="cs">b'PF1'</span>):
    print(key.decode(), data[<span class="cs">b'stats:total_revenue'</span>].decode())</pre></div>`,

  hdfs:`<div class="code-card"><div class="code-hdr"><span class="code-title">HDFS Operations — Upload, Verify, Manage</span><span class="code-file">hdfs_ops/hdfs_upload.sh</span><button class="cpbtn" onclick="cp(this)">Copy</button></div><pre><span class="cm">#!/bin/bash
# Check HDFS health</span>
hdfs dfsadmin -report | grep -E <span class="cs">"Live datanodes|Configured Capacity"</span>

<span class="cm"># Create directory structure</span>
hdfs dfs -mkdir -p /retail_platform/raw_data
hdfs dfs -mkdir -p /retail_platform/processed
hdfs dfs -mkdir -p /retail_platform/pig_output
hdfs dfs -mkdir -p /retail_platform/hive_data

<span class="cm"># Upload dataset (780MB → 7 blocks of 128MB)</span>
hdfs dfs -put -f dataset/retail_hdfs_ready.csv /retail_platform/raw_data/retail_data.csv

<span class="cm"># Verify upload</span>
hdfs dfs -ls /retail_platform/raw_data/
hdfs dfs -du -h /retail_platform/raw_data/

<span class="cm"># Check block distribution</span>
hdfs fsck /retail_platform/raw_data/retail_data.csv -files -blocks -locations

<span class="cm"># View first 5 rows</span>
hdfs dfs -cat /retail_platform/raw_data/retail_data.csv | head -5

<span class="cm"># Leave safe mode (if needed)</span>
hdfs dfsadmin -safemode leave</pre></div>`,

  py:`<div class="code-card"><div class="code-hdr"><span class="code-title">preprocess_real_data.py — Full ETL Pipeline</span><span class="code-file">dataset/preprocess_real_data.py</span><button class="cpbtn" onclick="cp(this)">Copy</button></div><pre><span class="ck">import</span> pandas <span class="ck">as</span> pd, numpy <span class="ck">as</span> np, hashlib, os, zipfile

<span class="cm"># 1. Extract Excel from zip</span>
<span class="ck">with</span> zipfile.ZipFile(<span class="cs">'dataset/online_retail_ii.zip'</span>) <span class="ck">as</span> z:
    z.extractall(<span class="cs">'dataset/extracted/'</span>)

<span class="cm"># 2. Load both sheets</span>
df1 = pd.read_excel(<span class="cs">'dataset/extracted/online_retail_II.xlsx'</span>, sheet_name=<span class="cs">'Year 2009-2010'</span>)
df2 = pd.read_excel(<span class="cs">'dataset/extracted/online_retail_II.xlsx'</span>, sheet_name=<span class="cs">'Year 2010-2011'</span>)
df  = pd.concat([df1, df2], ignore_index=True)  <span class="cm"># 1,067,371 rows</span>

<span class="cm"># 3. Clean dirty records</span>
df = df.dropna(subset=[<span class="cs">'Customer ID'</span>, <span class="cs">'Invoice'</span>])
df = df[df[<span class="cs">'Quantity'</span>] &gt; 0]
df = df[df[<span class="cs">'Price'</span>] &gt; 0]
df = df[~df[<span class="cs">'Invoice'</span>].str.startswith(<span class="cs">'C'</span>)]  <span class="cm"># remove returns</span>
<span class="cm"># 805,549 rows remain</span>

<span class="cm"># 4. Feature engineering</span>
df[<span class="cs">'user_id'</span>]     = <span class="cs">'U'</span> + df[<span class="cs">'Customer ID'</span>].astype(int).astype(str)
df[<span class="cs">'product_id'</span>]  = <span class="cs">'PF'</span> + df[<span class="cs">'StockCode'</span>].str[:6].str.upper()
df[<span class="cs">'total_revenue'</span>] = df[<span class="cs">'Price'</span>] * df[<span class="cs">'Quantity'</span>]
df[<span class="cs">'year_month'</span>]  = df[<span class="cs">'InvoiceDate'</span>].dt.strftime(<span class="cs">'%Y-%m'</span>)

<span class="cm"># 5. Scale 10x for Big Data simulation</span>
scaled = pd.concat([df] * <span class="cn">10</span>, ignore_index=True)
scaled = scaled.sample(frac=<span class="cn">1</span>, random_state=<span class="cn">42</span>).reset_index(drop=True)
scaled[<span class="cs">'Price'</span>] *= np.random.uniform(<span class="cn">0.98</span>, <span class="cn">1.02</span>, len(scaled))  <span class="cm"># ±2% noise</span>

<span class="cm"># 6. Save HDFS-ready CSV (no header)</span>
scaled.to_csv(<span class="cs">'dataset/retail_hdfs_ready.csv'</span>, index=False, header=False)
print(f<span class="cs">"Scaled rows: {len(scaled):,} | Size: {os.path.getsize('dataset/retail_hdfs_ready.csv')/1e6:.1f} MB"</span>)</pre></div>`
};

function buildSnippets(){snipBuilt=true;showSnip('pig',document.querySelector('.stab'));}
function showSnip(id,btn){
  document.querySelectorAll('.stab').forEach(t=>t.classList.remove('act'));
  if(btn)btn.classList.add('act');
  document.getElementById('snipContent').innerHTML=SNIPS[id]||'<div style="color:var(--dim);padding:2rem">Coming soon...</div>';
}
function cp(btn){
  const pre=btn.closest('.code-card').querySelector('pre');
  navigator.clipboard.writeText(pre.innerText).then(()=>{btn.textContent='Copied!';setTimeout(()=>btn.textContent='Copy',1500);});
}
