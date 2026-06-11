const { WebSocket } = require('ws');
const URL='ws://localhost:3000'; const SHARDS=+process.argv[2]||1;
let violation=0;
function mk(name){return new Promise(res=>{const ws=new WebSocket(URL);
const c={ws,name,id:null,isHost:false,final:null,last:null};
ws.on('open',()=>ws.send(JSON.stringify({type:'join',name})));
ws.on('message',d=>{const m=JSON.parse(d);
 if(m.type==='joined'){c.id=m.id;c.isHost=(m.id===m.hostId);res(c);}
 if(m.type==='state'){c.last=m;
  if(m.phase==='running'){const byShard={};m.players.filter(p=>p.holding).forEach(p=>{byShard[p.holdingShard]=(byShard[p.holdingShard]||0)+1;});
   for(const k in byShard)if(byShard[k]>1)violation++;}
  if(m.phase==='ended')c.final=m;}});
c.send=o=>ws.send(JSON.stringify(o));return c;});}
(async()=>{const all=[await mk('Aaa'),await mk('Bbb'),await mk('Ccc'),await mk('Ddd')];
const host=all.find(x=>x.isHost);
host.send({type:'settings',workPerJob:3,duration:15,shards:SHARDS});
host.send({type:'start'});
const iv=setInterval(()=>{for(const c of all){const m=c.last;if(!m||m.phase!=='running')continue;
  if(m.holdingShardSelf){} const me=m.players.find(p=>p.id===c.id);
  if(me&&me.holdingShard!=null)c.send({type:'work'});
  else if(me&&me.queuePos<0)c.send({type:'request'});}},110);
await new Promise(r=>setTimeout(r,17000));clearInterval(iv);
const fin=all.map(x=>x.final).find(Boolean);
console.log(`shards=${SHARDS} | mutexViolations=${violation} | jobs=${fin.totalJobs} | blockedMs=${fin.totalBlockedMs} | blockedSecs=${(fin.totalBlockedMs/1000).toFixed(1)}`);
process.exit(0);})();
