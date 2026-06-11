const { WebSocket } = require('ws');
const URL='ws://localhost:3000'; let maxC=0; const seen={};
function mk(name){return new Promise(res=>{const ws=new WebSocket(URL);
const c={ws,name,id:null,isHost:false,final:null,last:null};
ws.on('open',()=>ws.send(JSON.stringify({type:'join',name})));
ws.on('message',d=>{const m=JSON.parse(d);
 if(m.type==='joined'){c.id=m.id;c.isHost=(m.id===m.hostId);res(c);}
 if(m.type==='state'){c.last=m;seen[m.phase]=1;
   if(m.phase==='running')maxC=Math.max(maxC,m.players.filter(p=>p.holding).length);
   if(m.phase==='ended')c.final=m;}});
c.send=o=>ws.send(JSON.stringify(o));return c;});}
(async()=>{const a=await mk('Aaa'),b=await mk('Bbb'),d=await mk('Ccc');
const all=[a,b,d];const host=all.find(x=>x.isHost);
host.send({type:'settings',workPerJob:3,duration:4});host.send({type:'start'});
// human-paced clicking: every 120ms each bot acts once
const iv=setInterval(()=>{for(const c of all){const m=c.last;if(!m||m.phase!=='running')continue;
  if(m.holder&&m.holder.id===c.id)c.send({type:'work'});
  else{const me=m.players.find(p=>p.id===c.id);if(me&&me.queuePos<0)c.send({type:'request'});}}},120);
await new Promise(r=>setTimeout(r,6500));clearInterval(iv);
console.log('phases seen:',Object.keys(seen).join(','));
console.log('maxConcurrentHolders =',maxC,'(must be 1)');
const fin=all.map(x=>x.final).find(Boolean);
if(!fin){console.log('FAIL ❌ no final');process.exit(1);}
console.log('totalJobs =',fin.totalJobs,'| totalBlockedMs =',fin.totalBlockedMs);
console.log('board =',fin.players.map(p=>p.name+':'+p.jobsDone+'j/'+(p.waitMs/1000).toFixed(1)+'s').join(' | '));
console.log((maxC===1&&fin.totalJobs>0&&fin.totalBlockedMs>0)?'PASS ✅':'FAIL ❌');
process.exit(0);})();
