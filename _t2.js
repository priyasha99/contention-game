const { WebSocket } = require('ws');
const URL='ws://localhost:3000'; let maxC=0;
function mk(name){return new Promise(res=>{const ws=new WebSocket(URL);const c={ws,name,id:null,isHost:false};
ws.on('open',()=>ws.send(JSON.stringify({type:'join',name})));
ws.on('message',d=>{const m=JSON.parse(d);
if(m.type==='joined'){c.id=m.id;c.isHost=(m.id===m.hostId);res(c);}
if(m.type==='state'&&m.phase==='running'){maxC=Math.max(maxC,m.players.filter(p=>p.holding).length);
if(m.holder&&m.holder.id===c.id)ws.send(JSON.stringify({type:'work'}));
else{const me=m.players.find(p=>p.id===c.id);if(me&&me.queuePos<0)ws.send(JSON.stringify({type:'request'}));}}
if(m.type==='state'&&m.phase==='ended')c.final=m;});
c.send=o=>ws.send(JSON.stringify(o));return c;});}
(async()=>{const a=await mk('Aaa'),b=await mk('Bbb'),d=await mk('Ccc');
const host=[a,b,d].find(x=>x.isHost);
host.send({type:'settings',workPerJob:3,duration:4});host.send({type:'start'});
await new Promise(r=>setTimeout(r,7000));
const fin=[a,b,d].map(x=>x.final).find(Boolean);
if(!fin){console.log('no final state ❌');process.exit(1);}
console.log('maxConcurrentHolders =',maxC,'(must be 1)');
console.log('totalJobs =',fin.totalJobs);
console.log('totalBlockedMs =',fin.totalBlockedMs);
console.log('board =',fin.players.map(p=>p.name+':'+p.jobsDone+'j/'+(p.waitMs/1000).toFixed(1)+'s').join(' | '));
console.log((maxC===1&&fin.totalJobs>0&&fin.totalBlockedMs>0)?'PASS ✅':'FAIL ❌');process.exit(0);})();
