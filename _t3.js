const { WebSocket } = require('ws');
const URL='ws://localhost:3000';
const seen={};
function mk(name){return new Promise(res=>{const ws=new WebSocket(URL);const c={ws,name,id:null,isHost:false,final:null};
ws.on('error',e=>console.log(name,'ERR',e.message));
ws.on('close',()=>console.log(name,'closed'));
ws.on('open',()=>ws.send(JSON.stringify({type:'join',name})));
ws.on('message',d=>{const m=JSON.parse(d);
if(m.type==='joined'){c.id=m.id;c.isHost=(m.id===m.hostId);res(c);}
if(m.type==='state'){seen[m.phase]=(seen[m.phase]||0)+1;
 if(m.phase==='running'){if(m.holder&&m.holder.id===c.id)ws.send(JSON.stringify({type:'work'}));
 else{const me=m.players.find(p=>p.id===c.id);if(me&&me.queuePos<0)ws.send(JSON.stringify({type:'request'}));}}
 if(m.phase==='ended')c.final=m;}});
c.send=o=>ws.send(JSON.stringify(o));return c;});}
(async()=>{const a=await mk('Aaa'),b=await mk('Bbb'),d=await mk('Ccc');
console.log('joined. host=',[a,b,d].find(x=>x.isHost).name);
const host=[a,b,d].find(x=>x.isHost);
host.send({type:'settings',workPerJob:3,duration:4});host.send({type:'start'});
await new Promise(r=>setTimeout(r,7000));
console.log('phases seen:',seen);
const fin=[a,b,d].map(x=>x.final).find(Boolean);
console.log('final?',!!fin, fin&&('jobs='+fin.totalJobs+' blockedMs='+fin.totalBlockedMs));
process.exit(0);})();
