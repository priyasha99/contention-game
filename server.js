// LOCK WARS — a multiplayer game that teaches contention.
//
// Workers race to process jobs, but each job's key maps (by hash) to one of N
// shared locks, and only one worker can hold a given lock at a time. Everyone
// else who needs that lock has to WAIT. Players feel blocking and head-of-line
// stalls, and watch total throughput collapse under contention.
//
// THE DIAL: "shards" = how many locks the work is split across.
//   shards = 1  -> one global lock, everyone fights (max contention)
//   shards = 4  -> work spreads across 4 locks, ~4x less contention
// Run the same players at 1 shard, then 4, and compare the end screens.
//
// Run:  npm install  &&  npm start

const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const CLIENT = fs.readFileSync(path.join(__dirname, "client.html"));

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(CLIENT);
});
const wss = new WebSocketServer({ server });

// ---------------------------------------------------------------------------
// Game state
// ---------------------------------------------------------------------------
const game = {
  phase: "lobby", // lobby | running | ended
  players: new Map(),
  hostId: null,
  shards: 1, // number of locks (the dial)
  locks: [], // [{ holder: id|null, queue: [ids], timer }]
  workPerJob: 5,
  duration: 60,
  maxHoldMs: 8000,
  startTime: 0,
  endTime: 0,
  totalJobs: 0,
  totalBlockedMs: 0,
  throughput: [],
  lastSampleJobs: 0,
};

let nextId = 1;
let tickTimer = null;
let sampleTimer = null;

function makeLocks(n) {
  game.locks = [];
  for (let i = 0; i < n; i++) game.locks.push({ holder: null, queue: [], timer: null });
}
makeLocks(game.shards);

// each job's key hashes to a shard; we simulate that with a fresh random pick
function assignShard() {
  return Math.floor(Math.random() * game.shards);
}

function newPlayer(ws, name) {
  return {
    id: "p" + nextId++,
    ws,
    name: name || "Worker",
    jobsDone: 0,
    blocked: false,
    waitStart: 0,
    totalWait: 0,
    workDone: 0,
    workNeeded: 0,
    holding: false,
    currentShard: 0, // the lock this worker's current job needs
    holdingShard: null,
  };
}

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const p of game.players.values()) if (p.ws.readyState === 1) p.ws.send(msg);
}

function publicState() {
  const now = Date.now();
  const players = [...game.players.values()]
    .map((p) => {
      const liveWait = p.blocked ? p.totalWait + (now - p.waitStart) : p.totalWait;
      let queuePos = -1;
      if (p.blocked) queuePos = game.locks[p.currentShard].queue.indexOf(p.id);
      return {
        id: p.id,
        name: p.name,
        jobsDone: p.jobsDone,
        blocked: p.blocked,
        holding: p.holding,
        waitMs: Math.round(liveWait),
        currentShard: p.currentShard,
        holdingShard: p.holdingShard,
        queuePos,
        workDone: p.workDone,
        workNeeded: p.workNeeded,
      };
    })
    .sort((a, b) => b.jobsDone - a.jobsDone);

  let liveBlockedTotal = game.totalBlockedMs;
  let blockedCount = 0;
  for (const p of game.players.values()) {
    if (p.blocked) {
      liveBlockedTotal += now - p.waitStart;
      blockedCount++;
    }
  }

  const locks = game.locks.map((l, i) => {
    const h = l.holder ? game.players.get(l.holder) : null;
    return { i, holder: h ? { id: h.id, name: h.name } : null, queueLen: l.queue.length };
  });

  const timeLeft =
    game.phase === "running" ? Math.max(0, Math.ceil((game.endTime - now) / 1000))
    : game.phase === "ended" ? 0 : game.duration;

  return {
    type: "state",
    phase: game.phase,
    hostId: game.hostId,
    locks,
    blockedCount,
    players,
    totalJobs: game.totalJobs,
    totalBlockedMs: Math.round(liveBlockedTotal),
    throughput: game.throughput,
    timeLeft,
    settings: { workPerJob: game.workPerJob, duration: game.duration, shards: game.shards },
  };
}
const pushState = () => broadcast(publicState());

// ---------------------------------------------------------------------------
// Lock mechanics (per shard)
// ---------------------------------------------------------------------------
function grant(p, s) {
  if (p.blocked) {
    p.totalWait += Date.now() - p.waitStart;
    p.blocked = false;
  }
  const lock = game.locks[s];
  lock.holder = p.id;
  p.holding = true;
  p.holdingShard = s;
  p.workDone = 0;
  p.workNeeded = game.workPerJob;

  clearTimeout(lock.timer);
  lock.timer = setTimeout(() => {
    if (lock.holder === p.id) {
      broadcast({ type: "event", text: `${p.name} held Lock #${s + 1} too long — forced release (timeout).` });
      release(p);
    }
  }, game.maxHoldMs);
}

function release(p) {
  const s = p.holdingShard;
  if (s == null) return;
  const lock = game.locks[s];
  if (lock.holder !== p.id) return;
  p.holding = false;
  p.holdingShard = null;
  lock.holder = null;
  clearTimeout(lock.timer);
  p.currentShard = assignShard(); // next job hashes to a new lock

  while (lock.queue.length) {
    const nid = lock.queue.shift();
    const np = game.players.get(nid);
    if (np) {
      grant(np, s);
      break;
    }
  }
  pushState();
}

function requestLock(p) {
  if (game.phase !== "running") return;
  if (p.holding || p.blocked) return;
  const s = p.currentShard;
  const lock = game.locks[s];
  if (lock.holder === null && lock.queue.length === 0) {
    grant(p, s);
  } else {
    p.blocked = true;
    p.waitStart = Date.now();
    lock.queue.push(p.id);
  }
  pushState();
}

function doWork(p) {
  if (game.phase !== "running" || !p.holding) return;
  const lock = game.locks[p.holdingShard];
  if (!lock || lock.holder !== p.id) return;
  p.workDone++;
  if (p.workDone >= p.workNeeded) {
    p.jobsDone++;
    game.totalJobs++;
    broadcast({ type: "event", text: `${p.name} finished a job and released Lock #${p.holdingShard + 1}.` });
    release(p);
  } else {
    pushState();
  }
}

// ---------------------------------------------------------------------------
// Round lifecycle
// ---------------------------------------------------------------------------
function startRound() {
  if (game.phase === "running") return;
  makeLocks(game.shards);
  game.phase = "running";
  game.totalJobs = 0;
  game.totalBlockedMs = 0;
  game.throughput = [];
  game.lastSampleJobs = 0;
  for (const p of game.players.values()) {
    p.jobsDone = 0;
    p.blocked = false;
    p.totalWait = 0;
    p.holding = false;
    p.holdingShard = null;
    p.workDone = 0;
    p.workNeeded = 0;
    p.currentShard = assignShard();
  }
  game.startTime = Date.now();
  game.endTime = game.startTime + game.duration * 1000;

  clearInterval(tickTimer);
  clearInterval(sampleTimer);
  tickTimer = setInterval(() => {
    if (Date.now() >= game.endTime) endRound();
    else pushState();
  }, 250);
  sampleTimer = setInterval(() => {
    const elapsed = Math.round((Date.now() - game.startTime) / 1000);
    game.throughput.push({ t: elapsed, jobs: game.totalJobs - game.lastSampleJobs });
    game.lastSampleJobs = game.totalJobs;
  }, 1000);

  broadcast({ type: "event", text: `Round started with ${game.shards} lock(s)! Grab the lock your job needs.` });
  pushState();
}

function endRound() {
  game.phase = "ended";
  clearInterval(tickTimer);
  clearInterval(sampleTimer);
  game.locks.forEach((l) => clearTimeout(l.timer));
  const now = Date.now();
  for (const p of game.players.values()) {
    if (p.blocked) {
      p.totalWait += now - p.waitStart;
      p.blocked = false;
    }
    p.holding = false;
    p.holdingShard = null;
  }
  game.totalBlockedMs = [...game.players.values()].reduce((s, p) => s + p.totalWait, 0);
  makeLocks(game.shards);
  pushState();
}

function resetToLobby() {
  game.phase = "lobby";
  clearInterval(tickTimer);
  clearInterval(sampleTimer);
  game.locks.forEach((l) => clearTimeout(l.timer));
  makeLocks(game.shards);
  game.totalJobs = 0;
  game.totalBlockedMs = 0;
  game.throughput = [];
  for (const p of game.players.values()) {
    p.jobsDone = 0;
    p.blocked = false;
    p.totalWait = 0;
    p.holding = false;
    p.holdingShard = null;
  }
  pushState();
}

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------
wss.on("connection", (ws) => {
  let player = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    switch (msg.type) {
      case "join": {
        player = newPlayer(ws, String(msg.name || "Worker").slice(0, 16));
        game.players.set(player.id, player);
        if (!game.hostId) game.hostId = player.id;
        ws.send(JSON.stringify({ type: "joined", id: player.id, hostId: game.hostId }));
        broadcast({ type: "event", text: `${player.name} joined.` });
        pushState();
        break;
      }
      case "request":
        if (player) requestLock(player);
        break;
      case "work":
        if (player) doWork(player);
        break;
      case "start":
        if (player && player.id === game.hostId) startRound();
        break;
      case "reset":
        if (player && player.id === game.hostId) resetToLobby();
        break;
      case "settings":
        if (player && player.id === game.hostId && game.phase === "lobby") {
          if (msg.workPerJob) game.workPerJob = Math.max(1, Math.min(20, msg.workPerJob | 0));
          if (msg.duration) game.duration = Math.max(15, Math.min(300, msg.duration | 0));
          if (msg.shards) {
            game.shards = Math.max(1, Math.min(8, msg.shards | 0));
            makeLocks(game.shards);
          }
          pushState();
        }
        break;
    }
  });

  ws.on("close", () => {
    if (!player) return;
    if (player.holding) release(player);
    game.locks.forEach((l) => {
      l.queue = l.queue.filter((id) => id !== player.id);
    });
    game.players.delete(player.id);
    if (game.hostId === player.id) {
      game.hostId = game.players.size ? [...game.players.keys()][0] : null;
    }
    broadcast({ type: "event", text: `${player.name} left.` });
    pushState();
  });
});

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
server.listen(PORT, () => {
  const nets = os.networkInterfaces();
  const urls = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === "IPv4" && !net.internal) urls.push(`http://${net.address}:${PORT}`);
    }
  }
  console.log("\n  🔒  LOCK WARS is running!\n");
  console.log(`  On this machine:   http://localhost:${PORT}`);
  if (urls.length) {
    console.log("  Share with juniors (same Wi-Fi):");
    urls.forEach((u) => console.log("     " + u));
  }
  console.log("\n  First person to join is the host and can start the round.\n");
});
