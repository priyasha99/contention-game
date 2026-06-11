// LOCK WARS — a multiplayer game that teaches contention.
//
// One shared lock. Many workers. Only one worker can hold the lock at a time.
// Everyone else who wants it has to WAIT. Players feel blocking, head-of-line
// stalls, and watch total throughput collapse as more people contend.
//
// Run:  npm install  &&  npm start
// Then open the printed URL and share your LAN URL with your juniors.

const http = require("http");
const fs = require("fs");
const path = require("path");
const os = require("os");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const CLIENT = fs.readFileSync(path.join(__dirname, "client.html"));

// ---------------------------------------------------------------------------
// HTTP server: just serves the single-page client.
// ---------------------------------------------------------------------------
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
  players: new Map(), // id -> player
  hostId: null,
  lockHolder: null, // id of player holding the lock, or null
  queue: [], // ids waiting for the lock (FIFO)
  workPerJob: 5, // clicks needed inside the critical section
  duration: 60, // round length in seconds
  maxHoldMs: 8000, // auto-release a stalled holder (timeout)
  startTime: 0,
  endTime: 0,
  totalJobs: 0,
  totalBlockedMs: 0, // sum of all time any worker spent blocked = contention cost
  throughput: [], // [{t, jobs}] sampled per second for the live chart
  holdTimer: null,
  lastSampleJobs: 0,
};

let nextId = 1;

function newPlayer(ws, name) {
  return {
    id: "p" + nextId++,
    ws,
    name: name || "Worker",
    jobsDone: 0,
    blocked: false,
    waitStart: 0,
    totalWait: 0, // ms this player spent blocked
    workDone: 0,
    workNeeded: 0,
    holding: false,
  };
}

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const p of game.players.values()) {
    if (p.ws.readyState === 1) p.ws.send(msg);
  }
}

function publicState() {
  const now = Date.now();
  const holder = game.lockHolder ? game.players.get(game.lockHolder) : null;
  const players = [...game.players.values()]
    .map((p) => {
      // include in-progress wait for live display
      const liveWait = p.blocked ? p.totalWait + (now - p.waitStart) : p.totalWait;
      return {
        id: p.id,
        name: p.name,
        jobsDone: p.jobsDone,
        blocked: p.blocked,
        holding: p.holding,
        waitMs: Math.round(liveWait),
        queuePos: game.queue.indexOf(p.id), // -1 if not queued
        workDone: p.workDone,
        workNeeded: p.workNeeded,
      };
    })
    .sort((a, b) => b.jobsDone - a.jobsDone);

  let liveBlockedTotal = game.totalBlockedMs;
  for (const p of game.players.values()) {
    if (p.blocked) liveBlockedTotal += now - p.waitStart;
  }

  const timeLeft =
    game.phase === "running"
      ? Math.max(0, Math.ceil((game.endTime - now) / 1000))
      : game.phase === "ended"
      ? 0
      : game.duration;

  return {
    type: "state",
    phase: game.phase,
    hostId: game.hostId,
    holder: holder ? { id: holder.id, name: holder.name } : null,
    queueLength: game.queue.length,
    blockedCount: game.queue.length,
    players,
    totalJobs: game.totalJobs,
    totalBlockedMs: Math.round(liveBlockedTotal),
    throughput: game.throughput,
    timeLeft,
    settings: { workPerJob: game.workPerJob, duration: game.duration },
  };
}

function pushState() {
  broadcast(publicState());
}

// ---------------------------------------------------------------------------
// Lock mechanics
// ---------------------------------------------------------------------------
function grant(p) {
  if (p.blocked) {
    p.totalWait += Date.now() - p.waitStart;
    p.blocked = false;
  }
  game.lockHolder = p.id;
  p.holding = true;
  p.workDone = 0;
  p.workNeeded = game.workPerJob;

  clearTimeout(game.holdTimer);
  game.holdTimer = setTimeout(() => {
    // Held too long -> forced release (a stalled critical section blocks everyone).
    if (game.lockHolder === p.id) {
      broadcast({ type: "event", text: `${p.name} held the lock too long — forced release (timeout).` });
      release(p, false);
    }
  }, game.maxHoldMs);
}

function release(p, completed) {
  if (game.lockHolder !== p.id) return;
  p.holding = false;
  p.workDone = 0;
  p.workNeeded = 0;
  game.lockHolder = null;
  clearTimeout(game.holdTimer);

  // hand off to the next waiter (FIFO)
  while (game.queue.length) {
    const nextId = game.queue.shift();
    const np = game.players.get(nextId);
    if (np) {
      grant(np);
      break;
    }
  }
  pushState();
}

function requestLock(p) {
  if (game.phase !== "running") return;
  if (game.lockHolder === p.id) return; // already holding
  if (p.blocked) return; // already queued
  if (game.lockHolder === null && game.queue.length === 0) {
    grant(p);
  } else {
    p.blocked = true;
    p.waitStart = Date.now();
    game.queue.push(p.id);
  }
  pushState();
}

function doWork(p) {
  if (game.phase !== "running") return;
  if (game.lockHolder !== p.id) return; // can only work while holding the lock
  p.workDone++;
  if (p.workDone >= p.workNeeded) {
    p.jobsDone++;
    game.totalJobs++;
    broadcast({ type: "event", text: `${p.name} finished a job and released the lock.` });
    release(p, true);
  } else {
    pushState();
  }
}

// ---------------------------------------------------------------------------
// Round lifecycle
// ---------------------------------------------------------------------------
let tickTimer = null;
let sampleTimer = null;

function startRound() {
  if (game.phase === "running") return;
  game.phase = "running";
  game.totalJobs = 0;
  game.totalBlockedMs = 0;
  game.throughput = [];
  game.lastSampleJobs = 0;
  game.lockHolder = null;
  game.queue = [];
  for (const p of game.players.values()) {
    p.jobsDone = 0;
    p.blocked = false;
    p.totalWait = 0;
    p.holding = false;
    p.workDone = 0;
    p.workNeeded = 0;
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

  broadcast({ type: "event", text: "Round started! Grab the lock and process jobs." });
  pushState();
}

function endRound() {
  game.phase = "ended";
  clearInterval(tickTimer);
  clearInterval(sampleTimer);
  clearTimeout(game.holdTimer);
  // close out any in-progress blocked time
  const now = Date.now();
  for (const p of game.players.values()) {
    if (p.blocked) {
      p.totalWait += now - p.waitStart;
      p.blocked = false;
    }
    p.holding = false;
  }
  game.totalBlockedMs = [...game.players.values()].reduce((s, p) => s + p.totalWait, 0);
  game.lockHolder = null;
  game.queue = [];
  pushState();
}

function resetToLobby() {
  game.phase = "lobby";
  clearInterval(tickTimer);
  clearInterval(sampleTimer);
  clearTimeout(game.holdTimer);
  game.lockHolder = null;
  game.queue = [];
  game.totalJobs = 0;
  game.totalBlockedMs = 0;
  game.throughput = [];
  for (const p of game.players.values()) {
    p.jobsDone = 0;
    p.blocked = false;
    p.totalWait = 0;
    p.holding = false;
  }
  pushState();
}

// ---------------------------------------------------------------------------
// Connection handling
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
          pushState();
        }
        break;
    }
  });

  ws.on("close", () => {
    if (!player) return;
    const wasHolder = game.lockHolder === player.id;
    game.players.delete(player.id);
    game.queue = game.queue.filter((id) => id !== player.id);
    if (wasHolder) {
      game.lockHolder = null;
      release(player, false); // hand lock to next waiter
    }
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
