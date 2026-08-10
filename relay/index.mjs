/* ---------- Padel Check Mate relay server v1.6 ----------
   v1.6: true web push — a player with no open stream gets "your move" on
   their phone (VAPID keys persisted, per-player subscriptions, 90s
   throttle, dead subscriptions pruned). Optional dep: web-push; the
   server runs fine without it (push silently disabled).
   v1.5: silent-disconnect detection (left{lost:true} after a 45s grace,
   back{} on return), blitz flag in room meta/lobby/join, and quick-match
   pairing that prefers the same pace.
   Zero-dependency Node server for online turn-based matches.
   Rooms are 5-char codes; two players exchange turn payloads;
   delivery is Server-Sent Events (SSE) — no websockets, no npm
   installs, deployable anywhere Node ≥18 runs.

   v1.1: rooms persist to a JSON file across restarts, per-IP rate
   limiting, turn-payload validation, and a quick-match queue that
   auto-pairs strangers.
   v1.4: tokens move OUT of GET query strings (where server/proxy logs
   collect them). state/history accept `Authorization: Bearer <token>`;
   the SSE stream — which cannot send headers — uses a one-time 60s
   ticket minted via POST. Query-string tokens still work everywhere
   for older clients.

   Run:        node server/index.mjs        (port 8787, or $PORT;
                                             data file $DATA_FILE or
                                             ./relay-data.json)
   Health:     GET  /
   Create:     POST /api/rooms                {name} → {code, player:0, token}
   Quickmatch: POST /api/quickmatch           {name} → create-or-pair
   Join:       POST /api/rooms/:code/join     {name} → {player:1, token, names}
   Turn:       POST /api/rooms/:code/turn     {token, payload}
   Ticket:     POST /api/rooms/:code/ticket   (Bearer token) → {ticket, ttl}
   Stream:     GET  /api/rooms/:code/stream?ticket=…  (SSE: joined | turn | bye;
                                              legacy ?token=… still accepted)
   State:      GET  /api/rooms/:code/state    (Bearer token; legacy ?token=…)  */
import http from "node:http";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const PORT = process.env.PORT || 8787;
const DATA_FILE = process.env.DATA_FILE
  || path.join(path.dirname(fileURLToPath(import.meta.url)), "relay-data.json");
const ROOM_TTL_MS = 24 * 60 * 60 * 1000;   // idle rooms purged after 24h
const QUEUE_TTL_MS = 5 * 60 * 1000;        // quick-match slots live 5 min
const MAX_PAYLOAD = 64 * 1024;         // raised for pack publishing             // per-turn payload cap
/* v1.4.1: presence/aim streaming means a LIVELY match sends far more than
   the old 120/min — and two phones on one home NAT share an IP bucket.
   600/min absorbs two chatty players with headroom; creates stay tight. */
const LOST_GRACE_MS = 45000;  // stream gone this long without an explicit leave -> tell the rival
/* v1.7: the per-IP ceiling was 600/min, written when a room was two people
   who were rarely on the same network. Tournament night breaks that
   assumption completely: 3-8 phones in the same building share one public
   IP, and a live rally posts a turn per strike — so a full tournament room
   on one WiFi exceeded the limit and started getting 429s mid-match. The
   payloads are small and capped (MAX_PAYLOAD), and the abuse vector that
   actually matters — room creation — has its own much tighter createMax,
   which is unchanged. */
const RATE = { windowMs: 60 * 1000, max: 4000, createMax: 15, createWindowMs: 60 * 60 * 1000 };

const rooms = new Map();   // code → room
let boards = [];           // global leaderboard entries (capped)
const packLib = new Map(); // packId → published community pack
const catalog = new Map(); // itemId → marketplace item (admin-managed)
const queue = [];          // quick-match waiting rooms: {code, ts}
const errLog = [];         // R76: field error reports (ring buffer, deduped)
const rate = new Map();    // ip → {n, t0, created, c0}
const ADMIN_KEY = process.env.ADMIN_KEY || "change-me"; // set on deploy!
/* v1.6: web push — optional dependency, degrade gracefully without it */
let webpush = null;
try { webpush = (await import("web-push")).default; }
catch (e) { console.log("ℹ web-push not installed — push notifications disabled"); }
let vapidKeys = null;

/* ---------- persistence: best-effort JSON snapshot ---------- */
let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try {
      const snap = [...rooms.values()].map((r) => ({
        code: r.code, created: r.created, lastSeen: r.lastSeen, turns: r.turns,
        pub: r.pub, qm: r.qm, iq: r.iq, fmt: r.fmt, blz: !!r.blz, lv: !!r.lv,
        /* v1.7: this snapshot is an explicit field list, not a spread — a
           tournament room that lost `max` and `tour` across a restart would
           come back as an ordinary pair with no bracket and no way in. */
        max: r.max || 2, tour: r.tour || null,
        players: r.players.map((p) => ({ idx: p.idx, name: p.name, token: p.token, left: !!p.left, push: p.push || null })),
      }));
      fs.writeFileSync(DATA_FILE, JSON.stringify({ v: 1, vapid: vapidKeys, rooms: snap, boards, packs: [...packLib.values()], catalog: [...catalog.values()] }));
    } catch (e) { console.error("save failed:", e.message); }
  }, 2000);
  saveTimer.unref && saveTimer.unref();
}
try {
  if (fs.existsSync(DATA_FILE)) {
    const snap = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    (snap.rooms || []).forEach((r) => rooms.set(r.code, {
      ...r, players: r.players.map((p) => ({ ...p, streams: new Set() })),
    }));
    boards = Array.isArray(snap.boards) ? snap.boards : [];
    vapidKeys = snap.vapid || null;
    (snap.packs || []).forEach((p) => packLib.set(p.id, p));
    (snap.catalog || []).forEach((c) => catalog.set(c.id, c));
    console.log(`↻ restored ${rooms.size} room(s) from ${DATA_FILE}`);
  }
} catch (e) { console.error("restore failed:", e.message); }
if (webpush) {
  if (!vapidKeys || !vapidKeys.publicKey) { vapidKeys = webpush.generateVAPIDKeys(); scheduleSave(); }
  webpush.setVapidDetails("mailto:battle-padel@relay.local", vapidKeys.publicKey, vapidKeys.privateKey);
  console.log("🔔 web push armed");
}

/* v1.6: push "your move" to a player who has NO open stream — throttled,
   playable stages only, dead subscriptions pruned on 404/410 */
function maybePush(room, me, other, payload) {
  if (!webpush || !other || !other.push || other.streams.size > 0) return;
  if (!["serve", "rally", "end"].includes(payload.stage)) return;
  const now = Date.now();
  if (other.lastPushAt && now - other.lastPushAt < 90000) return;
  other.lastPushAt = now;
  webpush.sendNotification(other.push, JSON.stringify({
    t: payload.stage === "end" ? "🏁 Match over" : "🎾 Your move!",
    b: `${me.name} played — room ${room.code}`,
    code: room.code,
  }), { TTL: 3600 }).catch((e) => {
    if (e && (e.statusCode === 404 || e.statusCode === 410)) { other.push = null; scheduleSave(); }
    else console.log("push send failed:", (e && (e.statusCode || e.message)) || e);
  });
}

/* ---------- helpers ---------- */
const code5 = () => {
  const abc = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"; // no confusable chars
  let s = "";
  for (let i = 0; i < 5; i++) s += abc[crypto.randomInt(abc.length)];
  return rooms.has(s) ? code5() : s;
};

function limited(req, isCreate) {
  const ip = (req.headers["x-forwarded-for"] || req.socket.remoteAddress || "?").split(",")[0].trim();
  const now = Date.now();
  let r = rate.get(ip);
  if (!r) { r = { n: 0, t0: now, created: 0, c0: now }; rate.set(ip, r); }
  if (now - r.t0 > RATE.windowMs) { r.n = 0; r.t0 = now; }
  if (now - r.c0 > RATE.createWindowMs) { r.created = 0; r.c0 = now; }
  r.n++;
  if (isCreate) r.created++;
  return r.n > RATE.max || (isCreate && r.created > RATE.createMax);
}

function sse(res) {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Access-Control-Allow-Origin": "*",
  });
  res.write(":ok\n\n");
}

function emitTo(player, event, data) {
  const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  player.streams.forEach((s) => { try { s.write(msg); } catch (e) { /* gone */ } });
}

function emit(room, event, data) {
  room.players.forEach((p) => emitTo(p, event, data));
}

function json(res, status, obj) {
  res.writeHead(status, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
  res.end(JSON.stringify(obj));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let b = "";
    req.on("data", (c) => { b += c; if (b.length > MAX_PAYLOAD) { reject(new Error("too big")); req.destroy(); } });
    req.on("end", () => { try { resolve(b ? JSON.parse(b) : {}); } catch (e) { reject(e); } });
    req.on("error", reject);
  });
}

const playerByToken = (room, token) => room.players.find((p) => p.token === token);
/* v1.4: token from the Authorization header (preferred) or query (legacy) */
const bearerOf = (req) => {
  const h = req.headers.authorization || "";
  const m = /^Bearer\s+(.+)$/i.exec(h);
  return m ? m[1].trim() : null;
};
const tokenOf = (req, url) => bearerOf(req) || url.searchParams.get("token");
/* one-time stream tickets: EventSource can't send headers, so the client
   trades its token (in a header) for a short-lived single-use ticket that
   is safe to put in the stream URL — a leaked ticket is dead in 60s and
   dies on first use anyway. Kept per-player, purged on every mint. */
const TICKET_TTL = 60 * 1000;
function mintTicket(player) {
  player.tix = player.tix || new Map();
  const now = Date.now();
  for (const [t, exp] of player.tix) if (exp < now) player.tix.delete(t);
  while (player.tix.size >= 8) player.tix.delete(player.tix.keys().next().value);
  const t = crypto.randomBytes(12).toString("hex");
  player.tix.set(t, now + TICKET_TTL);
  return t;
}
function playerByTicket(room, ticket) {
  if (!ticket) return null;
  const now = Date.now();
  for (const p of room.players) {
    if (p.tix && p.tix.has(ticket)) {
      const exp = p.tix.get(ticket);
      p.tix.delete(ticket);              // single use
      if (exp >= now) return p;
    }
  }
  return null;
}
/* a waiting room is only advertised while its creator is connected
   (small grace window covers brief reconnects) */
const creatorLive = (r) => r.players[0] && !r.players[0].left
  && (r.players[0].streams.size > 0 || Date.now() - r.created < 120000);

function makeRoom(name, opts = {}) {
  const code = code5();
  const token = crypto.randomBytes(12).toString("hex");
  rooms.set(code, {
    code, created: Date.now(), lastSeen: Date.now(), turns: [],
    pub: !!opts.pub, qm: !!opts.qm, iq: Number(opts.iq) || null, blz: !!opts.blz, lv: !!opts.lv,
    fmt: typeof opts.fmt === "string" ? opts.fmt.slice(0, 12) : null,
    /* v1.7: a room holds more than two only when it is a tournament room.
       Ordinary match rooms keep the old cap by default, so nothing that
       relied on "a room is a pair" changes behaviour. */
    max: Math.max(2, Math.min(TOUR_MAX, Number(opts.max) || 2)),
    tour: opts.tour || null,
    players: [{ idx: 0, name: String(name || "Player 1").slice(0, 14), token, streams: new Set() }],
  });
  scheduleSave();
  return { code, token };
}

function joinRoom(room, name) {
  const token = crypto.randomBytes(12).toString("hex");
  /* v1.7: seat index is the seat, not the constant 1 — a tournament room
     seats up to TOUR_MAX. */
  const idx = room.players.length;
  const player = { idx, name: String(name || `Player ${idx + 1}`).slice(0, 14), token, streams: new Set() };
  room.players.push(player);
  room.lastSeen = Date.now();
  emitTo(room.players[0], "joined", { name: player.name });
  if (room.tour) emit(room, "roster", { players: room.players.map((p) => p.name) });
  scheduleSave();
  return { token, idx, names: room.players.map((p) => p.name) };
}

/* ---------- v1.7: TOURNAMENT ROOMS ----------
   One code, 3-8 phones. The server owns the bracket so every phone agrees
   on it without any phone being the source of truth.

   A pairing is NOT played inside the tournament room. The server spawns an
   ordinary two-player room per pairing and hands each of the two players
   its code and their own token privately. That is the whole trick: the live
   rally engine, the strike contract, reconnection, the lot, are reused
   untouched — a tournament match IS a normal live match that happened to be
   arranged for you. */
const TOUR_MAX = 8;
const TOUR_MIN = 3;
/* v1.7.1 (R115): a pairing must not be able to hang the bracket. A side
   with no open stream on its match room for this long forfeits — phone
   died, app killed, wandered off to the bar. Env-tunable so the test suite
   does not have to wait two real minutes. */
const TOUR_FORFEIT_MS = Number(process.env.TOUR_FORFEIT_MS) || 120000;
const TOUR_SWEEP_MS = Number(process.env.TOUR_SWEEP_MS) || 15000;

/* my unfinished pairing in the CURRENT round, with my child-room seat and
   token recovered from the child room itself (the invite carries no state
   the rooms don't already hold — which is what makes it re-derivable).
   This is the answer to the fire-and-forget invite: a phone that slept
   through the SSE event asks for this instead. */
function tourMineFor(room, me) {
  const t = room.tour;
  if (!t || t.state !== "running") return null;
  for (let mi = 0; mi < t.matches.length; mi++) {
    const m = t.matches[mi];
    if (m.winner != null || !m.code) continue;
    if (m.a !== me.idx && m.b !== me.idx) continue;
    const kid = rooms.get(m.code);
    if (!kid) continue;
    const seat = m.a === me.idx ? 0 : 1;         // tourSpawn seats a as creator
    const kp = kid.players[seat];
    if (!kp) continue;
    const opp = room.players[seat === 0 ? m.b : m.a];
    return { mi, round: t.round, code: m.code, token: kp.token, me: seat, opp: opp ? opp.name : "?" };
  }
  return null;
}

/* settle a pairing from outside a played match (forfeit / abandonment).
   winnerIdx is the ROOM player index. */
function tourSettle(room, m, winnerIdx, why) {
  m.winner = winnerIdx;
  m.code = null;
  m.why = why || null;
  tourAdvance(room);
  emit(room, "bracket", tourPublic(room));
  scheduleSave();
}

/* Round-robin by the circle method, so every round is a set of pairs that
   can all be played AT THE SAME TIME — which is the point of everyone
   having their own phone. A naive all-pairs list would put one player in
   several simultaneous matches. */
function rrRounds(n) {
  const ids = [...Array(n).keys()];
  if (ids.length % 2) ids.push(-1);            // -1 = the bye seat
  const m = ids.length, out = [];
  for (let r = 0; r < m - 1; r++) {
    const pairs = [];
    for (let i = 0; i < m / 2; i++) {
      const a = ids[i], b = ids[m - 1 - i];
      if (a !== -1 && b !== -1) pairs.push({ a, b, winner: null });
    }
    out.push(pairs);
    ids.splice(1, 0, ids.pop());               // rotate, seat 0 fixed
  }
  return out;
}

function koPairs(alive) {
  const ms = [];
  for (let i = 0; i + 1 < alive.length; i += 2) ms.push({ a: alive[i], b: alive[i + 1], winner: null });
  return { ms, bye: alive.length % 2 ? alive[alive.length - 1] : null };
}

/* what every phone is allowed to see: names, pairings, results. No tokens,
   no child-room codes — those go to the two players who need them. */
function tourPublic(room) {
  const t = room.tour;
  return {
    kind: t.kind, state: t.state, round: t.round, champ: t.champ, bye: t.bye,
    wins: t.wins, players: room.players.map((p) => p.name),
    matches: t.matches.map((m) => ({ a: m.a, b: m.b, winner: m.winner, live: !!m.code })),
  };
}

/* spawn a real two-player live room for each pairing of the current round
   and tell exactly those two players about it */
function tourSpawn(room) {
  const t = room.tour;
  t.matches.forEach((m, mi) => {
    if (m.code || m.winner != null) return;
    const pa = room.players[m.a], pb = room.players[m.b];
    if (!pa || !pb) return;
    const child = makeRoom(pa.name, { lv: true, fmt: "tb7" });
    const kid = rooms.get(child.code);
    const jb = joinRoom(kid, pb.name);
    m.code = child.code;
    /* each token goes ONLY to its owner — a broadcast here would hand every
       phone in the room the credentials to play someone else's match.
       The ROUND rides along on purpose: this event is emitted before the
       bracket broadcast, so a client that inferred the round from its own
       copy of the bracket would still be reading the previous one and its
       result would come back rejected as stale. */
    emitTo(pa, "match", { mi, round: t.round, code: child.code, token: child.token, me: 0, opp: pb.name });
    emitTo(pb, "match", { mi, round: t.round, code: child.code, token: jb.token, me: 1, opp: pa.name });
  });
  scheduleSave();
}

function tourAdvance(room) {
  const t = room.tour;
  if (t.matches.some((m) => m.winner == null)) return false;   // round still running
  t.matches.forEach((m) => { if (m.winner != null) t.wins[m.winner]++; });
  if (t.kind === "rr") {
    if (t.round >= t.plan.length) {
      const best = Math.max(...t.wins);
      t.champ = t.wins.indexOf(best);
      t.state = "done";
      return true;
    }
    t.matches = t.plan[t.round].map((m) => ({ ...m }));
    t.round++;
  } else {
    /* the previous round's bye goes FIRST so koPairs pairs them into a real
       match and the new bye rotates to someone else — appending the bye last
       let the same player ride byes all the way to the final (5-player KO:
       P5 byed round after round without ever striking a ball) */
    const winners = [...(t.bye != null ? [t.bye] : []), ...t.matches.map((m) => m.winner)];
    if (winners.length <= 1) { t.champ = winners[0] != null ? winners[0] : null; t.state = "done"; return true; }
    const { ms, bye } = koPairs(winners);
    t.matches = ms; t.bye = bye; t.round++;
  }
  tourSpawn(room);
  return true;
}

setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    if (now - room.lastSeen > ROOM_TTL_MS) {
      emit(room, "bye", { reason: "expired" });
      rooms.delete(code);
      scheduleSave();
    }
  }
}, 60 * 60 * 1000).unref();

/* ---------- v1.7.1: the tournament absence sweeper ----------
   R115 audit finding: nothing but a fully played match ever reported a
   result, so one dead phone hung the bracket for everybody, forever. The
   rule now: a side with no open stream on its MATCH room for
   TOUR_FORFEIT_MS forfeits. Presence clears the clock, so a reconnect
   within the window costs nothing. Both sides gone → side a is awarded it
   deterministically ("abandoned") purely so the round can end; in a party
   game an arbitrary-but-consistent rule beats a frozen bracket. */
setInterval(() => {
  const now = Date.now();
  for (const room of rooms.values()) {
    const t = room.tour;
    if (!t || t.state !== "running") continue;
    /* tourSettle can advance the round, which REPLACES t.matches — keep
       iterating the old array after that and we would settle pairings of a
       round that no longer exists. Bail out the moment the array changes. */
    const ms = t.matches;
    for (const m of ms) {
      if (t.matches !== ms || t.state !== "running") break;
      if (m.winner != null || !m.code) continue;
      const kid = rooms.get(m.code);
      if (!kid) { tourSettle(room, m, m.a, "abandoned"); continue; }   // child room expired
      m.absent = m.absent || {};
      [0, 1].forEach((seat) => {
        const kp = kid.players[seat];
        const here = kp && !kp.left && kp.streams.size > 0;
        if (here) delete m.absent[seat];
        else if (m.absent[seat] == null) m.absent[seat] = now;
      });
      const gone0 = m.absent[0] != null && now - m.absent[0] > TOUR_FORFEIT_MS;
      const gone1 = m.absent[1] != null && now - m.absent[1] > TOUR_FORFEIT_MS;
      if (gone0 && gone1) tourSettle(room, m, m.a, "abandoned");
      else if (gone0) tourSettle(room, m, m.b, "forfeit");
      else if (gone1) tourSettle(room, m, m.a, "forfeit");
    }
  }
}, TOUR_SWEEP_MS).unref();

/* ---------- http ---------- */
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://x");
  const parts = url.pathname.split("/").filter(Boolean);

  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    });
    return res.end();
  }

  if (req.method === "GET" && url.pathname === "/api/vapid") {
    return json(res, 200, { key: (webpush && vapidKeys && vapidKeys.publicKey) || null });
  }

  /* ---------- R76: field error telemetry ----------
     Phones report their crashes so bugs surface before anyone notices.
     Ring buffer of the last 120, deduped by message, 6/min/IP cap. */
  if (req.method === "POST" && url.pathname === "/api/err") {
    if (limited(req, true)) return json(res, 429, { error: "slow down" });
    try {
      const b = await readBody(req);
      const msg = String(b.msg || "").slice(0, 300);
      if (!msg) return json(res, 400, { error: "empty" });
      const dup = errLog.find((e) => e.msg === msg);
      if (dup) { dup.n++; dup.last = Date.now(); }
      else errLog.push({ msg, stack: String(b.stack || "").slice(0, 800), kind: String(b.kind || "error").slice(0, 20),
        v: String(b.v || "").slice(0, 24), ua: String(b.ua || "").slice(0, 120), n: 1, first: Date.now(), last: Date.now() });
      while (errLog.length > 120) errLog.shift();
      return json(res, 200, { ok: true });
    } catch (e) { return json(res, 400, { error: "bad report" }); }
  }
  if (req.method === "GET" && url.pathname === "/api/errs") {
    return json(res, 200, { errs: errLog.slice(-60).reverse() });
  }

  if (req.method === "GET" && url.pathname === "/") {
    return json(res, 200, { ok: true, service: "padel-check-mate-relay", v: "1.6.1", rooms: rooms.size, queued: queue.length, board: boards.length, packs: packLib.size, catalog: catalog.size });
  }

  /* creation is the expensive verb wherever it lives — a tournament create
     mints a room exactly like /api/rooms does (and each pairing spawns a
     child room on top), so it counts against the same createMax budget */
  if (limited(req, req.method === "POST" && (url.pathname === "/api/rooms" || url.pathname === "/api/quickmatch" || url.pathname === "/api/tours")))
    return json(res, 429, { error: "slow down" });

  try {
    /* quick match: pair with the closest-IQ waiting stranger, else open a slot */
    if (req.method === "POST" && url.pathname === "/api/quickmatch") {
      const { name, iq, fmt, blz } = await readBody(req);
      const now = Date.now();
      const myIq = Number(iq) || 1000;
      const myBlz = !!blz;
      /* prune dead slots, then pick the nearest IQ */
      for (let i = queue.length - 1; i >= 0; i--) {
        const room = rooms.get(queue[i].code);
        if (!room || room.players.length !== 1 || now - queue[i].ts > QUEUE_TTL_MS || !creatorLive(room)) queue.splice(i, 1);
      }
      /* pair only within an IQ window that WIDENS the longer a slot waits
         (fresh: ±200; +5/s, so after a minute anyone matches anyone) */
      /* two passes: prefer a slot at MY pace (blitz vs classic), then anyone */
      let bestI = -1, bestD = Infinity;
      for (const samePace of [true, false]) {
        queue.forEach((slot, i) => {
          if (samePace && !!slot.blz !== myBlz) return;
          const d = Math.abs((slot.iq || 1000) - myIq);
          const allowed = 200 + ((now - slot.ts) / 1000) * 5;
          if (d <= allowed && d < bestD) { bestD = d; bestI = i; }
        });
        if (bestI >= 0) break;
      }
      if (bestI >= 0) {
        const slot = queue.splice(bestI, 1)[0];
        const room = rooms.get(slot.code);
        const r = joinRoom(room, name);
        return json(res, 200, { matched: true, code: room.code, player: 1, token: r.token, names: r.names, fmt: room.fmt || null, blz: !!room.blz });
      }
      const r = makeRoom(name, { qm: true, iq: myIq, fmt, blz: myBlz });
      queue.push({ code: r.code, ts: now, iq: myIq, blz: myBlz });
      return json(res, 200, { matched: false, code: r.code, player: 0, token: r.token });
    }

    /* ---------- global leaderboard ---------- */
    if (url.pathname === "/api/board") {
      if (req.method === "GET") {
        /* best entry per player name, ranked by Padel IQ */
        const best = new Map();
        boards.forEach((e) => {
          const k = e.name.toLowerCase();
          if (!best.has(k) || e.iq > best.get(k).iq) best.set(k, e);
        });
        const top = [...best.values()].sort((a, b) => b.iq - a.iq || b.avg - a.avg).slice(0, 30);
        const wins = boards.filter((e) => e.won).length;
        return json(res, 200, { top, total: boards.length, wins });
      }
      if (req.method === "POST") {
        const b = await readBody(req);
        const entry = {
          name: String(b.name || "Player").slice(0, 14),
          iq: Math.max(600, Math.min(1400, Math.round(Number(b.iq) || 0))),
          avg: Math.max(0, Math.min(100, Math.round(Number(b.avg) || 0))),
          won: !!b.won,
          vs: String(b.vs || "").slice(0, 40),
          fmt: String(b.fmt || "").slice(0, 20),
          at: Date.now(),
        };
        if (!entry.iq) return json(res, 400, { error: "invalid entry" });
        boards.push(entry);
        if (boards.length > 1000) boards = boards.slice(-1000);
        scheduleSave();
        return json(res, 200, { ok: true });
      }
    }

    /* ---------- community pack library ---------- */
    if (parts[0] === "api" && parts[1] === "packs") {
      if (req.method === "GET" && parts.length === 2) {
        const list = [...packLib.values()]
          .sort((a, b) => b.ts - a.ts).slice(0, 50)
          .map((p) => ({ id: p.id, title: p.title, author: p.author, count: p.count, kind: p.kind, downloads: p.downloads, ts: p.ts }));
        return json(res, 200, { packs: list });
      }
      if (req.method === "GET" && parts.length === 3) {
        const p = packLib.get(decodeURIComponent(parts[2]));
        if (!p) return json(res, 404, { error: "pack not found" });
        p.downloads = (p.downloads || 0) + 1;
        scheduleSave();
        return json(res, 200, { pack: p.pack });
      }
      if (req.method === "POST" && parts.length === 2) {
        const { pack, author } = await readBody(req);
        if (!pack || typeof pack !== "object" || typeof pack.packId !== "string" || !pack.packId.trim())
          return json(res, 400, { error: "invalid pack (need packId)" });
        const sits = Array.isArray(pack.situations) ? pack.situations.length : 0;
        const opps = Array.isArray(pack.opponents) ? pack.opponents.length : 0;
        if (!sits && !opps) return json(res, 400, { error: "pack has no situations or opponents" });
        if (JSON.stringify(pack).length > 48 * 1024) return json(res, 400, { error: "pack too large (48KB max)" });
        if (packLib.size >= 200 && !packLib.has(pack.packId))
          return json(res, 409, { error: "library full" });
        packLib.set(pack.packId, {
          id: pack.packId.slice(0, 60),
          title: String(pack.title || pack.packId).slice(0, 60),
          author: String(author || pack.author || "anon").slice(0, 20),
          count: sits + opps, kind: sits ? "scenarios" : "opponents",
          downloads: packLib.get(pack.packId)?.downloads || 0,
          ts: Date.now(), pack,
        });
        scheduleSave();
        return json(res, 200, { ok: true, id: pack.packId });
      }
    }

    /* ---------- marketplace catalog (v1.3) ----------
       Public: browse metadata, fetch one item's payload on install.
       Admin (ADMIN_KEY env): upsert + delete items from the admin screen. */
    if (parts[0] === "api" && parts[1] === "catalog") {
      /* admin upsert / delete */
      if (req.method === "POST" && parts[2] === "admin") {
        const body = await readBody(req);
        if (body.key !== ADMIN_KEY) return json(res, 403, { error: "bad admin key" });
        if (parts[3] === "delete") {
          if (!catalog.has(body.id)) return json(res, 404, { error: "no such item" });
          catalog.delete(body.id);
          scheduleSave();
          return json(res, 200, { ok: true });
        }
        const it = body.item;
        if (!it || typeof it !== "object" || !String(it.id || "").trim())
          return json(res, 400, { error: "invalid item (need id)" });
        if (!["pack", "opponents", "character", "cosmetic"].includes(it.type))
          return json(res, 400, { error: "invalid type" });
        if (JSON.stringify(it).length > 56 * 1024) return json(res, 400, { error: "item too large (56KB max)" });
        const priceIn = it.price || {};
        catalog.set(String(it.id).slice(0, 60), {
          id: String(it.id).slice(0, 60),
          type: it.type,
          title: String(it.title || it.id).slice(0, 60),
          blurb: String(it.blurb || "").slice(0, 200),
          icon: String(it.icon || "📦").slice(0, 8),
          price: {
            mode: ["free", "coins", "iap"].includes(priceIn.mode) ? priceIn.mode : "free",
            coins: Math.max(0, Math.min(99999, Math.round(Number(priceIn.coins) || 0))),
            promo: String(priceIn.promo || "").slice(0, 30),   // IAP test-unlock code
            display: String(priceIn.display || "").slice(0, 16), // e.g. "$1.99"
          },
          payload: it.payload ?? null,
          downloads: catalog.get(it.id)?.downloads || 0,
          ts: Date.now(),
        });
        scheduleSave();
        return json(res, 200, { ok: true, id: it.id, items: catalog.size });
      }
      /* public browse: metadata only — never leak payloads or promo codes */
      if (req.method === "GET" && parts.length === 2) {
        const items = [...catalog.values()].sort((a, b) => b.ts - a.ts).map((c) => ({
          id: c.id, type: c.type, title: c.title, blurb: c.blurb, icon: c.icon,
          price: { mode: c.price.mode, coins: c.price.coins, display: c.price.display },
          downloads: c.downloads, ts: c.ts,
          count: c.payload && Array.isArray(c.payload.situations) ? c.payload.situations.length
            : c.payload && Array.isArray(c.payload.opponents) ? c.payload.opponents.length : null,
        }));
        return json(res, 200, { items });
      }
      /* fetch one item WITH payload (install). Paid items accept the promo
         code as proof-of-purchase in the IAP-stub era. */
      if (req.method === "GET" && parts.length === 3) {
        const c = catalog.get(decodeURIComponent(parts[2]));
        if (!c) return json(res, 404, { error: "item not found" });
        if (c.price.mode === "iap" && c.price.promo
          && url.searchParams.get("promo") !== c.price.promo && url.searchParams.get("key") !== ADMIN_KEY)
          return json(res, 402, { error: "purchase required" });
        c.downloads = (c.downloads || 0) + 1;
        scheduleSave();
        return json(res, 200, { item: { id: c.id, type: c.type, title: c.title, payload: c.payload } });
      }
    }

    /* lobby: open challenges anyone can join */
    if (req.method === "GET" && url.pathname === "/api/lobby") {
      const now = Date.now();
      const open = [...rooms.values()]
        .filter((r) => r.players.length === 1 && (r.pub || r.qm) && creatorLive(r))
        .sort((a, b) => b.created - a.created).slice(0, 20)
        .map((r) => ({ code: r.code, name: r.players[0].name, iq: r.iq, qm: !!r.qm, blz: !!r.blz, lv: !!r.lv, ageMin: Math.round((now - r.created) / 60000) }));
      return json(res, 200, { open });
    }

    /* ---------- v1.7: tournament rooms ----------
       create / join / start / result. Everything else (the SSE stream, the
       ticket exchange, leave, persistence) is the ordinary room machinery,
       because a tournament room IS a room. */
    if (parts[0] === "api" && parts[1] === "tours") {
      if (req.method === "POST" && parts.length === 2) {
        const { name, kind } = await readBody(req);
        const k = kind === "rr" ? "rr" : "ko";
        const r = makeRoom(name, { max: TOUR_MAX, tour: { kind: k, state: "lobby", round: 0, matches: [], bye: null, champ: null, wins: [], plan: null } });
        return json(res, 200, { code: r.code, token: r.token, idx: 0, kind: k });
      }
      const room = rooms.get((parts[2] || "").toUpperCase());
      if (!room || !room.tour) return json(res, 404, { error: "tournament not found" });
      room.lastSeen = Date.now();
      const act = parts[3];
      const t = room.tour;

      if (req.method === "GET" && !act) return json(res, 200, { tour: tourPublic(room) });

      /* v1.7.1: everything a reconnecting/restarted phone needs, in one
         authenticated call: who I am, the bracket, and my pending match
         invite if one is waiting. Stateless on the client by design. */
      if (req.method === "GET" && act === "me") {
        const me = playerByToken(room, tokenOf(req, url));
        if (!me) return json(res, 403, { error: "bad token" });
        return json(res, 200, { idx: me.idx, kind: t.kind, tour: tourPublic(room), match: tourMineFor(room, me) });
      }

      /* v1.7.1: the CALLER concedes their current pairing. Quitting a
         tournament match is conceding it — stated in the UI, enforced here. */
      if (req.method === "POST" && act === "forfeit") {
        const { token, mi, round } = await readBody(req);
        const me = playerByToken(room, token);
        if (!me) return json(res, 403, { error: "bad token" });
        if (Number(round) !== t.round) return json(res, 200, { tour: tourPublic(room), stale: true });
        const m = t.matches[mi];
        if (!m) return json(res, 404, { error: "no such match" });
        if (me.idx !== m.a && me.idx !== m.b) return json(res, 403, { error: "not your match" });
        if (m.winner == null) tourSettle(room, m, me.idx === m.a ? m.b : m.a, "forfeit");
        return json(res, 200, { tour: tourPublic(room) });
      }

      if (req.method === "POST" && act === "join") {
        if (t.state !== "lobby") return json(res, 409, { error: "already under way" });
        if (room.players.length >= TOUR_MAX) return json(res, 409, { error: "tournament full" });
        const { name } = await readBody(req);
        const r = joinRoom(room, name);
        emit(room, "bracket", tourPublic(room));
        return json(res, 200, { token: r.token, idx: r.idx, names: r.names, kind: t.kind });
      }

      /* only the host can call it on */
      if (req.method === "POST" && act === "start") {
        const { token } = await readBody(req);
        if (!room.players[0] || room.players[0].token !== token) return json(res, 403, { error: "host only" });
        if (t.state !== "lobby") return json(res, 409, { error: "already under way" });
        const n = room.players.length;
        if (n < TOUR_MIN) return json(res, 400, { error: `need at least ${TOUR_MIN} players` });
        t.wins = new Array(n).fill(0);
        if (t.kind === "rr") {
          t.plan = rrRounds(n);
          t.matches = t.plan[0].map((m) => ({ ...m }));
          t.round = 1;
        } else {
          const seeds = [...Array(n).keys()].sort(() => Math.random() - 0.5);
          const { ms, bye } = koPairs(seeds);
          t.matches = ms; t.bye = bye; t.round = 1;
        }
        t.state = "running";
        tourSpawn(room);
        emit(room, "bracket", tourPublic(room));
        return json(res, 200, { tour: tourPublic(room) });
      }

      /* either player of the pairing reports it; first report wins, so a
         double report from both phones cannot double-count a win */
      if (req.method === "POST" && act === "result") {
        const { token, mi, winner, round } = await readBody(req);
        const me = playerByToken(room, token);
        if (!me) return json(res, 403, { error: "bad token" });
        /* the round must match. Without this a result that arrives late —
           after its round has already been settled and t.matches replaced —
           would land on whatever pairing now sits at that index and hand a
           win to someone who has not played yet. */
        if (Number(round) !== t.round) return json(res, 200, { tour: tourPublic(room), stale: true });
        const m = t.matches[mi];
        if (!m) return json(res, 404, { error: "no such match" });
        if (me.idx !== m.a && me.idx !== m.b) return json(res, 403, { error: "not your match" });
        if (m.winner == null) {
          m.winner = winner === 1 ? m.b : m.a;
          m.code = null;
          tourAdvance(room);
          emit(room, "bracket", tourPublic(room));
          scheduleSave();
        }
        return json(res, 200, { tour: tourPublic(room) });
      }
      return json(res, 404, { error: "not found" });
    }

    if (parts[0] !== "api" || parts[1] !== "rooms") return json(res, 404, { error: "not found" });

    /* create room (public: true → listed in the lobby) */
    if (req.method === "POST" && parts.length === 2) {
      const { name, public: pub, iq, fmt, blz, lv } = await readBody(req);
      const r = makeRoom(name, { pub, iq, fmt, blz, lv });
      return json(res, 200, { code: r.code, player: 0, token: r.token });
    }

    const room = rooms.get((parts[2] || "").toUpperCase());
    if (!room) return json(res, 404, { error: "room not found" });
    room.lastSeen = Date.now();
    const action = parts[3];

    /* join */
    if (req.method === "POST" && action === "join") {
      /* v1.7.1: a tournament code typed into the ordinary join box used to
         create a phantom seat that could deadlock the bracket. Say what it
         is instead. */
      if (room.tour) return json(res, 409, { error: "tournament code — join it from Tournament night" });
      if (room.players.length >= (room.max || 2)) return json(res, 409, { error: "room full" });
      const { name } = await readBody(req);
      const r = joinRoom(room, name);
      return json(res, 200, { player: 1, token: r.token, names: r.names, fmt: room.fmt || null, blz: !!room.blz, lv: !!room.lv });
    }

    /* leave: abandoned single-player rooms die immediately; a room where
       BOTH players have left is deleted (rejoining a stream un-leaves) */
    if (req.method === "POST" && action === "leave") {
      const { token } = await readBody(req);
      const me = playerByToken(room, token);
      if (!me) return json(res, 403, { error: "bad token" });
      me.left = true;
      me.streams.forEach((s) => { try { s.end(); } catch (e) { /* gone */ } });
      me.streams.clear();
      if (room.players.length === 1 || room.players.every((p) => p.left)) {
        emit(room, "bye", { reason: "closed" });
        rooms.delete(room.code);
      } else {
        /* tell the remaining player their rival walked off the court */
        const other = room.players.find((p) => p.idx !== me.idx);
        if (other) emitTo(other, "left", { name: me.name });
      }
      scheduleSave();
      return json(res, 200, { ok: true });
    }

    /* v1.4: trade the token (in a header, out of the logs) for a one-time
       stream ticket — the only credential that ever rides a GET URL */
    if (req.method === "POST" && action === "ticket") {
      const body = await readBody(req);
      const me = playerByToken(room, bearerOf(req) || body.token);
      if (!me) return json(res, 403, { error: "bad token" });
      return json(res, 200, { ticket: mintTicket(me), ttl: Math.round(TICKET_TTL / 1000) });
    }

    /* v1.6: store this player's push subscription (or null to clear) */
    if (req.method === "POST" && action === "subscribe") {
      const body = await readBody(req);
      const me = playerByToken(room, bearerOf(req) || body.token);
      if (!me) return json(res, 403, { error: "bad token" });
      const sub = body.sub;
      me.push = sub && typeof sub === "object" && typeof sub.endpoint === "string" && sub.endpoint.length < 1024 ? sub : null;
      scheduleSave();
      return json(res, 200, { ok: true, push: !!me.push });
    }

    /* post a turn — validated, relayed to the OTHER player */
    if (req.method === "POST" && action === "turn") {
      const { token, payload } = await readBody(req);
      const me = playerByToken(room, token);
      if (process.env.TURN_LOG) console.log(`[turn] room=${room.code} auth=${me ? me.idx + ":" + me.name : "REJECTED"} stage=${payload && payload.stage} sq=${payload && payload.sq} otherStreams=${me ? (room.players.find((p) => p.idx !== me.idx) || { streams: { size: "?" } }).streams.size : "?"}`);
      if (!me) return json(res, 403, { error: "bad token" });
      if (!payload || typeof payload !== "object" || payload.v !== 1 || typeof payload.stage !== "string")
        return json(res, 400, { error: "invalid turn payload (need v:1 and stage)" });
      /* game turns, point results and chat enter history — handshakes
         (rematch etc.) are relay-only so match stories stay clean.
         v1.7.1: live "strike"/"miss" stages are stored too. They were
         relay-only, which meant a player whose stream wasn't up yet — or
         had silently died — missed the ball FOREVER: R115 measured a
         four-phone tournament final sitting at 0-0 for seven minutes
         because one serve message evaporated. Stored strikes make the
         stream-open replay (below) able to hand a reconnecting player the
         last ball, and the client's sq guard makes re-delivery free. */
      if (["serve", "rally", "end", "point", "chat", "strike", "miss"].includes(payload.stage)) {
        room.turns.push({ from: me.idx, at: Date.now(), payload });
        if (room.turns.length > 50) room.turns.shift();
        scheduleSave();
      }
      const other = room.players.find((p) => p.idx !== me.idx);
      if (other) emitTo(other, "turn", payload);
      if (other) maybePush(room, me, other, payload);
      return json(res, 200, { ok: true, delivered: !!(other && other.streams.size) });
    }

    /* SSE stream — ticket first (v1.4), legacy query token still accepted */
    if (req.method === "GET" && action === "stream") {
      const me = playerByTicket(room, url.searchParams.get("ticket"))
        || playerByToken(room, url.searchParams.get("token"));
      if (!me) return json(res, 403, { error: "bad token" });
      sse(res);
      if (process.env.TURN_LOG) console.log(`[stream] room=${room.code} player=${me.idx}:${me.name} open (now ${me.streams.size + 1})`);
      me.left = false; // reconnecting counts as coming back
      me.streams.add(res);
      /* v1.5: back from the dead — cancel the lost verdict, tell the rival */
      clearTimeout(me.lostTimer);
      if (me.lostAnnounced) {
        me.lostAnnounced = false;
        const other = room.players.find((p) => p.idx !== me.idx);
        if (other) emitTo(other, "back", { name: me.name });
      }
      /* replay the last PLAYABLE turn the other side sent while we were
         offline (point results aren't playable — skip those) */
      const missed = room.turns.filter((t) => t.from !== me.idx && ["serve", "rally", "end", "strike", "miss"].includes(t.payload.stage));
      if (missed.length) res.write(`event: turn\ndata: ${JSON.stringify(missed[missed.length - 1].payload)}\n\n`);
      if (me.idx === 0 && room.players.length === 2)
        res.write(`event: joined\ndata: ${JSON.stringify({ name: room.players[1].name })}\n\n`);
      /* v1.7.1: a tournament stream that (re)opens gets the current bracket
         and — privately — its owner's pending match invite. This is what
         makes the invite safe to miss: iOS suspends the connection, the
         EventSource reconnects on foreground, and the state it slept
         through is simply sent again. */
      if (room.tour) {
        res.write(`event: bracket\ndata: ${JSON.stringify(tourPublic(room))}\n\n`);
        const mine = tourMineFor(room, me);
        if (mine) res.write(`event: match\ndata: ${JSON.stringify(mine)}\n\n`);
      }
      const ping = setInterval(() => { try { res.write(":ping\n\n"); } catch (e) { /* gone */ } }, 25000);
      req.on("close", () => {
        clearInterval(ping);
        me.streams.delete(res);
        /* v1.5: no explicit leave, no surviving stream — after a grace
           window (app killed, battery died, tunnel collapsed) the rival
           finally hears about it instead of waiting forever */
        if (!me.left && me.streams.size === 0 && room.players.length === 2) {
          clearTimeout(me.lostTimer);
          me.lostTimer = setTimeout(() => {
            const rm = rooms.get(room.code);
            const cur = rm && rm.players.find((p) => p.idx === me.idx);
            if (!rm || !cur || cur.left || cur.streams.size) return;
            cur.lostAnnounced = true;
            const other = rm.players.find((p) => p.idx !== me.idx);
            if (other) emitTo(other, "left", { name: cur.name, lost: true });
          }, LOST_GRACE_MS);
        }
      });
      return;
    }

    /* match story: full turn history. Public rooms are open to anyone
       (spectators); private rooms require a player token. */
    if (req.method === "GET" && action === "history") {
      const me = playerByToken(room, tokenOf(req, url));
      if (!room.pub && !room.qm && !me) return json(res, 403, { error: "private room — player token required" });
      return json(res, 200, {
        names: room.players.map((p) => p.name),
        created: room.created,
        turns: room.turns.map((t) => ({
          from: t.from, at: t.at, stage: t.payload.stage, turn: t.payload.turn,
          sc: t.payload.sc || null, res: t.payload.res || null,
          sit: t.payload.sit ? t.payload.sit.id : null,
          msg: t.payload.msg || null, name: t.payload.name || null,
        })),
      });
    }

    /* poll fallback */
    if (req.method === "GET" && action === "state") {
      const me = playerByToken(room, tokenOf(req, url));
      if (!me) return json(res, 403, { error: "bad token" });
      const theirs = room.turns.filter((t) => t.from !== me.idx);
      return json(res, 200, {
        names: room.players.map((p) => p.name),
        full: room.players.length === 2,
        nTheirs: theirs.length,
        lastTurnAt: theirs.length ? theirs[theirs.length - 1].at : 0,
        lastTurn: theirs.length ? theirs[theirs.length - 1].payload : null,
      });
    }

    return json(res, 404, { error: "unknown action" });
  } catch (e) {
    return json(res, 400, { error: "bad request" });
  }
});

server.listen(PORT, () => console.log(`🎾 Padel Check Mate relay v1.6 listening on :${PORT}${ADMIN_KEY === "change-me" ? "  ⚠ ADMIN_KEY not set — marketplace admin uses the default key!" : ""}`));
