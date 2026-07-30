/* ---------- Padel Check Mate relay server v1.4 ----------
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
const RATE = { windowMs: 60 * 1000, max: 600, createMax: 15, createWindowMs: 60 * 60 * 1000 };

const rooms = new Map();   // code → room
let boards = [];           // global leaderboard entries (capped)
const packLib = new Map(); // packId → published community pack
const catalog = new Map(); // itemId → marketplace item (admin-managed)
const queue = [];          // quick-match waiting rooms: {code, ts}
const rate = new Map();    // ip → {n, t0, created, c0}
const ADMIN_KEY = process.env.ADMIN_KEY || "change-me"; // set on deploy!

/* ---------- persistence: best-effort JSON snapshot ---------- */
let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try {
      const snap = [...rooms.values()].map((r) => ({
        code: r.code, created: r.created, lastSeen: r.lastSeen, turns: r.turns,
        pub: r.pub, qm: r.qm, iq: r.iq, fmt: r.fmt,
        players: r.players.map((p) => ({ idx: p.idx, name: p.name, token: p.token, left: !!p.left })),
      }));
      fs.writeFileSync(DATA_FILE, JSON.stringify({ v: 1, rooms: snap, boards, packs: [...packLib.values()], catalog: [...catalog.values()] }));
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
    (snap.packs || []).forEach((p) => packLib.set(p.id, p));
    (snap.catalog || []).forEach((c) => catalog.set(c.id, c));
    console.log(`↻ restored ${rooms.size} room(s) from ${DATA_FILE}`);
  }
} catch (e) { console.error("restore failed:", e.message); }

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
    pub: !!opts.pub, qm: !!opts.qm, iq: Number(opts.iq) || null,
    fmt: typeof opts.fmt === "string" ? opts.fmt.slice(0, 12) : null,
    players: [{ idx: 0, name: String(name || "Player 1").slice(0, 14), token, streams: new Set() }],
  });
  scheduleSave();
  return { code, token };
}

function joinRoom(room, name) {
  const token = crypto.randomBytes(12).toString("hex");
  const player = { idx: 1, name: String(name || "Player 2").slice(0, 14), token, streams: new Set() };
  room.players.push(player);
  room.lastSeen = Date.now();
  emitTo(room.players[0], "joined", { name: player.name });
  scheduleSave();
  return { token, names: room.players.map((p) => p.name) };
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

  if (req.method === "GET" && url.pathname === "/") {
    return json(res, 200, { ok: true, service: "padel-check-mate-relay", v: "1.4", rooms: rooms.size, queued: queue.length, board: boards.length, packs: packLib.size, catalog: catalog.size });
  }

  if (limited(req, req.method === "POST" && (url.pathname === "/api/rooms" || url.pathname === "/api/quickmatch")))
    return json(res, 429, { error: "slow down" });

  try {
    /* quick match: pair with the closest-IQ waiting stranger, else open a slot */
    if (req.method === "POST" && url.pathname === "/api/quickmatch") {
      const { name, iq, fmt } = await readBody(req);
      const now = Date.now();
      const myIq = Number(iq) || 1000;
      /* prune dead slots, then pick the nearest IQ */
      for (let i = queue.length - 1; i >= 0; i--) {
        const room = rooms.get(queue[i].code);
        if (!room || room.players.length !== 1 || now - queue[i].ts > QUEUE_TTL_MS || !creatorLive(room)) queue.splice(i, 1);
      }
      /* pair only within an IQ window that WIDENS the longer a slot waits
         (fresh: ±200; +5/s, so after a minute anyone matches anyone) */
      let bestI = -1, bestD = Infinity;
      queue.forEach((slot, i) => {
        const d = Math.abs((slot.iq || 1000) - myIq);
        const allowed = 200 + ((now - slot.ts) / 1000) * 5;
        if (d <= allowed && d < bestD) { bestD = d; bestI = i; }
      });
      if (bestI >= 0) {
        const slot = queue.splice(bestI, 1)[0];
        const room = rooms.get(slot.code);
        const r = joinRoom(room, name);
        return json(res, 200, { matched: true, code: room.code, player: 1, token: r.token, names: r.names, fmt: room.fmt || null });
      }
      const r = makeRoom(name, { qm: true, iq: myIq, fmt });
      queue.push({ code: r.code, ts: now, iq: myIq });
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
        .map((r) => ({ code: r.code, name: r.players[0].name, iq: r.iq, qm: !!r.qm, ageMin: Math.round((now - r.created) / 60000) }));
      return json(res, 200, { open });
    }

    if (parts[0] !== "api" || parts[1] !== "rooms") return json(res, 404, { error: "not found" });

    /* create room (public: true → listed in the lobby) */
    if (req.method === "POST" && parts.length === 2) {
      const { name, public: pub, iq, fmt } = await readBody(req);
      const r = makeRoom(name, { pub, iq, fmt });
      return json(res, 200, { code: r.code, player: 0, token: r.token });
    }

    const room = rooms.get((parts[2] || "").toUpperCase());
    if (!room) return json(res, 404, { error: "room not found" });
    room.lastSeen = Date.now();
    const action = parts[3];

    /* join */
    if (req.method === "POST" && action === "join") {
      if (room.players.length >= 2) return json(res, 409, { error: "room full" });
      const { name } = await readBody(req);
      const r = joinRoom(room, name);
      return json(res, 200, { player: 1, token: r.token, names: r.names, fmt: room.fmt || null });
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

    /* post a turn — validated, relayed to the OTHER player */
    if (req.method === "POST" && action === "turn") {
      const { token, payload } = await readBody(req);
      const me = playerByToken(room, token);
      if (!me) return json(res, 403, { error: "bad token" });
      if (!payload || typeof payload !== "object" || payload.v !== 1 || typeof payload.stage !== "string")
        return json(res, 400, { error: "invalid turn payload (need v:1 and stage)" });
      /* game turns, point results and chat enter history — handshakes
         (rematch etc.) are relay-only so match stories stay clean */
      if (["serve", "rally", "end", "point", "chat"].includes(payload.stage)) {
        room.turns.push({ from: me.idx, at: Date.now(), payload });
        if (room.turns.length > 50) room.turns.shift();
        scheduleSave();
      }
      const other = room.players.find((p) => p.idx !== me.idx);
      if (other) emitTo(other, "turn", payload);
      return json(res, 200, { ok: true, delivered: !!(other && other.streams.size) });
    }

    /* SSE stream — ticket first (v1.4), legacy query token still accepted */
    if (req.method === "GET" && action === "stream") {
      const me = playerByTicket(room, url.searchParams.get("ticket"))
        || playerByToken(room, url.searchParams.get("token"));
      if (!me) return json(res, 403, { error: "bad token" });
      sse(res);
      me.left = false; // reconnecting counts as coming back
      me.streams.add(res);
      /* replay the last PLAYABLE turn the other side sent while we were
         offline (point results aren't playable — skip those) */
      const missed = room.turns.filter((t) => t.from !== me.idx && ["serve", "rally", "end"].includes(t.payload.stage));
      if (missed.length) res.write(`event: turn\ndata: ${JSON.stringify(missed[missed.length - 1].payload)}\n\n`);
      if (me.idx === 0 && room.players.length === 2)
        res.write(`event: joined\ndata: ${JSON.stringify({ name: room.players[1].name })}\n\n`);
      const ping = setInterval(() => { try { res.write(":ping\n\n"); } catch (e) { /* gone */ } }, 25000);
      req.on("close", () => { clearInterval(ping); me.streams.delete(res); });
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

server.listen(PORT, () => console.log(`🎾 Padel Check Mate relay v1.4 listening on :${PORT}${ADMIN_KEY === "change-me" ? "  ⚠ ADMIN_KEY not set — marketplace admin uses the default key!" : ""}`));
