/* ---------- Battle Padel service worker ----------
   Offline after the first visit, without ever trapping anyone on stale code:

   · NAVIGATIONS are network-first — a fresh deploy is picked up on the next
     online load; offline falls back to the cached shell.
   · everything else same-origin (the content-hashed /assets bundles, the
     GLB models, icons) is cache-first: hashed names are immutable, models
     never change between deploys without a rename.
   · Google-Fonts requests are cached opaque, cache-first, so the Outfit
     face survives offline too.
   · /api/ traffic (the relay) is NEVER touched — live match streams and
     catalog calls must not be replayed from a cache.

   The cache name is versioned and prefixed `padelchess` — main.jsx's dev
   cleanup wipes that prefix, and activate() deletes every older version. */

const VER = "padelchess-sw-v11"; // v11: R180 — the medallion is out, the shield
                                 // is in under a NEW name (logo.png). The name
                                 // change is what actually saves us here: this
                                 // worker is cache-first on stable names, so
                                 // reusing emblem.png would have kept serving
                                 // the old mark to every phone that had it.
// NOTE: the twenty character .glb files are cache-first under STABLE names,
// so changing what is inside them is invisible to a phone unless this VER
// changes and the old cache is dropped. Bump it on every model rebuild.
const CORE = ["./", "./index.html", "./manifest.webmanifest", "./icon-192.png", "./icon-512.png", "./logo.png", "./intro-poster.jpg"];

self.addEventListener("install", (e) => {
  e.waitUntil(
    caches.open(VER).then((c) => c.addAll(CORE)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (e) => {
  e.waitUntil(
    caches.keys()
      .then((ks) => Promise.all(ks.filter((k) => k.startsWith("padelchess") && k !== VER).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (e) => {
  const req = e.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);
  /* the relay is live traffic — never cached, never intercepted */
  if (url.pathname.includes("/api/")) return;

  /* app navigations: network-first with offline shell fallback */
  if (req.mode === "navigate") {
    e.respondWith(
      fetch(req)
        .then((r) => {
          const cp = r.clone();
          caches.open(VER).then((c) => c.put("./index.html", cp));
          return r;
        })
        .catch(() => caches.match("./index.html"))
    );
    return;
  }

  /* R179: MEDIA IS NOT A STATIC ASSET. A <video> fetch usually arrives as a
     Range request, and two things go wrong if this worker treats it like a
     .js file. Response.ok is true for 206, so cache.put() gets handed a
     partial response and throws "Partial response is unsupported" as an
     unhandled rejection. And a cached full 200 answered back to a Range
     request is a thing Safari's media stack is entitled to refuse. So: range
     requests go straight to the network, and only a complete 200 is ever
     stored. The film is not precached either — that would download 4.6MB at
     install AND again for the element on the very first visit. */
  if (req.headers.get("range")) return;

  const sameOrigin = url.origin === self.location.origin;
  const isFont = url.hostname.endsWith("fonts.googleapis.com") || url.hostname.endsWith("fonts.gstatic.com");
  if (!sameOrigin && !isFont) return;

  /* static content: cache-first, populate on first fetch */
  e.respondWith(
    caches.match(req).then((hit) =>
      hit ||
      fetch(req).then((r) => {
        if (r.status === 200 || r.type === "opaque") {
          const cp = r.clone();
          caches.open(VER).then((c) => c.put(req, cp)).catch(() => {});
        }
        return r;
      })
    )
  );
});

/* ---------- push (Round 56): "your move" lands with the app closed ---------- */
self.addEventListener("push", (e) => {
  let d = {};
  try { d = e.data ? e.data.json() : {}; } catch (err) { /* opaque payload */ }
  e.waitUntil(self.registration.showNotification(d.t || "Battle Padel", {
    body: d.b || "It's your move!",
    icon: "./icon-192.png",
    badge: "./icon-192.png",
    tag: "bp-turn-" + (d.code || ""),   // newer pushes for the same room replace, not stack
    data: d,
  }));
});
self.addEventListener("notificationclick", (e) => {
  e.notification.close();
  e.waitUntil(clients.matchAll({ type: "window", includeUncontrolled: true }).then((cs) => {
    for (const c of cs) if ("focus" in c) return c.focus();
    return clients.openWindow("./");
  }));
});
