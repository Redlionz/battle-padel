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

const VER = "padelchess-sw-v2"; // v2: meshopt-compressed models — refetch /models/
const CORE = ["./", "./index.html", "./manifest.webmanifest", "./icon-192.png", "./icon-512.png"];

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

  const sameOrigin = url.origin === self.location.origin;
  const isFont = url.hostname.endsWith("fonts.googleapis.com") || url.hostname.endsWith("fonts.gstatic.com");
  if (!sameOrigin && !isFont) return;

  /* static content: cache-first, populate on first fetch */
  e.respondWith(
    caches.match(req).then((hit) =>
      hit ||
      fetch(req).then((r) => {
        if (r.ok || r.type === "opaque") {
          const cp = r.clone();
          caches.open(VER).then((c) => c.put(req, cp));
        }
        return r;
      })
    )
  );
});
