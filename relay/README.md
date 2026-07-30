# Battle Padel relay (v1.4)

Zero-dependency Node server for online matches, quickmatch, the global
leaderboard and the community marketplace. Runs anywhere Node ≥18 runs:
`node index.mjs` (port 8787 or $PORT; set ADMIN_KEY in production).

Free-tier note (Render): the service sleeps after ~15 idle minutes and takes
up to a minute to wake — the game's client retries and says so.
