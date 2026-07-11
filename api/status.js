// Public, read-only view of the latest state for the dashboard.
// Reads only what check.js wrote; never touches credentials.

const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const STATE_KEY = "dash:state";

async function redis(command) {
  const r = await fetch(REDIS_URL, {
    method: "POST",
    headers: { Authorization: `Bearer ${REDIS_TOKEN}`, "Content-Type": "application/json" },
    body: JSON.stringify(command),
  });
  const j = await r.json();
  return j.result;
}

module.exports = async (req, res) => {
  try {
    const raw = await redis(["GET", STATE_KEY]);
    const state = raw
      ? JSON.parse(raw)
      : { history: [], feed: [], latest: null };

    const h = state.history || [];
    const step = Math.max(1, Math.ceil(h.length / 300));
    const history = h.filter((_, i) => i % step === 0);

    res.setHeader("Cache-Control", "s-maxage=30, stale-while-revalidate=60");
    return res.status(200).json({
      appVersion: state.appVersion || "unknown",
      latest: state.latest || null,
      feed: (state.feed || []).slice(0, 40),
      history,
    });
  } catch (err) {
    return res.status(500).json({ error: String(err.message || err) });
  }
};
