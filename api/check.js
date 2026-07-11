// Cron-driven poller. Vercel invokes this every N minutes (see vercel.json).
// Polls watched exchange addresses on Dash, stores rolling history + a whale
// feed in Upstash Redis, and pushes ntfy alerts on large flows.
//
// Auth: Vercel Cron attaches "Authorization: Bearer <CRON_SECRET>" automatically.

const { getAddressBalance, getAddressDeltas, rpc } = require("./_rpc");

const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
const NTFY_TOPIC = process.env.NTFY_TOPIC;
const CRON_SECRET = process.env.CRON_SECRET;

const STATE_KEY = "dash:state";
const APP_VERSION = "v3-binance-flow";

// --- Tuning knobs -----------------------------------------------------------
const FEED_THRESHOLD_DASH = 100;     // record flows >= this in the feed
const WHALE_THRESHOLD_DASH = 1000;   // ntfy push on single flows >= this
const HISTORY_MAX = 2016;            // ~7 days at 5-min sampling
const FEED_MAX = 100;                // whale feed entries retained
const SATS = 1e8;
// ---------------------------------------------------------------------------

// Watched exchange wallets. Binance only for now.
const WATCHED = {
  "XnT33zjrFKjt3ymfyQZs2FPiKNer3WVj14": "Binance",
};

async function redis(command) {
  const r = await fetch(REDIS_URL, {
    method: "POST",
    headers: { Authorization: `Bearer ${REDIS_TOKEN}`, "Content-Type": "application/json" },
    body: JSON.stringify(command),
  });
  const j = await r.json();
  return j.result;
}

async function push(title, message, priority = "default", tags = "") {
  if (!NTFY_TOPIC) return;
  await fetch(`https://ntfy.sh/${NTFY_TOPIC}`, {
    method: "POST",
    headers: { Title: title, Priority: priority, Tags: tags },
    body: message,
  });
}

module.exports = async (req, res) => {
  if (CRON_SECRET) {
    const auth = req.headers["authorization"] || "";
    if (auth !== `Bearer ${CRON_SECRET}`) {
      return res.status(401).json({ error: "unauthorized" });
    }
  }

  try {
    const now = Date.now();

    // Chain height first — used to only pull NEW deltas each run.
    const height = await rpc("getblockcount");

    const raw = await redis(["GET", STATE_KEY]);
    const state = raw
      ? JSON.parse(raw)
      : { history: [], feed: [], balances: {}, lastHeight: 0 };

    const feed = state.feed || [];
    const balancesNow = {};
    const snapshot = { t: now, height, wallets: {} };

    for (const [address, label] of Object.entries(WATCHED)) {
      // Current balance.
      const bal = await getAddressBalance(address);
      const balanceDash = bal.balance / SATS;
      balancesNow[address] = bal.balance;

      const prevBal = state.balances ? state.balances[address] : undefined;
      const deltaDash =
        prevBal != null ? (bal.balance - prevBal) / SATS : 0;

      snapshot.wallets[address] = {
        label,
        balance: balanceDash,
        received: bal.received / SATS,
        delta: deltaDash,
      };

      // New transaction deltas since last processed height.
      const startH = (state.lastHeight || height) + 1;
      let deltas = [];
      try {
        deltas = await getAddressDeltas(address, startH, height);
      } catch (e) {
        // Non-fatal: keep balances even if delta window fails.
        deltas = [];
      }

      for (const d of deltas) {
        const amt = d.satoshis / SATS;
        const abs = Math.abs(amt);
        if (abs < FEED_THRESHOLD_DASH) continue; // skip dust from the feed
        const dir = amt >= 0 ? "in" : "out";
        const entry = {
          t: now,
          label,
          address,
          dir,
          amount: abs,
          txid: d.txid,
          height: d.height,
        };
        feed.unshift(entry);

        if (abs >= WHALE_THRESHOLD_DASH) {
          await push(
            `\ud83d\udc0b ${label} ${dir === "in" ? "inflow" : "outflow"}`,
            `${Math.abs(amt).toLocaleString()} DASH ${dir === "in" ? "\u2192" : "\u2190"} ${label}\ntx ${d.txid.slice(0, 16)}\u2026`,
            "high",
            dir === "in" ? "inbox_tray" : "outbox_tray"
          );
        }
      }
    }

    // Append total-across-wallets balance point for the chart.
    const totalDash =
      Object.values(snapshot.wallets).reduce((s, w) => s + w.balance, 0);
    state.history.push({ t: now, height, total: totalDash });
    if (state.history.length > HISTORY_MAX) {
      state.history = state.history.slice(-HISTORY_MAX);
    }

    state.feed = feed.slice(0, FEED_MAX);
    state.balances = balancesNow;
    state.lastHeight = height;
    state.latest = snapshot;
    state.appVersion = APP_VERSION;

    await redis(["SET", STATE_KEY, JSON.stringify(state)]);

    return res.status(200).json({
      ok: true,
      height,
      wallets: Object.keys(WATCHED).length,
      feed: state.feed.length,
    });
  } catch (err) {
    return res.status(500).json({ error: String(err.message || err) });
  }
};
