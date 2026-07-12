// api/check.js — the 5-minute sampler, triggered by Vercel Cron (Pro plan).
// Vercel automatically sends: Authorization: Bearer <CRON_SECRET>
// Manual trigger also works: /api/check?secret=<CRON_SECRET>
//
// Metric: cumulative net balance change over the last 24 hours.
// Alerts are edge-triggered: one push when the 24h change crosses the
// threshold, re-armed when it falls back below 80% of the threshold.

// Watched addresses. Labels are deliberately neutral — these are addresses
// whose on-chain behaviour is consistent with exchange hot wallets, but none
// is confirmed as belonging to any named exchange.
// Numbers follow balance rank at the time of adding; the dashboard sorts cards
// by LIVE balance, so if the ranking shifts the cards reorder and the labels
// stay put as stable identifiers.
const WATCHED = {
  // 85,541 DASH · 53,904 tx
  "XsqwdGfza8Rf3GrSom988Rxhaprecw3XD6": "Suspected-Exchange-Hot-Wallet-1",
  // 63,587 DASH · 1,244,379 tx · near-balanced in/out, active daily
  "XmZQkfLtk3xLtbBMenTdaZMxsUBYAsRz1o": "Suspected-Exchange-Hot-Wallet-2",
  // 57,517 DASH · 101,471 tx · balanced in/out
  "XypDdrwkYRdur4FidN52dX4nLcsMfSJRaT": "Suspected-Exchange-Hot-Wallet-3",
};

const DEFAULT_THRESHOLD_PCT = 1; // alert if 24h net change exceeds 1%
const SATS = 100000000;          // 1 DASH = 1e8 duffs
const STATE_KEY = "dash_monitor_state";
const CONFIG_KEY = "dash_monitor_config";
const EXPLORER = "https://insight.dash.org/insight";

// Dash address: Base58Check, mainnet P2PKH starts 'X', P2SH starts '7'.
const DASH_ADDR_RE = /^[X7][1-9A-HJ-NP-Za-km-z]{33}$/;

// Rolling history: hourly samples for the recent ~8 days, thinned to
// one sample per ~6h beyond that, kept ~30.5 days total.
const SAMPLE_EVERY_MS = 55 * 60 * 1000;
const KEEP_MS = 30.5 * 24 * 3600 * 1000;
const DENSE_MS = 8 * 24 * 3600 * 1000;      // full hourly resolution window
const THIN_EVERY_MS = 5.5 * 3600 * 1000;    // resolution beyond that
const DAY_MS = 24 * 3600 * 1000;
const WEEK_MS = 7 * 24 * 3600 * 1000;
const WEEK_MIN_MS = 6 * 24 * 3600 * 1000;
const MONTH_MS = 30 * 24 * 3600 * 1000;
const MONTH_MIN_MS = 26 * 24 * 3600 * 1000;
const REARM_FRACTION = 0.8; // hysteresis: re-arm below 80% of threshold

// --- Node sync guards ---------------------------------------------------
// A node that is behind serves stale balances that LOOK fine — worse than an
// error. So we refuse to sample when out of sync, and say why.
const SYNC_TOLERANCE_BLOCKS = 2;   // 1-2 blocks behind the header tip is normal
// Dash targets ~2.5 min/block. If the height hasn't advanced in this long the
// node is stalled even if it thinks it's at the tip (e.g. peers gone).
const STALL_MS = 30 * 60 * 1000;   // 30 min with no new block = stalled
const SYNC_PUSH_COOLDOWN_MS = 60 * 60 * 1000; // 1 push/hour while degraded

// Price-move correlation flag: fire when a large aggregate net flow in the
// last hour coincides with a price move of the opposite sign.
// "Bullish divergence" = coins leaving exchanges (outflow) while price rises.
// "Bearish divergence" = coins arriving (inflow) while price falls.
const CORR_FLOW_DASH = 2000;     // min |aggregate 1h net flow| to consider
const CORR_PRICE_PCT = 1.0;      // min |1h price move %| to consider
const CORR_COOLDOWN_MS = 3 * 3600 * 1000; // don't re-fire within 3h

// Whale transaction feed. Dash's RPC whitelist blocks block-scanning methods,
// but getaddressdeltas returns per-transaction flows for a given address
// directly — cheaper and more precise than Quai's full block scan. We pull
// deltas in the (cursor..head] height window and keep transfers whose value
// is at least whaleTxPercent (default 0.5%) of combined watched balance.
const WHALE_DEFAULT_PCT = 0.5;
const WHALE_KEEP = 50;              // feed length
const MAX_BLOCKS_PER_RUN = 500;     // cap the delta window per 5-min cycle
// Private watchlist (any-size incoming tx alerts) lives in Redis config as
// config.watchedAddresses = [{addr, label}] — managed from the admin page.
// It is never written to public state; alerts go to push/email only.

// --- Upstash Redis (REST) ---------------------------------------------
const REDIS_URL =
  process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const REDIS_TOKEN =
  process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;

function redisEnvError() {
  if (!REDIS_URL || !REDIS_TOKEN) {
    return "Redis env vars not set. Add UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN, then redeploy.";
  }
  if (!/^https:\/\//.test(REDIS_URL)) {
    return `UPSTASH_REDIS_REST_URL must start with https:// (currently: "${REDIS_URL}"). Use the REST endpoint, not the redis:// TCP string.`;
  }
  if (/:\d+$/.test(REDIS_URL) || REDIS_URL.includes("@")) {
    return `UPSTASH_REDIS_REST_URL looks like a TCP connection string ("${REDIS_URL}"). Use https://<name>.upstash.io with no port.`;
  }
  return null;
}

async function redisCall(path, opts = {}) {
  const r = await fetch(`${REDIS_URL.replace(/\/$/, "")}${path}`, {
    ...opts,
    headers: { Authorization: `Bearer ${REDIS_TOKEN}`, ...(opts.headers || {}) },
  });
  const text = await r.text();
  let j;
  try {
    j = JSON.parse(text);
  } catch (_) {
    throw new Error(
      `Redis endpoint returned non-JSON (HTTP ${r.status}) — check UPSTASH_REDIS_REST_URL. Body starts: ${text.slice(0, 120).replace(/\s+/g, " ")}`
    );
  }
  if (j.error) throw new Error(`Redis error: ${j.error}`);
  return j;
}

async function redisGet(key) {
  const j = await redisCall(`/get/${key}`);
  return j.result ? JSON.parse(j.result) : null;
}

async function redisSet(key, value) {
  await redisCall(`/set/${key}`, { method: "POST", body: JSON.stringify(value) });
}

// --- Dash Core JSON-RPC (HTTP Basic Auth) -------------------------------
// Credentials come ONLY from env vars; never hard-coded, never client-side.
const RPC_URL = process.env.DASH_RPC_URL;
const RPC_USER = process.env.DASH_RPC_USER;
const RPC_PASS = process.env.DASH_RPC_PASS;

function rpcEnvError() {
  if (!RPC_URL || !RPC_USER || !RPC_PASS) {
    return "Dash RPC env vars not set. Add DASH_RPC_URL, DASH_RPC_USER and DASH_RPC_PASS, then redeploy.";
  }
  return null;
}

function duffsToDash(duffs) {
  return Number(duffs) / SATS;
}

// External fetches get a hard timeout so a hung endpoint can't stall the
// whole function until Vercel kills it.
async function fetchWithTimeout(url, opts = {}, ms = 10000) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), ms);
  try {
    return await fetch(url, { ...opts, signal: ctl.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function rpcRaw(method, params = [], id = 1, timeoutMs = 15000) {
  const token = Buffer.from(`${RPC_USER}:${RPC_PASS}`).toString("base64");
  const r = await fetchWithTimeout(
    RPC_URL,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Basic ${token}`,
      },
      body: JSON.stringify({ jsonrpc: "1.0", id, method, params }),
    },
    timeoutMs
  );
  const text = await r.text();
  if (r.status === 401) throw new Error("RPC auth failed (401) — check DASH_RPC_USER / DASH_RPC_PASS");
  if (r.status === 403) throw new Error(`RPC method '${method}' is blocked by the node whitelist (403)`);
  if (r.status === 429) throw new Error("RPC rate-limited (429) — reduce poll frequency");
  let j;
  try {
    j = JSON.parse(text);
  } catch (_) {
    throw new Error(
      `Non-JSON from RPC — HTTP ${r.status}, body starts: ${text.slice(0, 120).replace(/\s+/g, " ")}`
    );
  }
  if (j.error) throw new Error(`RPC error for ${method}: ${JSON.stringify(j.error)}`);
  return j.result;
}

async function rpcBlockNumber() {
  const h = await rpcRaw("getblockcount");
  if (!Number.isInteger(h)) throw new Error(`getblockcount failed: ${JSON.stringify(h).slice(0, 120)}`);
  return h;
}

// Node sync state. getblockcount alone can't tell us if we're at the head — it
// reports our height with no reference. getblockchaininfo gives both `blocks`
// (validated) and `headers` (known best), so headers-blocks is the lag, plus
// verificationprogress as a second signal. All from one whitelisted call.
async function rpcChainInfo() {
  const info = await rpcRaw("getblockchaininfo");
  if (!info || !Number.isInteger(info.blocks)) {
    throw new Error(`getblockchaininfo failed: ${JSON.stringify(info).slice(0, 120)}`);
  }
  const blocks = info.blocks;
  const headers = Number.isInteger(info.headers) ? info.headers : blocks;
  const behind = Math.max(0, headers - blocks);
  const progress = Number.isFinite(info.verificationprogress)
    ? info.verificationprogress
    : null;
  return {
    height: blocks,
    headers,
    behind,
    progress,
    // Synced = caught up to the headers we know about, and (if reported)
    // verification is essentially complete.
    synced: behind <= SYNC_TOLERANCE_BLOCKS && (progress === null || progress > 0.9999),
    ibd: Boolean(info.initialblockdownload),
  };
}

async function rpcGetBalanceOnce(address) {
  const res = await rpcRaw("getaddressbalance", [{ addresses: [address] }]);
  if (!res || res.balance === undefined) {
    throw new Error(`RPC gave no balance for ${address}`);
  }
  return duffsToDash(res.balance);
}

async function rpcGetBalance(address) {
  try {
    return await rpcGetBalanceOnce(address);
  } catch (first) {
    await new Promise((r) => setTimeout(r, 1500));
    return rpcGetBalanceOnce(address); // second attempt; throws if it fails too
  }
}

async function fetchBalances() {
  const out = {};
  const errors = [];
  for (const a of Object.keys(WATCHED)) {
    try {
      out[a] = await rpcGetBalance(a);
    } catch (e) {
      errors.push(`${WATCHED[a]}: ${String((e && e.message) || e).slice(0, 140)}`);
    }
  }
  return { balances: out, errors };
}

async function fetchPrice() {
  try {
    const r = await fetchWithTimeout("https://api.kraken.com/0/public/Ticker?pair=DASHUSD");
    const j = await r.json();
    const key = Object.keys(j.result || {})[0];
    const px = key ? parseFloat(j.result[key].c[0]) : null;
    return Number.isFinite(px) ? px : null;
  } catch (_) {
    return null;
  }
}

// --- History reference lookups ------------------------------------------
// Sample nearest to (now - periodMs); if history is younger than the
// period, fall back to the oldest sample (partial window).
function dayReference(history, addr, nowMs) {
  const candidates = history.filter((s) => s.b[addr] !== undefined);
  if (!candidates.length) return null;
  const target = nowMs - DAY_MS;
  let best = candidates[0];
  let bestDist = Math.abs(new Date(best.t).getTime() - target);
  for (const s of candidates) {
    const dist = Math.abs(new Date(s.t).getTime() - target);
    if (dist < bestDist) {
      bestDist = dist;
      best = s;
    }
  }
  const tMs = new Date(best.t).getTime();
  return { bal: best.b[addr], spanMs: nowMs - tMs, partial: nowMs - tMs < DAY_MS * 0.85 };
}

// Sample nearest to (now - periodMs); null until history is at least minMs old.
function periodReference(history, addr, nowMs, periodMs, minMs) {
  const candidates = history.filter((s) => s.b[addr] !== undefined);
  if (!candidates.length) return null;
  const oldest = new Date(candidates[0].t).getTime();
  if (nowMs - oldest < minMs) return null;
  const target = nowMs - periodMs;
  let best = candidates[0];
  let bestDist = Math.abs(new Date(best.t).getTime() - target);
  for (const s of candidates) {
    const dist = Math.abs(new Date(s.t).getTime() - target);
    if (dist < bestDist) {
      bestDist = dist;
      best = s;
    }
  }
  return { bal: best.b[addr] };
}

// --- Whale transaction scanning ------------------------------------------
// getaddressdeltas returns one row per UTXO EVENT, not per transaction. A
// single spend from a hot wallet produces several rows for the SAME address
// and the SAME txid:
//
//     -50,000   (the input being spent)
//     +48,500   (the change output returning to the same address)
//
// Read row-by-row that looks like a 50,000 outflow AND a 48,500 inflow, when
// the real economic effect is a 1,500 outflow. Change is not a real flow.
//
// So we NET the deltas per (address, txid) before applying any threshold. The
// change output cancels against its own input automatically — no address
// comparison needed, because change returns to the originating address by
// definition. Only the net movement of value in or out of the wallet survives.
function netDeltasByTx(deltas) {
  const byTx = new Map();
  for (const d of deltas || []) {
    const key = d.txid;
    const cur = byTx.get(key) || { txid: d.txid, height: d.height, satoshis: 0, rows: 0 };
    cur.satoshis += Number(d.satoshis);
    cur.rows += 1;
    // Keep the lowest height seen (rows for one tx share a height anyway).
    if (Number.isInteger(d.height) && d.height < cur.height) cur.height = d.height;
    byTx.set(key, cur);
  }
  return [...byTx.values()];
}

async function scanWhaleTxs(prevCursor, head, thresholdDash, watchMap = {}) {
  let from = prevCursor + 1;
  let skipped = 0;
  if (head - from + 1 > MAX_BLOCKS_PER_RUN) {
    skipped = head - from + 1 - MAX_BLOCKS_PER_RUN;
    from = head - MAX_BLOCKS_PER_RUN + 1;
  }
  const found = [];
  const incoming = [];

  // Exchange wallets: large NET flows go into the public whale feed.
  for (const [addr, label] of Object.entries(WATCHED)) {
    const deltas = await rpcRaw("getaddressdeltas", [
      { addresses: [addr], start: from, end: head },
    ]);
    // Net per txid first — otherwise a spend's change output would be counted
    // as a separate large inflow alongside its own input as a large outflow.
    for (const tx of netDeltasByTx(deltas)) {
      const dash = duffsToDash(tx.satoshis);
      // A pure self-send (everything returned as change) nets to ~0 and is
      // dropped here, which is exactly what we want — no value actually moved.
      if (Math.abs(dash) >= thresholdDash) {
        found.push({
          t: new Date().toISOString(), // deltas carry height, not timestamp
          block: tx.height,
          hash: tx.txid,
          // Direction of the NET movement: + = into the wallet, - = out of it.
          from: dash < 0 ? addr : null,
          to: dash > 0 ? addr : null,
          dir: dash > 0 ? "in" : "out",
          dash: Math.abs(dash),
          exchange: label,
          // How many UTXO rows collapsed into this one net figure. >2 means
          // the tx had change; useful for spotting mis-reads later.
          rows: tx.rows,
        });
      }
    }
  }

  // Private watchlist: ANY incoming tx alerts (push/email only, never public).
  for (const [addr, label] of Object.entries(watchMap)) {
    try {
      const deltas = await rpcRaw("getaddressdeltas", [
        { addresses: [addr], start: from, end: head },
      ]);
      // Net per txid so a spend's change output is not reported as an
      // "incoming" payment to the very wallet that just sent the funds.
      for (const tx of netDeltasByTx(deltas)) {
        const dash = duffsToDash(tx.satoshis);
        if (dash > 0) {
          incoming.push({
            t: new Date().toISOString(),
            block: tx.height,
            hash: tx.txid,
            to: addr,
            dash,
            label,
            rows: tx.rows,
          });
        }
      }
    } catch (e) {
      // A bad watchlist entry must not break the run.
      console.error(`watchlist delta failed for ${addr}:`, e);
    }
  }

  return { found, incoming, lastBlock: head, scanned: head - from + 1, skipped };
}

function tagAddress(addr) {
  if (!addr) return null;
  for (const [a, name] of Object.entries(WATCHED)) {
    if (a === addr) return name;
  }
  return null;
}

// --- Push via ntfy.sh ---------------------------------------------------
async function sendPush(title, body, tags = "rotating_light,whale", email = false) {
  const topic = process.env.NTFY_TOPIC;
  if (!topic) return;
  // HTTP headers are Latin-1 only — emoji/unicode in the Title header throws.
  // ntfy renders emoji via the Tags header instead (e.g. "zap" -> ⚡).
  const safeTitle = title.replace(/[^\x20-\x7E]/g, "").replace(/\s+/g, " ").trim();
  const headers = {
    Title: safeTitle,
    Priority: "high",
    Tags: tags,
  };
  // Optional email copy of this notification (ntfy Email header).
  if (email && process.env.ALERT_EMAIL) headers.Email = process.env.ALERT_EMAIL;
  try {
    await fetchWithTimeout(`https://ntfy.sh/${topic}`, {
      method: "POST",
      headers,
      body,
    }, 8000);
  } catch (e) {
    // A push failure must never fail the run (or silence the heartbeat).
    console.error("ntfy push failed:", e);
  }
}

// --- Handler ------------------------------------------------------------
module.exports = async (req, res) => {
  try {
    const secret = process.env.CRON_SECRET;
    const auth = req.headers["authorization"] || "";
    const qsSecret = (req.query && req.query.secret) || "";
    if (!secret || (auth !== `Bearer ${secret}` && qsSecret !== secret)) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const envErr = redisEnvError() || rpcEnvError();
    if (envErr) return res.status(500).json({ error: envErr });

    const now = new Date().toISOString();
    const nowMs = Date.now();
    const config = (await redisGet(CONFIG_KEY)) || {};
    const thresholdPct =
      Number(config.thresholdPercent) > 0
        ? Number(config.thresholdPercent)
        : DEFAULT_THRESHOLD_PCT;

    // --- Node sync gate ---------------------------------------------------
    // Check sync BEFORE sampling. A node that is behind returns balances that
    // are simply wrong-but-plausible, which would silently poison the history,
    // fire bogus alerts, and skew the 24h deltas. Better to skip the sample and
    // say so than to record bad data.
    const state = (await redisGet(STATE_KEY)) || { exchanges: {}, alerts: [] };
    if (!Array.isArray(state.history)) state.history = [];
    if (!Array.isArray(state.alerts)) state.alerts = [];
    if (!state.exchanges) state.exchanges = {};

    let chain = null;
    let chainErr = null;
    try {
      chain = await rpcChainInfo();
    } catch (e) {
      chainErr = String((e && e.message) || e).slice(0, 160);
      console.error("chain info failed:", e);
    }

    if (chain) {
      // Stall detection: height frozen for too long means the node is stuck
      // even when it believes it is at the header tip (e.g. lost all peers).
      const prevChain = state.chain || {};
      const advanced = prevChain.height === undefined || chain.height > prevChain.height;
      const sinceAdvanceMs = advanced
        ? 0
        : nowMs - new Date(prevChain.advancedAt || prevChain.at || now).getTime();
      const stalled = !advanced && sinceAdvanceMs >= STALL_MS;

      chain.stalled = stalled;
      chain.stalledMin = stalled ? Math.round(sinceAdvanceMs / 60000) : 0;
      chain.at = now;
      chain.advancedAt = advanced ? now : prevChain.advancedAt || prevChain.at || now;

      const healthy = chain.synced && !stalled;

      if (!healthy) {
        // Graceful degradation: keep every previous reading, record WHY we are
        // not sampling, and surface it. No history append, no alerts, no whale
        // scan — none of which would be trustworthy right now.
        const why = chain.ibd
          ? `node in initial block download (${chain.behind.toLocaleString("en-US")} blocks behind)`
          : stalled
          ? `node stalled — no new block in ${chain.stalledMin} min (height ${chain.height.toLocaleString("en-US")})`
          : `node ${chain.behind.toLocaleString("en-US")} blocks behind the tip` +
            (chain.progress !== null ? ` (${(chain.progress * 100).toFixed(3)}% verified)` : "");

        state.chain = chain;
        state.syncOk = false;
        state.lastError = { time: now, message: `Skipped sample — ${why}. Showing last good data.` };
        await redisSet(STATE_KEY, state);

        // Rate-limited push so a long resync doesn't spam the phone.
        const cooldownOk =
          !state.lastSyncPushAt ||
          nowMs - new Date(state.lastSyncPushAt).getTime() > SYNC_PUSH_COOLDOWN_MS;
        if (cooldownOk) {
          await sendPush("Dash node out of sync", `${why}\nSampling paused until it catches up.`, "warning");
          state.lastSyncPushAt = now;
          await redisSet(STATE_KEY, state);
        }

        // 503: degraded, not broken. Cron retries in 5 min; data is intact.
        return res.status(503).json({
          ok: false,
          skipped: "out_of_sync",
          reason: why,
          height: chain.height,
          headers: chain.headers,
          behind: chain.behind,
          stalled,
        });
      }
    } else {
      // Could not determine sync state at all. Do not guess — skip the sample.
      state.syncOk = false;
      state.lastError = {
        time: now,
        message: `Skipped sample — could not read node sync state: ${chainErr}`,
      };
      await redisSet(STATE_KEY, state);
      return res.status(503).json({ ok: false, skipped: "sync_unknown", error: chainErr });
    }

    // Node is synced and advancing — safe to sample.
    const chainHeight = chain.height;
    state.chain = chain;
    state.syncOk = true;

    const { balances, errors } = await fetchBalances();
    const price = await fetchPrice();

    if (price !== null) state.price = { usd: price, at: now };

    if (Object.keys(balances).length === 0) {
      // Total RPC failure: record it so the dashboard can say so; keep old data.
      state.lastError = { time: now, message: `RPC unreachable — ${errors.join(" | ")}` };
      await redisSet(STATE_KEY, state);
      return res.status(502).json({ ok: false, error: state.lastError.message });
    }
    if (errors.length) {
      state.lastError = { time: now, message: `Partial data — ${errors.join(" | ")}` };
    } else {
      delete state.lastError; // healthy run clears the banner
    }

    // Append hourly sample, prune old ones
    const lastSample = state.history.length
      ? new Date(state.history[state.history.length - 1].t).getTime()
      : 0;
    if (nowMs - lastSample >= SAMPLE_EVERY_MS || !state.history.length) {
      const sample = { t: now, b: balances };
      if (price !== null) sample.p = price;
      state.history.push(sample);
    }
    // Prune beyond 30.5 days, and thin resolution for samples older than
    // the dense window (keep ~6h spacing) to cap storage size.
    const kept = [];
    let lastOldKept = 0;
    for (const s of state.history) {
      const age = nowMs - new Date(s.t).getTime();
      if (age > KEEP_MS) continue;
      if (age <= DENSE_MS) {
        kept.push(s);
      } else {
        const tMs = new Date(s.t).getTime();
        if (tMs - lastOldKept >= THIN_EVERY_MS) {
          kept.push(s);
          lastOldKept = tMs;
        }
      }
    }
    state.history = kept;

    const fired = [];
    let corrFired = null;

    // Combined holdings across all watched wallets — the shared denominator
    // for per-exchange alerts (same basis as the whale feed threshold).
    const totalNow = Object.values(balances).reduce((s, v) => s + v, 0);

    for (const [addr, bal] of Object.entries(balances)) {
      const name = WATCHED[addr];
      const prev = state.exchanges[addr] || {};

      const dayRef = dayReference(state.history, addr, nowMs);
      const weekRef = periodReference(state.history, addr, nowMs, WEEK_MS, WEEK_MIN_MS);
      const monthRef = periodReference(state.history, addr, nowMs, MONTH_MS, MONTH_MIN_MS);

      const dayDelta = dayRef ? bal - dayRef.bal : null;
      const dayPct =
        dayRef && dayRef.bal > 0 ? (dayDelta / dayRef.bal) * 100 : null;
      const weekDelta = weekRef ? bal - weekRef.bal : null;
      const weekPct =
        weekRef && weekRef.bal > 0 ? (weekDelta / weekRef.bal) * 100 : null;
      const monthDelta = monthRef ? bal - monthRef.bal : null;
      const monthPct =
        monthRef && monthRef.bal > 0 ? (monthDelta / monthRef.bal) * 100 : null;

      // Edge-triggered alert on the 24h (or partial-window) change.
      // Trigger basis: the wallet's move as a percent of COMBINED holdings
      // across all watched exchanges, so every wallet alerts at the same
      // absolute DASH move (same basis as the whale feed).
      let alertActive = Boolean(prev.alertActive);
      const basisPct =
        dayDelta !== null && totalNow > 0 ? (dayDelta / totalNow) * 100 : null;
      if (basisPct !== null) {
        const magnitude = Math.abs(basisPct);
        if (!alertActive && magnitude > thresholdPct) {
          alertActive = true;
          const dir = dayDelta > 0 ? "INFLOW" : "OUTFLOW";
          const windowLabel = dayRef.partial
            ? `${Math.round(dayRef.spanMs / 3600000)}h`
            : "24h";
          const alert = {
            time: now,
            exchange: name,
            address: addr,
            direction: dir,
            window: windowLabel,
            delta: dayDelta,
            deltaPct: basisPct,   // % of combined holdings (what triggered)
            walletPct: dayPct,    // % of this wallet's own balance (context)
            threshold: thresholdPct,
            from: dayRef.bal,
            to: bal,
          };
          fired.push(alert);
          state.alerts.unshift(alert);
        } else if (alertActive && magnitude < thresholdPct * REARM_FRACTION) {
          alertActive = false; // change subsided; re-arm
        }
      }

      state.exchanges[addr] = {
        name,
        balance: bal,
        dayDelta,
        dayPct,
        daySpanH: dayRef ? Math.round(dayRef.spanMs / 3600000) : null,
        dayPartial: dayRef ? dayRef.partial : true,
        weekDelta,
        weekPct,
        monthDelta,
        monthPct,
        alertActive,
        checked: now,
      };
    }

    // Self-clean: drop any stored wallet no longer in WATCHED (e.g. after an
    // address swap), so stale cards can't linger in the dashboard.
    const watchedSet = new Set(Object.keys(WATCHED));
    for (const addr of Object.keys(state.exchanges)) {
      if (!watchedSet.has(addr)) delete state.exchanges[addr];
    }
    for (const s of state.history) {
      if (s && s.b) {
        for (const addr of Object.keys(s.b)) {
          if (!watchedSet.has(addr)) delete s.b[addr];
        }
      }
    }

    // --- Aggregate total across all watched wallets (for trend chart) -----
    state.totalExchange = { dash: totalNow, at: now };

    // --- 24h gross transfer volume across the hot wallets ------------------
    // Sum of |balance change| between consecutive samples in the last 24h,
    // across all watched wallets (churn, not net).
    {
      const dayAgo = nowMs - DAY_MS;
      let vol = 0;
      const recent = state.history.filter((s) => new Date(s.t).getTime() >= dayAgo);
      for (let i = 1; i < recent.length; i++) {
        for (const a of Object.keys(WATCHED)) {
          const p = recent[i - 1].b[a];
          const q = recent[i].b[a];
          if (p !== undefined && q !== undefined) vol += Math.abs(q - p);
        }
      }
      // Include movement since the most recent sample
      if (recent.length) {
        const lastS = recent[recent.length - 1];
        for (const a of Object.keys(WATCHED)) {
          if (lastS.b[a] !== undefined && balances[a] !== undefined) {
            vol += Math.abs(balances[a] - lastS.b[a]);
          }
        }
      }
      const oldestRecent = recent.length ? new Date(recent[0].t).getTime() : nowMs;
      const coverageH = Math.round((nowMs - oldestRecent) / 3600000);
      state.volume24h = { dash: vol, at: now, partial: coverageH < 23, hours: coverageH };
    }

    // --- Whale transaction feed --------------------------------------------
    const whalePct =
      Number(config.whaleTxPercent) > 0 && Number(config.whaleTxPercent) <= 10
        ? Number(config.whaleTxPercent)
        : WHALE_DEFAULT_PCT;
    const whaleThreshold = totalNow * (whalePct / 100);
    if (!Array.isArray(state.whaleTxs)) state.whaleTxs = [];
    let whaleFound = 0;
    const incomingAlerts = [];
    // Private watchlist from config -> map {addr: label}
    const watchMap = {};
    if (Array.isArray(config.watchedAddresses)) {
      for (const w of config.watchedAddresses) {
        if (w && typeof w.addr === "string" && DASH_ADDR_RE.test(w.addr)) {
          watchMap[w.addr] =
            (typeof w.label === "string" && w.label.trim()) ||
            w.addr.slice(0, 10) + "…";
        }
      }
    }
    try {
      const head = chainHeight; // guaranteed synced & non-null past the sync gate
      const prevCursor =
        state.whaleScan && Number.isInteger(state.whaleScan.lastBlock)
          ? state.whaleScan.lastBlock
          : null;
      if (prevCursor === null) {
        // First run: set the cursor, scan starts next cycle.
        state.whaleScan = { lastBlock: head, at: now, percent: whalePct, threshold: whaleThreshold };
      } else if (head > prevCursor) {
        const scan = await scanWhaleTxs(prevCursor, head, whaleThreshold, watchMap);
        for (const tx of scan.found) {
          tx.fromTag = tagAddress(tx.from);
          tx.toTag = tagAddress(tx.to);
        }
        whaleFound = scan.found.length;
        state.whaleTxs = [...scan.found.reverse(), ...state.whaleTxs].slice(0, WHALE_KEEP);

        // Watched-address hits go to push/email ONLY — never into public state.
        for (const tx of scan.incoming || []) incomingAlerts.push(tx);

        state.whaleScan = {
          lastBlock: scan.lastBlock,
          at: now,
          percent: whalePct,
          threshold: whaleThreshold,
          scanned: scan.scanned,
          skipped: scan.skipped || 0,
        };
      }
    } catch (e) {
      // Scan failure never fails the check; cursor stays put and we retry next cycle.
      console.error("whale scan failed:", e);
      state.whaleScan = {
        ...(state.whaleScan || {}),
        at: now,
        percent: whalePct,
        threshold: whaleThreshold,
        error: String((e && e.message) || e).slice(0, 160),
      };
    }

    // --- Price-move correlation flag --------------------------------------
    // Compare the last ~1h: aggregate net flow vs price move. A large flow
    // opposite in sign to the price move is the actionable divergence.
    try {
      const hourAgo = nowMs - 60 * 60 * 1000;
      const past = state.history
        .filter((s) => new Date(s.t).getTime() <= hourAgo + 20 * 60 * 1000)
        .sort(
          (a, b) =>
            Math.abs(new Date(a.t).getTime() - hourAgo) -
            Math.abs(new Date(b.t).getTime() - hourAgo)
        )[0];

      if (past && price !== null) {
        const pastTotal = Object.keys(WATCHED).reduce(
          (s, a) => s + (past.b[a] !== undefined ? past.b[a] : 0),
          0
        );
        const flow1h = totalNow - pastTotal; // + = net onto exchanges
        const pastPrice = past.p;
        const cooldownOk =
          !state.lastCorrAt ||
          nowMs - new Date(state.lastCorrAt).getTime() > CORR_COOLDOWN_MS;

        if (pastPrice && cooldownOk && Math.abs(flow1h) >= CORR_FLOW_DASH) {
          const pricePct = ((price - pastPrice) / pastPrice) * 100;
          if (Math.abs(pricePct) >= CORR_PRICE_PCT) {
            const outflowUp = flow1h < 0 && pricePct > 0; // coins leaving + price up
            const inflowDown = flow1h > 0 && pricePct < 0; // coins arriving + price down
            if (outflowUp || inflowDown) {
              const kind = outflowUp ? "BULLISH" : "BEARISH";
              const corr = {
                time: now,
                type: "correlation",
                kind,
                flow1h,
                pricePct,
                fromPrice: pastPrice,
                toPrice: price,
              };
              state.alerts.unshift(corr);
              state.lastCorrAt = now;
              corrFired = corr;
            }
          }
        }
      }
    } catch (e) {
      console.error("correlation check failed:", e);
    }

    // Approved public counterparty tags -> state (single-read status stays cheap)
    const publicTags = {};
    if (Array.isArray(config.publicTags)) {
      for (const w of config.publicTags) {
        if (w && typeof w.addr === "string" && DASH_ADDR_RE.test(w.addr)) {
          const lbl = typeof w.label === "string" && w.label.trim();
          if (lbl) publicTags[w.addr] = lbl.slice(0, 40);
        }
      }
    }
    state.publicTags = publicTags;
    state.alerts = state.alerts.slice(0, 20);

    state.lastCheck = now;
    state.thresholdPercent = thresholdPct;
    await redisSet(STATE_KEY, state);

    for (const a of fired) {
      await sendPush(
        `${a.exchange} ${a.window} ${a.direction}: ${Math.abs(a.deltaPct).toFixed(2)}%`,
        `${a.delta > 0 ? "+" : "−"}${Math.round(Math.abs(a.delta)).toLocaleString("en-US")} DASH over ${a.window}\n` +
          `${a.from.toLocaleString("en-US")} → ${a.to.toLocaleString("en-US")}\n` +
          `${EXPLORER}/address/${a.address}`
      );
    }

    if (corrFired) {
      const c = corrFired;
      const flowDir = c.flow1h < 0 ? "outflow" : "inflow";
      const priceDir = c.pricePct > 0 ? "up" : "down";
      const headline =
        c.kind === "BULLISH"
          ? "Divergence: outflow + price up"
          : "Divergence: inflow + price down";
      await sendPush(
        headline,
        `Last hour: ${Math.round(Math.abs(c.flow1h)).toLocaleString("en-US")} DASH ${flowDir} ` +
          `while price ${priceDir} ${Math.abs(c.pricePct).toFixed(2)}%\n` +
          `$${c.fromPrice} → $${c.toPrice}`,
        "zap"
      );
    }

    for (const tx of incomingAlerts) {
      await sendPush(
        `Incoming: ${tx.label}`,
        `Received ${Math.round(tx.dash).toLocaleString("en-US")} DASH\n` +
          `${EXPLORER}/tx/${tx.hash}`,
        "inbox_tray,bell",
        true
      );
    }

    // Dead man's switch: report a fully successful run. Silence -> alert.
    // Best-effort: a heartbeat outage must never fail the check itself.
    if (process.env.HEALTHCHECK_URL) {
      try {
        await fetchWithTimeout(process.env.HEALTHCHECK_URL, {}, 5000);
      } catch (e) {
        console.error("heartbeat ping failed:", e);
      }
    }

    return res.status(200).json({
      ok: true,
      checked: now,
      thresholdPercent: thresholdPct,
      whaleTxPercent: whalePct,
      whaleTxsFound: whaleFound,
      alertsFired: fired.length,
      balances,
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String((e && e.message) || e) });
  }
};
