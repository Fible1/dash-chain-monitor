// Shared helper: talk to a Dash Core node over JSON-RPC with HTTP Basic Auth.
//
// SECURITY: credentials come ONLY from environment variables set in Vercel.
// Never hard-code them and never expose them to the browser. All calls happen
// inside serverless functions (api/*), never client-side.

const RPC_URL = process.env.DASH_RPC_URL;
const RPC_USER = process.env.DASH_RPC_USER;
const RPC_PASS = process.env.DASH_RPC_PASS;

function authHeader() {
  if (!RPC_URL || !RPC_USER || !RPC_PASS) {
    throw new Error("Missing DASH_RPC_URL / DASH_RPC_USER / DASH_RPC_PASS env vars");
  }
  const token = Buffer.from(`${RPC_USER}:${RPC_PASS}`).toString("base64");
  return `Basic ${token}`;
}

async function rpc(method, params = [], id = 1) {
  const r = await fetch(RPC_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: authHeader() },
    body: JSON.stringify({ jsonrpc: "1.0", id, method, params }),
  });
  const text = await r.text();
  if (r.status === 401) throw new Error("RPC auth failed (401)");
  if (r.status === 403) throw new Error(`RPC method '${method}' blocked by whitelist (403)`);
  if (r.status === 429) throw new Error("RPC rate-limited (429)");
  let j;
  try { j = JSON.parse(text); }
  catch (_) { throw new Error(`Non-JSON from RPC (HTTP ${r.status}): ${text.slice(0,120)}`); }
  if (j.error) throw new Error(`RPC error for ${method}: ${JSON.stringify(j.error)}`);
  return j.result;
}

// getaddressbalance: current balance for one address (satoshis).
async function getAddressBalance(address) {
  const res = await rpc("getaddressbalance", [{ addresses: [address] }]);
  return {
    balance: res.balance,
    spendable: res.balance_spendable,
    immature: res.balance_immature,
    received: res.received,
  };
}

// getaddressdeltas: per-tx balance changes for one address.
// Optional start/end block height to limit the window.
async function getAddressDeltas(address, start, end) {
  const q = { addresses: [address] };
  if (start != null) q.start = start;
  if (end != null) q.end = end;
  const res = await rpc("getaddressdeltas", [q]);
  // Each: { satoshis, txid, index, blockindex, height, address }
  return res;
}

module.exports = { rpc, getAddressBalance, getAddressDeltas, RPC_URL };
