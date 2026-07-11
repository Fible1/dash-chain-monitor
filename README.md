# NodeSoda · Dash Exchange Flow Monitor

Vercel-hosted monitor for Dash exchange wallet flows, mirroring the Quai
dashboard: serverless functions + Upstash Redis + ntfy push + Chart.js + native
Vercel cron.

## What it monitors

- Per-wallet balance (currently Binance only)
- Inflow/outflow diff between polls
- Transaction feed built from getaddressdeltas
- Whale alerts via ntfy on any single flow >= WHALE_THRESHOLD_DASH (default 1000)

## Requirements on the RPC node

This build depends on the node running with `-addressindex=1` and the
`external_rpc` whitelist including `getaddressbalance` and `getaddressdeltas`
(both confirmed working). Also uses `getblockcount`.

## Adding more wallets

Edit `WATCHED` in `api/check.js` — add OKX / KuCoin / MEXC addresses as
`"address": "Label"` pairs. The dashboard renders a card per wallet automatically.

## Files

- `api/_rpc.js` — Dash Core JSON-RPC helper (Basic Auth, env vars only)
- `api/check.js` — cron poller; balances, deltas, whale alerts
- `api/status.js` — public read-only status
- `index.html` — dashboard
- `vercel.json` — native cron (`*/5 * * * *`, requires Vercel Pro)

## Security

Credentials live ONLY in Vercel env vars, used server-side. Nothing sensitive
ships to the browser. Never commit `.env`.
