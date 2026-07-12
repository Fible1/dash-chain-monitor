# NodeSoda · DASH Exchange Flow Monitor

Port of the Quai exchange monitor to Dash. Vercel serverless + Upstash Redis +
ntfy push + admin panel + public tag suggestions.

## Watched wallets
Edit `WATCHED` in `api/check.js`. Labels are deliberately neutral — these
addresses behave like exchange hot/consolidation wallets, but none is confirmed
as belonging to any named exchange:

    XsqwdGfza8Rf3GrSom988Rxhaprecw3XD6  Suspected-Exchange-Hot-Wallet-1  (85,541 DASH)
    XmZQkfLtk3xLtbBMenTdaZMxsUBYAsRz1o  Suspected-Exchange-Hot-Wallet-2  (near-balanced, active daily)
    XypDdrwkYRdur4FidN52dX4nLcsMfSJRaT  Suspected-Exchange-Hot-Wallet-3  (balanced in/out)

Dashboard cards are sorted by current balance (largest first), so the ordering
stays true as balances move.

## Endpoints
- `/api/check`   — 5-min cron sampler (balances, alerts, whale feed, correlation)
- `/api/status`  — public dashboard state
- `/api/history` — balance samples + Kraken DASHUSD OHLC for the price chart
- `/api/config`  — GET public thresholds; POST (secret) to update
- `/api/suggest-tag` — public tag suggestions -> pending queue
- `/admin.html`  — thresholds, private watchlist, tag approvals (needs CRON_SECRET)

## Env vars (Vercel -> Settings -> Environment Variables)
DASH_RPC_URL, DASH_RPC_USER, DASH_RPC_PASS,
UPSTASH_REDIS_REST_URL, UPSTASH_REDIS_REST_TOKEN,
NTFY_TOPIC, CRON_SECRET
Optional: ALERT_EMAIL, HEALTHCHECK_URL

## Dash vs Quai — what changed and why
- **Whale feed**: Quai scanned every block's transactions. Dash's RPC whitelist
  blocks block-scanning methods, so this uses `getaddressdeltas` over the
  (cursor..head] height window instead — cheaper and more precise. Trade-off:
  deltas expose the *direction* of each flow but NOT the counterparty address,
  so feed rows show "deposit/withdrawal + exchange", not "from → to".
- **Address validation**: Ethereum `0x`+40hex -> Dash Base58Check (`X…`/`7…`).
  Base58 is case-sensitive, so addresses are never lowercased.
- **Units**: wei/BigInt -> duffs (1 DASH = 1e8).
- **Price**: Kraken DASHUSD.

## Requires on the RPC node
`-addressindex=1` and whitelist: `getblockcount`, `getaddressbalance`,
`getaddressdeltas`. (`getmempoolinfo` is blocked and unused.)

## Security
Credentials live only in Vercel env vars, used server-side. Never commit `.env`.
