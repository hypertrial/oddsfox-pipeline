# Overview

**oddsfox** is a self-hosted, MIT-licensed FOSS data lake creator for prediction-market research.
v0.2.0 builds a local Polymarket and Kalshi lake end-to-end: fetch, normalize, catalog, compute, query, and serve.

It answers five research questions locally:

1. What markets exist?
2. What were their probabilities over time?
3. How liquid were they?
4. How did they resolve?
5. How accurate/calibrated were they?

## Non-goals

- Trading, signing, wallets
- Hosted data mirrors
- Order submission, portfolio, balances, fills, or user-specific exchange data
- On-chain archive reconstruction

## Success demo

```bash
oddsfox init
oddsfox sync markets --active
oddsfox snapshot books --active --top-volume 100
oddsfox compute liquidity --active
oddsfox serve
```
