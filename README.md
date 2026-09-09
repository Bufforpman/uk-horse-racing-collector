# UK Horse Racing Prediction Pipeline

A free-data, legal-only quant pipeline for UK horse racing. Point-in-time
correct by design — no feature is ever built from information that
wasn't available before the race went off.

## Status

Data collection phase. Accumulating daily snapshots via GitHub Actions.
Not enough history yet for feature engineering or modelling.

## Data source

The Racing API (theracingapi.com), free tier only:
- `/v1/racecards/free` — today's declared racecards
- `/v1/results/today/free` — today's results

Free tier is **today-only** — no historical backfill available without
a paid Standard plan. This is why daily collection matters: a missed
day is a day of history gone for good.

## Automation

`.github/workflows/daily_collector.yml` runs `daily_collector.py` on a
schedule (22:00 UTC daily, after UK racing finishes) via GitHub Actions.
No manual steps required. It:

1. Pulls today's racecards + results
2. Filters to GB races only (free tier also returns other regions)
3. Loads into `raw.*` DuckDB tables (immutable, timestamped by `as_of_utc`,
   idempotent — safe to re-run same day without duplicating)
4. Rebuilds `clean.*` tables (typed, harmonised field names between the
   racecards/results endpoints, normalised horse names)
5. Commits the updated `horse_racing_pipeline/` folder back to this repo

Credentials are stored as GitHub Actions repository secrets
(`RACING_API_USERNAME`, `RACING_API_PASSWORD`) — never in code.

## Data layout

## Why GB-only, why point-in-time

The Racing API free tier mixes in non-UK races. This project is scoped
to UK racing only, filtered at ingestion.

Ratings, form, and trainer/jockey stats all get revised after the fact.
Every feature must be computable using only what was knowable strictly
before a race's off time.

## Legal note

No scraping. Free-tier API only, used within its terms. rpscrape and
similar scrapers were deliberately ruled out due to Racing Post's terms
of service and UK database right.

## Next steps

- Let history accumulate (weeks, not days)
- Step 5: as-of feature engineering (form, days-since-run, trainer/jockey
  strike rates, rating-relative-to-field)
- Resolve Betfair BSP ingestion for market benchmarking
