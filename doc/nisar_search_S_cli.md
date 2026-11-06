# seppo_nisar_search_S -- CLI Reference

Search NISAR S-band (`NISAR_SSAR_*`) products through the
[ISRO Bhoonidhi API](https://bhoonidhi.nrsc.gov.in/bhoonidhi-api/). It takes the
same filter and output options as [`seppo_nisar_search`](nisar_search_cli.md),
which searches NASA Earthdata.

## Credentials

Add your Bhoonidhi account to `~/.netrc` (mode 600). If there is no entry and
stdin is a terminal, a prompt is shown.

```
machine bhoonidhi-api.nrsc.gov.in
    login <your_userid>
    password <your_password>
```

An entry under the portal host, `bhoonidhi.nrsc.gov.in`, is also accepted.

Tokens are cached in `~/.cache/openseppo/bhoonidhi_token.json` (mode 600) and
reused between runs, because the API allows 20 auth requests per hour per IP and
a limited number of active sessions per user. If login fails with "Max sessions
already active", run `seppo_nisar_search_S --logout` on the machine holding the
session.

## How it differs from seppo_nisar_search

- **Filters.** Collection, time, area, track, frame, cycle, direction and online
  status are evaluated by the API, along with the Urgent Response exclusion. Other
  column filters and the latest-release selection are applied afterwards, so
  `--limit` counts products before those steps.
- **Collections.** Each product searches `NISAR_SSAR_{product}` and then, for GCOV,
  GSLC and RSLC, `NISAR_SSAR-Beta_{product}`, one request at a time. `--collection`
  overrides this.
- **Time ranges.** The API rejects open-ended ranges and ranges over 366 days, so
  long ranges become several requests. With only `--start_time_before`, the search
  starts from 2025-07-01; with only `--start_time_after`, it ends now.
- **URLs.** `url` is the Bhoonidhi download endpoint, which needs a Bearer token.
  There is no `s3://` URL, no `url_https` column and no `--https` option. The
  columns `granule` and `online` are added.
- **Offline products.** Products with `online` = `N` cannot be downloaded through
  the API until they are ordered through Bhoonidhi Browse&Order. They are returned
  with a note on stderr; `--online_only` excludes them.
- **Products.** SME2 is not listed among the Bhoonidhi collections.
- **`--union_geojson`** searches the bounding envelope of all features.

## Examples

```bash
# Show the search request without logging in
seppo_nisar_search_S --bbox 76 10 78 12 --dryrun

# List the collections available to your account
seppo_nisar_search_S --list_collections

# GCOV download URLs for an area and date range, online products only
seppo_nisar_search_S --bbox 76 10 78 12 --start_time_after 2026-01-01 --online_only

# CSV of GSLC products on descending track 64
seppo_nisar_search_S --product GSLC --track 64 --direction D --format csv -o results.csv

# One URL file per (track, direction, frame)
seppo_nisar_search_S --bbox 76 10 78 12 --group -o urls/

# Unmodified API records
seppo_nisar_search_S --limit 2 --raw
```

Run `seppo_nisar_search_S --help` for the full option list.
