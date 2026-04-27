# calendar_discovery bundle

`calendar_discovery` fetches official or explicitly approved market calendar pages/feeds and normalizes release events into `release_calendar.csv`.

It is for future/realtime scheduling dates and approximate release windows, not historical source payloads or model-ready facts. Use it to discover `release_time` inputs for later acquisition runs and event scheduling.

Run a task key with:

```bash
PYTHONPATH=src python3 -m trading_execution.calendar_discovery path/to/task_key.json --run-id calendar_discovery_run_<id>
```

Supported params:

- `calendar_source` — e.g. `fomc_calendar`, `bls_release_calendar`, `nasdaq_earnings_calendar`, or a custom source label.
- `url` — optional for known sources, required for custom official calendars unless search mode is enabled.
- `search` — optional boolean; when true, discover an official URL with the shared `trading_web_search` Brave helper.
- `search_query` — optional explicit search query; otherwise known source defaults are used.
- `search_count` — optional result count, default 5.
- `format` — optional `auto`, `ics`, `json`, `fomc_html`, `bls_html`, or `nasdaq_earnings_json`.
- `date` — optional for `nasdaq_earnings_calendar`; defaults to the current America/New_York date and is sent to Nasdaq's calendar API as `YYYY-MM-DD`.

Search mode is discovery only. Fetched URLs must still be on an approved source domain: BLS, Census, BEA, Treasury Fiscal Data, FRED/St. Louis Fed, Federal Reserve, or Nasdaq.

Outputs:

- `request_manifest.json` — sanitized source URL, status, content type, and params.
- `cleaned/release_calendar.jsonl` and `cleaned/schema.json`.
- `saved/release_calendar.csv` with `event_id,calendar_source,event_name,release_time,event_date,timezone,source_url,raw_summary`.
- `completion_receipt.json` at task root.

Rules:

- Prefer official government, exchange, or issuing-agency URLs.
- Third-party calendars are secondary references only unless explicitly approved. Nasdaq earnings-calendar rows are allowed here because the user explicitly approved Nasdaq as the near-term earnings scheduling source.
- `nasdaq_earnings_calendar` phase labels are approximate scheduling windows: pre-market is normalized to 08:00 ET, after-hours to 16:00 ET, and unknown time to 00:00 ET.
- Raw HTML/ICS/JSON is not persisted by default.
