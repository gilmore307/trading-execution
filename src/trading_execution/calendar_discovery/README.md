# calendar_discovery bundle

`calendar_discovery` fetches official calendar pages or feeds and normalizes release events into `release_calendar.csv`.

It is for release dates/times, not macro values. Use it to discover `release_time` inputs for later `macro_data` runs.

Run a task key with:

```bash
PYTHONPATH=src python3 -m trading_data.data_sources.calendar_discovery path/to/task_key.json --run-id calendar_discovery_run_<id>
```

Supported params:

- `calendar_source` — e.g. `fomc_calendar`, `bls_release_calendar`, or a custom source label.
- `url` — optional for known sources, required for custom official calendars unless search mode is enabled.
- `search` — optional boolean; when true, discover an official URL with the shared `trading_web_search` Brave helper.
- `search_query` — optional explicit search query; otherwise known source defaults are used.
- `search_count` — optional result count, default 5.
- `format` — optional `auto`, `ics`, `json`, `fomc_html`, or `bls_html`.

Search mode is discovery only. Fetched URLs must still be on an approved official domain: BLS, Census, BEA, Treasury Fiscal Data, FRED/St. Louis Fed, or Federal Reserve.

Outputs:

- `request_manifest.json` — sanitized source URL, status, content type, and params.
- `cleaned/release_calendar.jsonl` and `cleaned/schema.json`.
- `saved/release_calendar.csv` with `event_id,calendar_source,event_name,release_time,event_date,timezone,source_url,raw_summary`.
- `completion_receipt.json` at task root.

Rules:

- Prefer official government or issuing-agency URLs.
- Third-party calendars are secondary references only unless explicitly approved.
- Raw HTML/ICS/JSON is not persisted by default.
