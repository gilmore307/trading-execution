"""Official release-calendar acquisition bundle.

This bundle fetches official calendar pages/feeds and normalizes release events
into a small CSV. It intentionally does not decide which calendar fields future
models need; it preserves enough source evidence to derive macro release_time
values later.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timezone
from html import unescape
from pathlib import Path
from urllib.parse import urlparse
from typing import Any
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")
UTC = timezone.utc
BUNDLE = "calendar_discovery"
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_URLS = {
    "fomc_calendar": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
    "bls_release_calendar": "https://www.bls.gov/schedule/news_release/",
    "nasdaq_earnings_calendar": "https://api.nasdaq.com/api/calendar/earnings",
}
OFFICIAL_DOMAIN_SUFFIXES = (
    "bls.gov",
    "census.gov",
    "bea.gov",
    "fiscaldata.treasury.gov",
    "fred.stlouisfed.org",
    "federalreserve.gov",
    "nasdaq.com",
)
DEFAULT_SEARCH_QUERIES = {
    "bls_release_calendar": "site:bls.gov schedule economic news release calendar",
    "census_release_calendar": "site:census.gov economic indicators release calendar",
    "bea_release_calendar": "site:bea.gov release schedule full",
    "fred_release_calendar": "site:fred.stlouisfed.org releases calendar",
    "fomc_calendar": "site:federalreserve.gov FOMC calendar",
    "nasdaq_earnings_calendar": "site:nasdaq.com/market-activity/earnings earnings calendar",
}
CALENDAR_FIELDS = [
    "event_id",
    "calendar_source",
    "event_name",
    "release_time",
    "event_date",
    "timezone",
    "source_url",
    "raw_summary",
]


@dataclass(frozen=True)
class HttpResult:
    url: str
    status: int | None
    headers: dict[str, str]
    body: bytes
    error_type: str | None = None
    error_message: str | None = None

    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")


class HttpClient:
    def __init__(self, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS) -> None:
        self.timeout_seconds = timeout_seconds

    def get(self, url: str, *, params: dict[str, str] | None = None, headers: dict[str, str] | None = None) -> HttpResult:
        import urllib.error
        import urllib.parse
        import urllib.request

        if params:
            separator = "&" if urllib.parse.urlparse(url).query else "?"
            url = url + separator + urllib.parse.urlencode(params)
        request = urllib.request.Request(url, headers=headers or {}, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                return HttpResult(url=request.full_url, status=response.status, headers=dict(response.headers.items()), body=response.read(5_000_000))
        except urllib.error.HTTPError as exc:
            return HttpResult(url=request.full_url, status=exc.code, headers=dict(exc.headers.items()) if exc.headers else {}, body=exc.read(5_000_000), error_type=type(exc).__name__, error_message=str(exc))
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            return HttpResult(url=request.full_url, status=None, headers={}, body=b"", error_type=type(exc).__name__, error_message=str(exc))


def sanitize_url(url: str) -> str:
    return url


def sanitize_value(value: Any) -> Any:
    return value


@dataclass(frozen=True)
class BundleContext:
    task_key: dict[str, Any]
    run_dir: Path
    cleaned_dir: Path
    saved_dir: Path
    receipt_path: Path
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StepResult:
    status: str
    references: list[str] = field(default_factory=list)
    row_counts: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FetchedCalendar:
    calendar_source: str
    source_url: str
    content_type: str
    body: str
    http_status: int | None
    discovered_candidates: list[dict[str, str]] = field(default_factory=list)


class CalendarDiscoveryError(ValueError):
    """Raised for invalid calendar discovery tasks."""


def _now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _required(mapping: dict[str, Any], key: str) -> Any:
    value = mapping.get(key)
    if value in (None, "", []):
        raise CalendarDiscoveryError(f"{BUNDLE}.params.{key} is required")
    return value


def build_context(task_key: dict[str, Any], run_id: str) -> BundleContext:
    if task_key.get("bundle") != BUNDLE:
        raise CalendarDiscoveryError(f"task_key.bundle must be {BUNDLE}")
    output_root = Path(str(task_key.get("output_root") or f"storage/{task_key.get('task_id', BUNDLE + '_task')}"))
    run_dir = output_root / "runs" / run_id
    return BundleContext(task_key, run_dir, run_dir / "cleaned", run_dir / "saved", output_root / "completion_receipt.json", {"run_id": run_id, "started_at": _now_utc()})


def _json_response(result: HttpResult) -> Any:
    if result.status is None:
        raise CalendarDiscoveryError(f"request failed before HTTP response: {result.error_type}: {result.error_message}")
    if result.status < 200 or result.status >= 300:
        raise CalendarDiscoveryError(f"request returned HTTP {result.status}: {result.error_message or result.text()[:240]}")
    return result



def _is_official_url(url: str, allowed_domains: tuple[str, ...] = OFFICIAL_DOMAIN_SUFFIXES) -> bool:
    host = urlparse(url).netloc.lower().split(":", 1)[0]
    return any(host == domain or host.endswith("." + domain) for domain in allowed_domains)


def _load_brave_search():
    try:
        from trading_web_search import brave_search
    except ImportError as exc:
        raise CalendarDiscoveryError(
            "calendar_discovery search mode requires trading-main helper package on PYTHONPATH: trading_web_search"
        ) from exc
    return brave_search


def discover_official_calendar_url(
    *,
    calendar_source: str,
    query: str | None = None,
    count: int = 5,
    allowed_domains: tuple[str, ...] = OFFICIAL_DOMAIN_SUFFIXES,
) -> tuple[str, list[dict[str, str]]]:
    search = _load_brave_search()
    search_query = query or DEFAULT_SEARCH_QUERIES.get(calendar_source)
    if not search_query:
        raise CalendarDiscoveryError(f"calendar_discovery search mode needs params.search_query for {calendar_source!r}")
    results = search(search_query, count=count)
    candidates: list[dict[str, str]] = []
    for result in results:
        candidates.append({"title": result.title, "url": result.url, "official": str(_is_official_url(result.url, allowed_domains)).lower()})
        if _is_official_url(result.url, allowed_domains):
            return result.url, candidates
    raise CalendarDiscoveryError(f"web search found no official calendar URL for query: {search_query}")

def fetch(context: BundleContext, *, client: HttpClient | None = None) -> tuple[StepResult, FetchedCalendar]:
    params = dict(context.task_key.get("params") or {})
    calendar_source = str(params.get("calendar_source") or params.get("data_kind") or "macro_release_calendar")
    discovered_candidates: list[dict[str, str]] = []
    if params.get("url"):
        source_url = str(params["url"])
    elif params.get("search") or params.get("search_query"):
        source_url, discovered_candidates = discover_official_calendar_url(
            calendar_source=calendar_source,
            query=str(params.get("search_query") or "") or None,
            count=int(params.get("search_count", 5)),
        )
    else:
        source_url = str(DEFAULT_URLS.get(calendar_source) or _required(params, "url"))
    if not _is_official_url(source_url):
        raise CalendarDiscoveryError(f"calendar source URL is not on an approved official domain: {source_url}")
    client = client or HttpClient(timeout_seconds=int(params.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)))
    request_params: dict[str, str] | None = None
    if calendar_source == "nasdaq_earnings_calendar":
        if "nasdaq.com/market-activity/earnings" in source_url:
            source_url = DEFAULT_URLS["nasdaq_earnings_calendar"]
        request_params = {"date": str(params.get("date") or datetime.now(ET).date().isoformat())}
    result = _json_response(
        client.get(
            source_url,
            params=request_params,
            headers={
                "User-Agent": "trading-execution-calendar-discovery/0.1",
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://www.nasdaq.com/market-activity/earnings",
            },
        )
    )
    content_type = result.headers.get("Content-Type", "")
    context.run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = context.run_dir / "request_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "calendar_source": calendar_source,
                "source_url": sanitize_url(result.url),
                "http_status": result.status,
                "content_type": content_type,
                "discovered_candidates": discovered_candidates,
                "params": sanitize_value(params),
                "fetched_at_utc": _now_utc(),
                "raw_persistence": "not_persisted_by_default",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return StepResult("succeeded", [str(manifest_path)], {"raw_calendar_payloads": 1}, details={"calendar_source": calendar_source, "source_url": sanitize_url(result.url), "discovered_candidates": len(discovered_candidates)}), FetchedCalendar(calendar_source, result.url, content_type, result.text(), result.status, discovered_candidates)


def _event_id(calendar_source: str, name: str, release_time: str, source_url: str) -> str:
    digest = hashlib.sha1(f"{calendar_source}|{name}|{release_time}|{source_url}".encode("utf-8")).hexdigest()[:12]
    return f"cal_{digest}"


def _row(calendar_source: str, name: str, release_time: str, source_url: str, raw_summary: str = "") -> dict[str, str]:
    event_date = release_time[:10] if release_time else ""
    return {
        "event_id": _event_id(calendar_source, name, release_time, source_url),
        "calendar_source": calendar_source,
        "event_name": name,
        "release_time": release_time,
        "event_date": event_date,
        "timezone": "America/New_York",
        "source_url": source_url,
        "raw_summary": raw_summary,
    }


def _parse_ics_datetime(value: str) -> str:
    value = value.strip()
    if len(value) == 8 and value.isdigit():
        return datetime.combine(date.fromisoformat(f"{value[:4]}-{value[4:6]}-{value[6:]}"), time.min, ET).isoformat()
    if value.endswith("Z"):
        parsed = datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        return parsed.astimezone(ET).isoformat()
    parsed = datetime.strptime(value[:15], "%Y%m%dT%H%M%S").replace(tzinfo=ET)
    return parsed.isoformat()


def parse_ics(body: str, *, calendar_source: str, source_url: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for block in body.split("BEGIN:VEVENT")[1:]:
        event = block.split("END:VEVENT", 1)[0]
        summary_match = re.search(r"^SUMMARY(?:;[^:]*)?:(.*)$", event, re.MULTILINE)
        dt_match = re.search(r"^DTSTART(?:;[^:]*)?:(.*)$", event, re.MULTILINE)
        if not summary_match or not dt_match:
            continue
        name = unescape(summary_match.group(1).strip().replace("\\,", ","))
        release_time = _parse_ics_datetime(dt_match.group(1))
        rows.append(_row(calendar_source, name, release_time, source_url, raw_summary=name))
    return rows


def parse_json_calendar(body: str, *, calendar_source: str, source_url: str) -> list[dict[str, str]]:
    payload = json.loads(body)
    items = payload.get("events", payload) if isinstance(payload, dict) else payload
    if not isinstance(items, list):
        return []
    rows: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("event_name") or item.get("name") or item.get("title") or item.get("summary") or "").strip()
        release_time = str(item.get("release_time") or item.get("datetime") or item.get("date") or "").strip()
        if not name or not release_time:
            continue
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", release_time):
            release_time = datetime.combine(date.fromisoformat(release_time), time.min, ET).isoformat()
        rows.append(_row(calendar_source, name, release_time, source_url, raw_summary=json.dumps(item, sort_keys=True)))
    return rows


def parse_fomc_html(body: str, *, source_url: str) -> list[dict[str, str]]:
    text = re.sub(r"<[^>]+>", " ", body)
    text = unescape(re.sub(r"\s+", " ", text))
    rows: list[dict[str, str]] = []
    # Handles official page text such as "January 30-31" under a year heading.
    year_matches = list(re.finditer(r"\b(20\d{2})\b", text))
    for idx, year_match in enumerate(year_matches):
        year = int(year_match.group(1))
        end = year_matches[idx + 1].start() if idx + 1 < len(year_matches) else len(text)
        section = text[year_match.end():end]
        for match in re.finditer(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:\s*[-–]\s*(\d{1,2}))?\b", section):
            month = datetime.strptime(match.group(1), "%B").month
            day = int(match.group(3) or match.group(2))
            release_time = datetime(year, month, day, 14, 0, tzinfo=ET).isoformat()
            name = f"FOMC meeting {match.group(1)} {match.group(2)}" + (f"-{match.group(3)}" if match.group(3) else "")
            rows.append(_row("fomc_calendar", name, release_time, source_url, raw_summary=match.group(0)))
    # Deduplicate repeated page/sidebar dates.
    seen: set[tuple[str, str]] = set()
    unique = []
    for row in rows:
        key = (row["event_name"], row["release_time"])
        if key not in seen:
            unique.append(row)
            seen.add(key)
    return unique


def parse_bls_html(body: str, *, source_url: str) -> list[dict[str, str]]:
    text = re.sub(r"<[^>]+>", " ", body)
    text = unescape(re.sub(r"\s+", " ", text))
    rows: list[dict[str, str]] = []
    pattern = re.compile(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\s+(\d{1,2}):(\d{2})\s*(AM|PM)\s+([^.;]{4,120})", re.IGNORECASE)
    for match in pattern.finditer(text):
        month, day, year, hour, minute, ampm, name = match.groups()
        hour_i = int(hour) % 12 + (12 if ampm.upper() == "PM" else 0)
        release_time = datetime(int(year), int(month), int(day), hour_i, int(minute), tzinfo=ET).isoformat()
        rows.append(_row("bls_release_calendar", name.strip(), release_time, source_url, raw_summary=match.group(0)))
    return rows


def _parse_nasdaq_as_of(value: str) -> date:
    value = value.strip()
    for fmt in ("%a, %b %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise CalendarDiscoveryError(f"could not parse Nasdaq earnings calendar date: {value!r}")


def _nasdaq_phase_time(value: str) -> time:
    phase = value.strip().lower()
    if phase == "time-pre-market":
        return time(8, 0)
    if phase == "time-after-hours":
        return time(16, 0)
    return time.min


def parse_nasdaq_earnings_json(body: str, *, source_url: str) -> list[dict[str, str]]:
    payload = json.loads(body)
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return []
    report_date = _parse_nasdaq_as_of(str(data.get("asOf") or ""))
    rows_payload = data.get("rows") or []
    if not isinstance(rows_payload, list):
        return []

    rows: list[dict[str, str]] = []
    for item in rows_payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        company_name = str(item.get("name") or "").strip()
        if not symbol or not company_name:
            continue
        phase = str(item.get("time") or "time-not-supplied")
        release_time = datetime.combine(report_date, _nasdaq_phase_time(phase), ET).isoformat()
        event_name = f"{symbol} earnings release ({company_name})"
        rows.append(_row("nasdaq_earnings_calendar", event_name, release_time, source_url, raw_summary=json.dumps(item, sort_keys=True)))
    return rows


def parse_calendar(fetched: FetchedCalendar, requested_format: str = "auto") -> tuple[list[dict[str, str]], list[str]]:
    body = fetched.body
    fmt = requested_format.lower()
    warnings: list[str] = []
    parsers: list[str]
    if fmt != "auto":
        parsers = [fmt]
    elif fetched.calendar_source == "nasdaq_earnings_calendar":
        parsers = ["nasdaq_earnings_json"]
    elif "BEGIN:VCALENDAR" in body:
        parsers = ["ics"]
    elif fetched.calendar_source == "fomc_calendar":
        parsers = ["fomc_html"]
    elif fetched.calendar_source == "bls_release_calendar":
        parsers = ["bls_html"]
    elif "json" in fetched.content_type.lower() or body.lstrip().startswith(("{", "[")):
        parsers = ["json"]
    else:
        parsers = ["ics", "json", "fomc_html", "bls_html", "nasdaq_earnings_json"]

    for parser in parsers:
        try:
            if parser == "ics":
                rows = parse_ics(body, calendar_source=fetched.calendar_source, source_url=fetched.source_url)
            elif parser == "json":
                rows = parse_json_calendar(body, calendar_source=fetched.calendar_source, source_url=fetched.source_url)
            elif parser == "fomc_html":
                rows = parse_fomc_html(body, source_url=fetched.source_url)
            elif parser == "bls_html":
                rows = parse_bls_html(body, source_url=fetched.source_url)
            elif parser == "nasdaq_earnings_json":
                rows = parse_nasdaq_earnings_json(body, source_url=fetched.source_url)
            else:
                raise CalendarDiscoveryError(f"unsupported calendar format {parser!r}")
        except (json.JSONDecodeError, ValueError) as exc:
            warnings.append(f"{parser} parse failed: {exc}")
            continue
        if rows:
            return sorted(rows, key=lambda row: (row["release_time"], row["event_name"])), warnings
        warnings.append(f"{parser} parser found no events")
    return [], warnings


def clean(context: BundleContext, fetched: FetchedCalendar) -> StepResult:
    params = dict(context.task_key.get("params") or {})
    rows, warnings = parse_calendar(fetched, str(params.get("format") or "auto"))
    if not rows:
        raise CalendarDiscoveryError("calendar payload produced zero release events; provide format/url-specific adapter params")
    context.cleaned_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = context.cleaned_dir / "release_calendar.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    schema_path = context.cleaned_dir / "schema.json"
    schema_path.write_text(json.dumps({"columns": CALENDAR_FIELDS, "row_count": len(rows)}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return StepResult("succeeded", [str(jsonl_path), str(schema_path)], {"release_calendar": len(rows)}, warnings=warnings, details={"columns": CALENDAR_FIELDS})


def save(context: BundleContext, clean_result: StepResult) -> StepResult:
    context.saved_dir.mkdir(parents=True, exist_ok=True)
    rows = [json.loads(line) for line in (context.cleaned_dir / "release_calendar.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    path = context.saved_dir / "release_calendar.csv"
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CALENDAR_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp_path, path)
    return StepResult("succeeded", [str(path)], dict(clean_result.row_counts), warnings=list(clean_result.warnings), details={"format": "csv", "atomic_write": True})


def write_receipt(context: BundleContext, *, status: str, fetch_result: StepResult | None = None, clean_result: StepResult | None = None, save_result: StepResult | None = None, error: BaseException | None = None) -> StepResult:
    context.receipt_path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, Any] = {"task_id": context.task_key.get("task_id"), "bundle": BUNDLE, "runs": []}
    if context.receipt_path.exists():
        try:
            existing = json.loads(context.receipt_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    row_counts = save_result.row_counts if save_result else clean_result.row_counts if clean_result else fetch_result.row_counts if fetch_result else {}
    outputs = save_result.references if save_result else []
    warnings = [warning for result in (fetch_result, clean_result, save_result) if result is not None for warning in result.warnings]
    entry = {
        "run_id": context.metadata["run_id"],
        "status": status,
        "started_at": context.metadata.get("started_at"),
        "completed_at": _now_utc(),
        "output_dir": str(context.run_dir),
        "outputs": outputs,
        "row_counts": row_counts,
        "warnings": warnings,
        "steps": {"fetch": asdict(fetch_result) if fetch_result else None, "clean": asdict(clean_result) if clean_result else None, "save": asdict(save_result) if save_result else None},
        "error": None if error is None else {"type": type(error).__name__, "message": str(error)},
    }
    existing["runs"] = [run for run in existing.get("runs", []) if run.get("run_id") != context.metadata["run_id"]] + [entry]
    existing.update({"task_id": context.task_key.get("task_id"), "bundle": BUNDLE})
    context.receipt_path.write_text(json.dumps(existing, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return StepResult(status, [str(context.receipt_path), *outputs], row_counts, warnings=warnings, details={"run_id": context.metadata["run_id"], "error": entry["error"]})


def run(task_key: dict[str, Any], *, run_id: str, client: HttpClient | None = None) -> StepResult:
    context = build_context(task_key, run_id)
    fetch_result = clean_result = save_result = None
    try:
        fetch_result, fetched = fetch(context, client=client)
        clean_result = clean(context, fetched)
        save_result = save(context, clean_result)
        return write_receipt(context, status="succeeded", fetch_result=fetch_result, clean_result=clean_result, save_result=save_result)
    except BaseException as exc:
        return write_receipt(context, status="failed", fetch_result=fetch_result, clean_result=clean_result, save_result=save_result, error=exc)
