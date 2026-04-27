from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from trading_execution.calendar_discovery.pipeline import HttpResult, discover_official_calendar_url, parse_ics, parse_nasdaq_earnings_json, run


class FakeCalendarClient:
    def __init__(self, body: str, content_type: str = "text/calendar"):
        self.body = body
        self.content_type = content_type

    def get(self, url, *, params=None, headers=None):
        return HttpResult(url=url, status=200, headers={"Content-Type": self.content_type}, body=self.body.encode())


class CalendarDiscoveryPipelineTests(unittest.TestCase):
    def test_parse_ics_release_events(self):
        rows = parse_ics(
            "BEGIN:VCALENDAR\nBEGIN:VEVENT\nSUMMARY:CPI Release\nDTSTART:20240410T083000\nEND:VEVENT\nEND:VCALENDAR\n",
            calendar_source="unit_calendar",
            source_url="https://www.bls.gov/calendar.ics",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_name"], "CPI Release")
        self.assertEqual(rows[0]["release_time"], "2024-04-10T08:30:00-04:00")


    def test_search_discovery_selects_first_official_url(self):
        import sys
        import types

        module = types.ModuleType("trading_web_search")

        class Result:
            def __init__(self, title, url):
                self.title = title
                self.url = url

        module.brave_search = lambda query, count=5: [
            Result("third party", "https://example.com/calendar"),
            Result("BLS", "https://www.bls.gov/schedule/news_release/cpi.htm"),
        ]
        old = sys.modules.get("trading_web_search")
        sys.modules["trading_web_search"] = module
        try:
            url, candidates = discover_official_calendar_url(calendar_source="bls_release_calendar", query="unit", count=2)
        finally:
            if old is None:
                sys.modules.pop("trading_web_search", None)
            else:
                sys.modules["trading_web_search"] = old
        self.assertEqual(url, "https://www.bls.gov/schedule/news_release/cpi.htm")
        self.assertEqual(candidates[0]["official"], "false")
        self.assertEqual(candidates[1]["official"], "true")

    def test_run_saves_release_calendar_csv_from_ics(self):
        body = "BEGIN:VCALENDAR\nBEGIN:VEVENT\nSUMMARY:Employment Situation\nDTSTART:20240503T083000\nEND:VEVENT\nEND:VCALENDAR\n"
        with tempfile.TemporaryDirectory() as tmp:
            task_key = {
                "task_id": "calendar_discovery_task_test",
                "bundle": "calendar_discovery",
                "params": {"calendar_source": "bls_release_calendar", "url": "https://www.bls.gov/bls.ics", "format": "ics"},
                "output_root": str(Path(tmp) / "calendar_discovery_task_test"),
            }
            result = run(task_key, run_id="calendar_discovery_run_test", client=FakeCalendarClient(body))
            self.assertEqual(result.status, "succeeded")
            saved = Path(task_key["output_root"]) / "runs" / "calendar_discovery_run_test" / "saved" / "release_calendar.csv"
            self.assertTrue(saved.exists())
            with saved.open(newline="") as handle:
                row = next(csv.DictReader(handle))
            self.assertEqual(row["calendar_source"], "bls_release_calendar")
            self.assertEqual(row["event_name"], "Employment Situation")
            self.assertEqual(row["release_time"], "2024-05-03T08:30:00-04:00")
            receipt = json.loads((Path(task_key["output_root"]) / "completion_receipt.json").read_text())
            self.assertEqual(receipt["runs"][0]["row_counts"]["release_calendar"], 1)

    def test_run_saves_release_calendar_csv_from_json(self):
        body = json.dumps({"events": [{"name": "Retail Sales", "release_time": "2024-04-15T08:30:00-04:00"}]})
        with tempfile.TemporaryDirectory() as tmp:
            task_key = {
                "task_id": "calendar_discovery_task_json",
                "bundle": "calendar_discovery",
                "params": {"calendar_source": "census_release_calendar", "url": "https://www.census.gov/calendar.json"},
                "output_root": str(Path(tmp) / "calendar_discovery_task_json"),
            }
            result = run(task_key, run_id="calendar_discovery_run_json", client=FakeCalendarClient(body, "application/json"))
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(result.row_counts["release_calendar"], 1)

    def test_parse_nasdaq_earnings_calendar_json(self):
        body = json.dumps(
            {
                "data": {
                    "asOf": "Mon, Apr 27, 2026",
                    "rows": [
                        {
                            "symbol": "VZ",
                            "name": "Verizon Communications Inc.",
                            "time": "time-pre-market",
                            "fiscalQuarterEnding": "Mar/2026",
                            "epsForecast": "$1.22",
                        }
                    ],
                }
            }
        )
        rows = parse_nasdaq_earnings_json(body, source_url="https://api.nasdaq.com/api/calendar/earnings?date=2026-04-27")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["calendar_source"], "nasdaq_earnings_calendar")
        self.assertEqual(rows[0]["event_name"], "VZ earnings release (Verizon Communications Inc.)")
        self.assertEqual(rows[0]["release_time"], "2026-04-27T08:00:00-04:00")

    def test_run_saves_nasdaq_earnings_calendar(self):
        body = json.dumps({"data": {"asOf": "Mon, Apr 27, 2026", "rows": [{"symbol": "CDNS", "name": "Cadence Design Systems, Inc.", "time": "time-after-hours"}]}})
        with tempfile.TemporaryDirectory() as tmp:
            task_key = {
                "task_id": "calendar_discovery_task_nasdaq",
                "bundle": "calendar_discovery",
                "params": {"calendar_source": "nasdaq_earnings_calendar", "date": "2026-04-27"},
                "output_root": str(Path(tmp) / "calendar_discovery_task_nasdaq"),
            }
            result = run(task_key, run_id="calendar_discovery_run_nasdaq", client=FakeCalendarClient(body, "application/json"))
            self.assertEqual(result.status, "succeeded")
            self.assertEqual(result.row_counts["release_calendar"], 1)


if __name__ == "__main__":
    unittest.main()
