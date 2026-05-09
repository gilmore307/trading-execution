from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class TradeRiskCapCliTests(unittest.TestCase):
    def test_cli_accepts_valid_decision_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            decision_path = Path(tmp) / "decision.json"
            decision_path.write_text(
                json.dumps(
                    {
                        "decision_record_id": "decision_001",
                        "trade_risk_cap": {
                            "max_loss_usd": 125.0,
                            "max_loss_pct": 0.0125,
                            "time_stop_at": "2026-05-08T15:55:00-04:00",
                            "cap_enforcement_mode": "risk_monitor_synthetic_stop",
                            "cap_failure_action": "reject_order",
                            "model_invalidation_price": 178.6,
                            "hard_stop_price": 179.2,
                        },
                    }
                ),
                encoding="utf-8",
            )
            result = subprocess.run(
                [sys.executable, "scripts/execution/validate_trade_risk_cap.py", str(decision_path)],
                cwd=Path(__file__).resolve().parents[1],
                env={"PYTHONPATH": "src"},
                check=False,
                capture_output=True,
                text=True,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)["valid"])

    def test_cli_rejects_missing_cap(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            decision_path = Path(tmp) / "decision.json"
            decision_path.write_text(json.dumps({"decision_record_id": "decision_missing"}), encoding="utf-8")
            result = subprocess.run(
                [sys.executable, "scripts/execution/validate_trade_risk_cap.py", str(decision_path)],
                cwd=Path(__file__).resolve().parents[1],
                env={"PYTHONPATH": "src"},
                check=False,
                capture_output=True,
                text=True,
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn("missing_trade_risk_cap", json.loads(result.stdout)["reason_codes"])


if __name__ == "__main__":
    unittest.main()
