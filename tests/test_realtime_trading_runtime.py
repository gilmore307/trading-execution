import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from trading_execution.model_lifecycle import build_active_model_config_write, build_shadow_cycle_selection
from trading_execution.runtime import build_realtime_trading_runtime_status


class RealtimeTradingRuntimeTests(unittest.TestCase):
    def test_runtime_waits_without_promoted_active_model(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            status = build_realtime_trading_runtime_status(
                active_model_config_path=Path(directory) / "missing.json",
                realtime_monitor_loop_ref="execution://monitor/latest",
                te_calendar_refresh_ref="trading-data://te/recent/latest",
            )

        self.assertEqual(status["contract_type"], "execution_realtime_trading_runtime_status")
        self.assertEqual(status["runtime_status"], "waiting_for_promoted_model")
        self.assertEqual(status["next_gate"], "write_active_model_config_after_promotion")
        self.assertFalse(status["allowed_actions"]["model_activation_allowed"])
        self.assertFalse(status["allowed_actions"]["broker_order_construction_allowed"])
        self.assertFalse(status["allowed_actions"]["broker_execution_allowed"])
        self.assertTrue(status["interfaces_available"]["model_decision_input_snapshot"])
        self.assertFalse(status["interfaces_connected"]["model_decision_input_snapshot"])
        self.assertFalse(status["interfaces_connected"]["active_model_config_write"])
        self.assertFalse(status["interfaces_connected"]["trade_risk_cap_validation"])
        self.assertEqual(status["provider_calls_performed"], 0)
        self.assertEqual(status["broker_calls_performed"], 0)

    def test_runtime_accepts_valid_active_pointer_but_keeps_broker_submit_closed(self) -> None:
        selection = build_shadow_cycle_selection(
            cycle_ref="cycle://2026-06",
            current_active_model_ref="model://incumbent",
            candidate_reviews=[
                {
                    "candidate_model_ref": "model://winner",
                    "promotion_readiness_ref": "ready://winner",
                    "overall_rank": 1,
                    "review_status": "active_candidate",
                }
            ],
        )
        active_write = build_active_model_config_write(
            shadow_cycle_selection=selection,
            expected_previous_active_model_ref="model://incumbent",
            new_active_config_ref="storage://active/winner",
            rollback_ref="storage://active/incumbent",
            write_window_ref="window://closed-market",
        )
        with tempfile.TemporaryDirectory() as directory:
            pointer_path = Path(directory) / "active.json"
            pointer_path.write_text(json.dumps(active_write), encoding="utf-8")
            status = build_realtime_trading_runtime_status(
                active_model_config_path=pointer_path,
                model_decision_input_snapshot_ref="decision-input://latest",
                trade_risk_cap_validation_ref="risk-cap://latest",
                order_construction_approval_ref="approval://latest",
                allow_model_activation=True,
                allow_order_construction=True,
            )

        self.assertEqual(status["runtime_status"], "ready_for_order_intent_construction_not_submission")
        self.assertEqual(status["active_model_pointer"]["selected_active_model_ref"], "model://winner")
        self.assertTrue(status["allowed_actions"]["model_activation_allowed"])
        self.assertTrue(status["allowed_actions"]["broker_order_construction_allowed"])
        self.assertFalse(status["allowed_actions"]["broker_execution_allowed"])
        self.assertTrue(status["interfaces_connected"]["active_model_config_write"])
        self.assertTrue(status["interfaces_connected"]["model_decision_input_snapshot"])
        self.assertTrue(status["interfaces_connected"]["trade_risk_cap_validation"])
        self.assertTrue(status["interfaces_connected"]["broker_order_intent_construction"])
        self.assertFalse(status["interfaces_connected"]["broker_submit_adapter"])

    def test_runtime_cli_writes_status(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "runtime"
            completed = subprocess.run(
                [
                    sys.executable,
                    "scripts/execution/run_realtime_trading_runtime_check.py",
                    "--active-model-config-path",
                    str(Path(directory) / "missing.json"),
                    "--output-dir",
                    str(output_dir),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            payload = json.loads(completed.stdout)
            self.assertEqual(payload["runtime_status"], "waiting_for_promoted_model")
            self.assertTrue((output_dir / "runtime_status.json").exists())

    def test_runtime_cli_requirement_blocks_without_active_model(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory) / "runtime"
            completed = subprocess.run(
                [
                    sys.executable,
                    "scripts/execution/run_realtime_trading_runtime_check.py",
                    "--active-model-config-path",
                    str(Path(directory) / "missing.json"),
                    "--require-active-model-config",
                    "--output-dir",
                    str(output_dir),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            payload = json.loads(completed.stdout)
            self.assertTrue((output_dir / "runtime_status.json").exists())

        self.assertEqual(completed.returncode, 2)
        self.assertEqual(payload["runtime_status"], "waiting_for_promoted_model")


if __name__ == "__main__":
    unittest.main()
