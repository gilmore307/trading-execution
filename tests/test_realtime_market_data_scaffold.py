from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from trading_execution.market_data import build_realtime_subscription_plan, validate_realtime_capture


class RealtimeMarketDataScaffoldTests(unittest.TestCase):
    def test_build_realtime_subscription_plan_for_alpaca_target_layer(self) -> None:
        plan = build_realtime_subscription_plan(
            {
                "request_id": "rtplan_unit",
                "mode": "dry_run",
                "sources": ["alpaca"],
                "model_layers": ["layer_03_target_state_vector"],
                "instrument_refs": ["AAPL"],
            }
        )

        self.assertEqual(plan["contract_type"], "execution_realtime_subscription_plan_set_v1")
        self.assertEqual(plan["provider_calls_performed"], 0)
        self.assertEqual(plan["broker_calls_performed"], 0)
        self.assertEqual(len(plan["subscription_plans"]), 1)
        row = plan["subscription_plans"][0]
        self.assertEqual(row["contract_type"], "execution_realtime_subscription_plan_v1")
        self.assertEqual(row["source_id"], "alpaca")
        self.assertEqual(row["model_layers"], ["layer_03_target_state_vector"])
        self.assertEqual(row["subscription_status"], "dry_run_plan_ready_no_provider_calls")
        self.assertTrue(row["requires_secret_alias"])

    def test_live_observe_plan_blocks_without_approval_ref(self) -> None:
        plan = build_realtime_subscription_plan(
            {
                "mode": "live_observe",
                "sources": ["thetadata"],
                "model_layers": ["layer_08_option_expression"],
                "instrument_refs": ["AAPL_20260515_270C"],
            }
        )

        row = plan["subscription_plans"][0]
        self.assertEqual(row["subscription_status"], "blocked_requires_live_stream_approval_ref")
        self.assertIn("live_stream_approval_ref", row["required_gate_refs"])
        self.assertEqual(row["provider_calls_performed"], 0)

    def test_execution_account_state_placeholder_routes_to_layer_six(self) -> None:
        plan = build_realtime_subscription_plan(
            {
                "sources": ["execution_account_state"],
                "model_layers": ["layer_06_position_projection"],
            }
        )

        row = plan["subscription_plans"][0]
        self.assertEqual(row["source_id"], "execution_account_state")
        self.assertEqual(row["realtime_interfaces"], ["execution_account_state_context_ref"])
        self.assertEqual(row["model_layers"], ["layer_06_position_projection"])

    def test_validate_realtime_capture_accepts_complete_forward_holdout_row(self) -> None:
        candidate = {
            "capture_id": "rtcap_unit",
            "observation_time": "2026-05-11T13:30:00+00:00",
            "provider_available_time": "2026-05-11T13:30:01+00:00",
            "tradeable_time": "2026-05-11T13:30:02+00:00",
            "source_id": "alpaca",
            "realtime_interface": "alpaca_market_data_websocket",
            "asset_class": "us_equity",
            "instrument_ref": "AAPL",
            "normalized_payload_ref": "memory://normalized/aapl",
            "frozen_model_config_ref": "trading-model://configs/model_03/unit",
            "model_output_ref": "trading-model://outputs/model_03/unit",
            "dataset_snapshot_ref": "trading-model://snapshots/unit",
            "dataset_role": "forward_holdout",
            "label_maturity_time": "2026-05-12T13:30:02+00:00",
            "outcome_label_ref": "trading-model://labels/unit",
            "ingestion_commit_ref": "git://trading-execution/unit",
            "run_manifest_ref": "manifest://unit",
            "artifact_ref": "artifact://unit",
            "ready_signal_ref": "ready://unit",
            "requested_actions": [],
        }

        result = validate_realtime_capture(candidate)
        self.assertTrue(result["valid"])
        self.assertEqual(result["provider_calls_performed"], 0)
        self.assertFalse(result["model_activation_performed"])

    def test_validate_realtime_capture_rejects_forbidden_action(self) -> None:
        result = validate_realtime_capture(
            {
                "capture_id": "bad",
                "dataset_role": "train",
                "requested_actions": ["broker_order_mutation"],
            }
        )

        self.assertFalse(result["valid"])
        self.assertFalse(result["dataset_role_valid"])
        self.assertIn("broker_order_mutation", result["forbidden_actions_present"])
        self.assertIn("observation_time", result["missing_fields"])

    def test_plan_and_validate_clis_are_side_effect_free(self) -> None:
        plan_result = subprocess.run(
            [
                sys.executable,
                "scripts/execution/plan_realtime_capture.py",
                "--source",
                "okx",
                "--model-layer",
                "layer_01_market_regime",
                "--instrument-ref",
                "BTC-USDT",
            ],
            check=True,
            cwd="/root/projects/trading-execution",
            env={"PYTHONPATH": "src"},
            text=True,
            capture_output=True,
        )
        plan = json.loads(plan_result.stdout)
        self.assertEqual(plan["provider_calls_performed"], 0)
        self.assertEqual(plan["subscription_plans"][0]["source_id"], "okx")

        capture = {
            "capture_id": "rtcap_cli",
            "observation_time": "2026-05-11T13:30:00+00:00",
            "provider_available_time": "2026-05-11T13:30:01+00:00",
            "tradeable_time": "2026-05-11T13:30:02+00:00",
            "source_id": "okx",
            "realtime_interface": "okx_public_websocket",
            "asset_class": "crypto_spot",
            "instrument_ref": "BTC-USDT",
            "normalized_payload_ref": "memory://normalized/btc-usdt",
            "frozen_model_config_ref": "trading-model://configs/model_01/unit",
            "model_output_ref": "trading-model://outputs/model_01/unit",
            "dataset_snapshot_ref": "trading-model://snapshots/unit",
            "dataset_role": "shadow_monitoring",
            "label_maturity_time": "2026-05-11T14:30:02+00:00",
            "outcome_label_ref": "trading-model://labels/unit",
            "ingestion_commit_ref": "git://trading-execution/unit",
            "run_manifest_ref": "manifest://unit",
            "artifact_ref": "artifact://unit",
            "ready_signal_ref": "ready://unit",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            capture_path = Path(temp_dir) / "capture.json"
            capture_path.write_text(json.dumps(capture), encoding="utf-8")
            validate_result = subprocess.run(
                [sys.executable, "scripts/execution/validate_realtime_capture.py", str(capture_path)],
                check=True,
                cwd="/root/projects/trading-execution",
                env={"PYTHONPATH": "src"},
                text=True,
                capture_output=True,
            )
        validation = json.loads(validate_result.stdout)
        self.assertTrue(validation["valid"])
        self.assertEqual(validation["broker_calls_performed"], 0)


if __name__ == "__main__":
    unittest.main()
