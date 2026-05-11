from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from trading_execution.market_data import (
    build_live_observe_adapter_plan,
    build_model_decision_input_snapshot,
    build_realtime_capture_fixture,
    build_realtime_feature_snapshot,
    build_realtime_shadow_fixture_bundle,
    build_realtime_subscription_plan,
    execute_live_observe,
    validate_live_observe_approval,
    validate_model_decision_input_snapshot,
    validate_realtime_capture,
    validate_realtime_feature_snapshot,
)


class RealtimeMarketDataScaffoldTests(unittest.TestCase):
    def test_live_observe_adapter_plan_covers_provider_event_account_routes(self) -> None:
        plan = build_live_observe_adapter_plan(
            {
                "request_id": "rtlive_unit",
                "mode": "fixture_replay",
                "sources": ["alpaca", "thetadata", "okx", "calendar_discovery", "execution_account_state", "derived_model_context"],
                "instrument_refs": ["AAPL"],
            }
        )

        self.assertEqual(plan["contract_type"], "execution_realtime_live_observe_adapter_plan_set_v1")
        self.assertEqual(plan["provider_calls_performed"], 0)
        self.assertEqual(plan["broker_calls_performed"], 0)
        self.assertFalse(plan["account_mutation_performed"])
        rows = {row["source_id"]: row for row in plan["adapter_plans"]}
        self.assertIn("layer_03_target_state_vector", rows["alpaca"]["model_layers"])
        self.assertIn("layer_08_option_expression", rows["thetadata"]["model_layers"])
        self.assertIn("layer_01_market_regime", rows["okx"]["model_layers"])
        self.assertEqual(rows["calendar_discovery"]["model_layers"], ["layer_04_event_overlay"])
        self.assertEqual(rows["derived_model_context"]["model_layers"], ["layer_05_alpha_confidence"])
        self.assertIn("layer_06_position_projection", rows["execution_account_state"]["model_layers"])

    def test_live_observe_adapter_blocks_real_stream_without_approval(self) -> None:
        plan = build_live_observe_adapter_plan(
            {
                "mode": "live_observe",
                "sources": ["alpaca"],
                "model_layers": ["layer_02_sector_context"],
                "instrument_refs": ["XLK"],
            }
        )

        row = plan["adapter_plans"][0]
        self.assertEqual(row["live_observe_status"], "blocked_requires_live_stream_approval_ref")
        self.assertIn("live_stream_approval_ref", row["required_gate_refs"])
        self.assertEqual(row["provider_calls_performed"], 0)

    def test_realtime_capture_fixture_rows_validate(self) -> None:
        fixture = build_realtime_capture_fixture(
            {
                "request_id": "rtcap_fixture_unit",
                "mode": "fixture_replay",
                "sources": ["alpaca", "execution_account_state"],
                "model_layers": ["layer_06_position_projection", "layer_07_underlying_action"],
                "instrument_refs": ["AAPL"],
                "decision_time": "2026-05-11T13:30:00+00:00",
                "historical_dataset_snapshot_ref": "trading-model://snapshots/historical/unit",
                "frozen_model_config_ref": "trading-model://configs/frozen/unit",
            }
        )

        self.assertEqual(fixture["contract_type"], "execution_realtime_capture_fixture_set_v1")
        self.assertGreaterEqual(len(fixture["captures"]), 2)
        for row in fixture["captures"]:
            validation = validate_realtime_capture(row)
            self.assertTrue(validation["valid"], validation)
            self.assertEqual(validation["provider_calls_performed"], 0)

    def test_realtime_shadow_fixture_bundle_builds_decision_input(self) -> None:
        bundle = build_realtime_shadow_fixture_bundle(
            {
                "request_id": "rtshadow_unit",
                "mode": "fixture_replay",
                "instrument_refs": ["AAPL"],
                "decision_time": "2026-05-11T13:30:00+00:00",
                "available_time": "2026-05-11T13:30:01+00:00",
                "tradeable_time": "2026-05-11T13:30:02+00:00",
                "historical_dataset_snapshot_ref": "trading-model://snapshots/historical/unit",
                "frozen_model_config_ref": "trading-model://configs/frozen/unit",
            }
        )

        self.assertEqual(bundle["contract_type"], "execution_realtime_shadow_fixture_bundle_v1")
        self.assertEqual(bundle["bundle_status"], "ready_for_model_route_plan")
        self.assertEqual(bundle["provider_calls_performed"], 0)
        self.assertFalse(bundle["broker_order_construction_performed"])
        self.assertEqual(len(bundle["decision_input_snapshot"]["layer_input_refs"]), 8)

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

    def test_build_realtime_feature_snapshot_covers_all_model_layers(self) -> None:
        snapshot = build_realtime_feature_snapshot(
            {
                "decision_time": "2026-05-11T13:30:00+00:00",
                "available_time": "2026-05-11T13:30:01+00:00",
                "tradeable_time": "2026-05-11T13:30:02+00:00",
                "instrument_ref": "AAPL",
                "historical_dataset_snapshot_ref": "trading-model://snapshots/historical/unit",
                "frozen_model_config_ref": "trading-model://configs/frozen/unit",
                "source_capture_refs": ["capture://alpaca/aapl/unit"],
            }
        )

        self.assertEqual(snapshot["contract_type"], "realtime_feature_snapshot_v1")
        self.assertEqual(snapshot["readiness_status"], "ready_for_fixture_or_shadow_model_decision_input")
        self.assertEqual(len(snapshot["feature_rows"]), 8)
        self.assertEqual(snapshot["provider_calls_performed"], 0)
        self.assertFalse(snapshot["model_activation_performed"])
        validation = validate_realtime_feature_snapshot(snapshot)
        self.assertTrue(validation["valid"])
        self.assertEqual(validation["missing_layer_rows"], [])

    def test_build_model_decision_input_snapshot_from_realtime_features(self) -> None:
        decision_input = build_model_decision_input_snapshot(
            {
                "decision_time": "2026-05-11T13:30:00+00:00",
                "available_time": "2026-05-11T13:30:01+00:00",
                "tradeable_time": "2026-05-11T13:30:02+00:00",
                "instrument_ref": "AAPL",
                "historical_dataset_snapshot_ref": "trading-model://snapshots/historical/unit",
                "frozen_model_config_ref": "trading-model://configs/frozen/unit",
                "source_capture_refs": ["capture://alpaca/aapl/unit"],
            }
        )

        self.assertEqual(decision_input["contract_type"], "execution_model_decision_input_snapshot_v1")
        self.assertEqual(decision_input["readiness_status"], "ready_for_historical_model_decision_handoff")
        self.assertEqual(len(decision_input["layer_input_refs"]), 8)
        self.assertEqual(decision_input["provider_calls_performed"], 0)
        self.assertFalse(decision_input["broker_order_construction_performed"])
        validation = validate_model_decision_input_snapshot(decision_input)
        self.assertTrue(validation["valid"])

    def test_live_observe_approval_blocks_mutating_flags(self) -> None:
        validation = validate_live_observe_approval(
            {
                "contract_type": "realtime_live_observe_approval_v1",
                "approval_id": "rtla_unit",
                "approval_scope": "realtime_market_data_observe_only",
                "approved_sources": ["okx"],
                "approved_instrument_refs": ["BTC-USDT"],
                "approved_at_utc": "2026-05-11T13:00:00+00:00",
                "expires_at_utc": "2099-05-11T14:00:00+00:00",
                "max_provider_calls": 1,
                "execute_live_observe_allowed": True,
                "model_activation_allowed": True,
                "broker_execution_allowed": False,
                "broker_order_construction_allowed": False,
                "account_mutation_allowed": False,
            },
            requested_sources=["okx"],
            requested_instrument_refs=["BTC-USDT"],
            requested_provider_calls=1,
        )

        self.assertFalse(validation["valid"])
        self.assertIn("model_activation_allowed_must_be_false", validation["invalid_fields"])

    def test_execute_live_observe_uses_approved_read_only_provider_call(self) -> None:
        calls: list[tuple[str, dict[str, str]]] = []

        def fake_transport(url: str, headers: dict[str, str]) -> dict[str, object]:
            calls.append((url, headers))
            return {"code": "0", "data": [{"instId": "BTC-USDT", "last": "100.0"}]}

        result = execute_live_observe(
            {
                "request_id": "rtlive_exec_unit",
                "sources": ["okx"],
                "model_layers": ["layer_01_market_regime"],
                "instrument_refs": ["BTC-USDT"],
                "decision_time": "2026-05-11T13:30:00+00:00",
                "historical_dataset_snapshot_ref": "trading-model://snapshots/historical/unit",
                "frozen_model_config_ref": "trading-model://configs/frozen/unit",
            },
            approval={
                "contract_type": "realtime_live_observe_approval_v1",
                "approval_id": "rtla_unit",
                "approval_scope": "realtime_market_data_observe_only",
                "approved_sources": ["okx"],
                "approved_instrument_refs": ["BTC-USDT"],
                "approved_at_utc": "2026-05-11T13:00:00+00:00",
                "expires_at_utc": "2099-05-11T14:00:00+00:00",
                "max_provider_calls": 1,
                "execute_live_observe_allowed": True,
                "model_activation_allowed": False,
                "broker_execution_allowed": False,
                "broker_order_construction_allowed": False,
                "account_mutation_allowed": False,
            },
            execute_live_observe=True,
            transport=fake_transport,
        )

        self.assertEqual(result["live_observe_status"], "observed")
        self.assertEqual(result["provider_calls_performed"], 1)
        self.assertEqual(len(calls), 1)
        self.assertIn("/api/v5/market/ticker", calls[0][0])
        self.assertEqual(result["broker_calls_performed"], 0)
        self.assertFalse(result["model_activation_performed"])
        self.assertFalse(result["broker_order_construction_performed"])
        self.assertFalse(result["account_mutation_performed"])
        self.assertEqual(len(result["captures"]), 1)
        self.assertTrue(result["captures"][0]["capture_validation"]["valid"])
        self.assertEqual(result["feature_snapshot"]["readiness_status"], "blocked_missing_realtime_feature_requirements")
        self.assertEqual(result["decision_input_snapshot"]["readiness_status"], "blocked_missing_model_decision_input_requirements")


    def test_execute_live_observe_resolves_alpaca_source_secret_file(self) -> None:
        calls: list[tuple[str, dict[str, str]]] = []

        def fake_transport(url: str, headers: dict[str, str]) -> dict[str, object]:
            calls.append((url, headers))
            return {"symbol": "SPY", "latestTrade": {"p": 500.0}}

        with tempfile.TemporaryDirectory() as temp_dir:
            secret_path = Path(temp_dir) / "alpaca.json"
            secret_path.write_text(
                json.dumps(
                    {
                        "api_key": "unit_api_key",
                        "secret_key": "unit_secret_key",
                        "endpoint": "https://unit-data.alpaca.example",
                    }
                ),
                encoding="utf-8",
            )
            result = execute_live_observe(
                {
                    "request_id": "rtlive_alpaca_secret_unit",
                    "sources": ["alpaca"],
                    "model_layers": ["layer_03_target_state_vector"],
                    "instrument_refs": ["SPY"],
                    "decision_time": "2026-05-11T13:30:00+00:00",
                    "historical_dataset_snapshot_ref": "trading-model://snapshots/historical/unit",
                    "frozen_model_config_ref": "trading-model://configs/frozen/unit",
                },
                approval={
                    "contract_type": "realtime_live_observe_approval_v1",
                    "approval_id": "rtla_alpaca_unit",
                    "approval_scope": "realtime_market_data_observe_only",
                    "approved_sources": ["alpaca"],
                    "approved_instrument_refs": ["SPY"],
                    "approved_at_utc": "2026-05-11T13:00:00+00:00",
                    "expires_at_utc": "2099-05-11T14:00:00+00:00",
                    "max_provider_calls": 1,
                    "execute_live_observe_allowed": True,
                    "model_activation_allowed": False,
                    "broker_execution_allowed": False,
                    "broker_order_construction_allowed": False,
                    "account_mutation_allowed": False,
                },
                execute_live_observe=True,
                transport=fake_transport,
                env={"ALPACA_SECRET_FILE": str(secret_path)},
            )

        self.assertEqual(result["live_observe_status"], "observed")
        self.assertEqual(result["provider_calls_performed"], 1)
        self.assertEqual(calls[0][1]["APCA-API-KEY-ID"], "unit_api_key")
        self.assertEqual(calls[0][1]["APCA-API-SECRET-KEY"], "unit_secret_key")
        self.assertTrue(calls[0][0].startswith("https://unit-data.alpaca.example/v2/stocks/SPY/snapshot"))
        self.assertEqual(result["broker_calls_performed"], 0)
        self.assertFalse(result["model_activation_performed"])

    def test_execute_live_observe_cli_plan_only_does_not_call_provider(self) -> None:
        request_payload = {
            "request_id": "rtlive_cli_unit",
            "sources": ["okx"],
            "instrument_refs": ["BTC-USDT"],
            "decision_time": "2026-05-11T13:30:00+00:00",
        }
        approval_payload = {
            "contract_type": "realtime_live_observe_approval_v1",
            "approval_id": "rtla_cli_unit",
            "approval_scope": "realtime_market_data_observe_only",
            "approved_sources": ["okx"],
            "approved_instrument_refs": ["BTC-USDT"],
            "approved_at_utc": "2026-05-11T13:00:00+00:00",
            "expires_at_utc": "2099-05-11T14:00:00+00:00",
            "max_provider_calls": 1,
            "execute_live_observe_allowed": True,
            "model_activation_allowed": False,
            "broker_execution_allowed": False,
            "broker_order_construction_allowed": False,
            "account_mutation_allowed": False,
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            request_path = Path(temp_dir) / "request.json"
            approval_path = Path(temp_dir) / "approval.json"
            request_path.write_text(json.dumps(request_payload), encoding="utf-8")
            approval_path.write_text(json.dumps(approval_payload), encoding="utf-8")
            result = subprocess.run(
                [sys.executable, "scripts/execution/execute_live_observe.py", "--request", str(request_path), "--approval", str(approval_path)],
                check=True,
                cwd="/root/projects/trading-execution",
                env={"PYTHONPATH": "src"},
                text=True,
                capture_output=True,
            )
        payload = json.loads(result.stdout)
        self.assertEqual(payload["live_observe_status"], "ready_requires_execute_live_observe_flag")
        self.assertEqual(payload["provider_calls_performed"], 0)

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

        with tempfile.TemporaryDirectory() as temp_dir:
            feature_path = Path(temp_dir) / "feature_snapshot.json"
            feature_result = subprocess.run(
                [
                    sys.executable,
                    "scripts/execution/build_realtime_feature_snapshot.py",
                    "--decision-time",
                    "2026-05-11T13:30:00+00:00",
                    "--available-time",
                    "2026-05-11T13:30:01+00:00",
                    "--tradeable-time",
                    "2026-05-11T13:30:02+00:00",
                    "--instrument-ref",
                    "AAPL",
                    "--historical-dataset-snapshot-ref",
                    "trading-model://snapshots/historical/unit",
                    "--frozen-model-config-ref",
                    "trading-model://configs/frozen/unit",
                    "--source-capture-ref",
                    "capture://alpaca/aapl/unit",
                ],
                check=True,
                cwd="/root/projects/trading-execution",
                env={"PYTHONPATH": "src"},
                text=True,
                capture_output=True,
            )
            feature_snapshot = json.loads(feature_result.stdout)
            feature_path.write_text(json.dumps(feature_snapshot), encoding="utf-8")
            self.assertEqual(feature_snapshot["provider_calls_performed"], 0)
            self.assertEqual(len(feature_snapshot["feature_rows"]), 8)

            decision_result = subprocess.run(
                [sys.executable, "scripts/execution/build_realtime_model_input.py", "--feature-snapshot", str(feature_path)],
                check=True,
                cwd="/root/projects/trading-execution",
                env={"PYTHONPATH": "src"},
                text=True,
                capture_output=True,
            )
            decision_input = json.loads(decision_result.stdout)
            decision_path = Path(temp_dir) / "decision_input.json"
            decision_path.write_text(json.dumps(decision_input), encoding="utf-8")
            self.assertEqual(decision_input["readiness_status"], "ready_for_historical_model_decision_handoff")

            decision_validation_result = subprocess.run(
                [sys.executable, "scripts/execution/validate_realtime_model_input.py", str(decision_path)],
                check=True,
                cwd="/root/projects/trading-execution",
                env={"PYTHONPATH": "src"},
                text=True,
                capture_output=True,
            )
            decision_validation = json.loads(decision_validation_result.stdout)
            self.assertTrue(decision_validation["valid"])
            self.assertFalse(decision_validation["model_activation_performed"])


if __name__ == "__main__":
    unittest.main()
