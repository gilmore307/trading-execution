from __future__ import annotations

import json
import subprocess
import sys
import unittest

from trading_execution.broker import broker_interfaces, build_execution_capability_catalog
from trading_execution.market_data import (
    model_decision_input_snapshot_contract,
    realtime_capture_contract,
    realtime_data_interfaces,
    realtime_feature_snapshot_contract,
    realtime_input_coverage_matrix,
)


class ExecutionCapabilityCatalogTests(unittest.TestCase):
    def test_realtime_data_interfaces_distinguish_historical_source_from_realtime_route(self) -> None:
        interfaces = {interface.source_id: interface for interface in realtime_data_interfaces()}

        self.assertIn("okx", interfaces)
        self.assertIn("okx_public_websocket", interfaces["okx"].realtime_interfaces)
        self.assertEqual(interfaces["okx"].canonical_historical_source_id, "okx")
        self.assertIn("historical OKX data", interfaces["okx"].boundary_note)
        self.assertIn("alpaca", interfaces)
        self.assertIn("thetadata", interfaces)

    def test_realtime_input_coverage_matrix_covers_all_model_layers(self) -> None:
        coverage = {row.model_layer: row for row in realtime_input_coverage_matrix()}

        self.assertEqual(len(coverage), 5)
        self.assertEqual(coverage["model_01_background_context"].model_id, "model_01_background_context")
        self.assertIn("alpaca", coverage["model_01_background_context"].primary_realtime_sources)
        self.assertIn("okx", coverage["model_01_background_context"].primary_realtime_sources)
        self.assertIn("proxy_gap_review_required", coverage["model_01_background_context"].coverage_status)
        self.assertEqual(coverage["model_03_event_state"].model_id, "model_03_event_state")
        self.assertIn("derived_governance_context", coverage["model_03_event_state"].primary_realtime_sources)
        self.assertIn("realtime_calendar_context", coverage["model_03_event_state"].primary_realtime_sources)
        self.assertEqual(coverage["model_04_unified_decision"].model_id, "model_04_unified_decision")
        self.assertIn("derived_model_context", coverage["model_04_unified_decision"].primary_realtime_sources)
        self.assertIn("execution_account_state", coverage["model_04_unified_decision"].primary_realtime_sources)
        self.assertIn("realtime_calendar_context", coverage["model_04_unified_decision"].primary_realtime_sources)
        self.assertIn("thetadata", coverage["model_05_option_expression"].primary_realtime_sources)
        self.assertIn("option_chain_snapshot", coverage["model_05_option_expression"].realtime_input_groups)
        for row in coverage.values():
            self.assertEqual(row.contract_type, "execution_realtime_input_coverage")
            self.assertIn("observation_time", row.required_capture_fields)
            self.assertIn("tradeable_time", row.required_capture_fields)

    def test_realtime_capture_contract_is_append_only_and_non_mutating(self) -> None:
        contract = realtime_capture_contract()

        self.assertEqual(contract.contract_type, "realtime_capture_contract")
        self.assertEqual(contract.accepted_dataset_roles, ("forward_holdout", "shadow_monitoring"))
        self.assertIn("frozen_model_config_ref", contract.required_fields)
        self.assertIn("label_maturity_time", contract.required_fields)
        self.assertIn("historical_snapshot_rewrite", contract.forbidden_actions)
        self.assertIn("broker_order_mutation", contract.forbidden_actions)
        self.assertIn("ready_signal", contract.manager_handoff_refs)

    def test_realtime_feature_covers_layers_and_decision_input_routes_components(self) -> None:
        feature_contract = realtime_feature_snapshot_contract()
        decision_contract = model_decision_input_snapshot_contract()

        self.assertEqual(feature_contract["contract_type"], "realtime_feature_snapshot_contract")
        self.assertEqual(len(feature_contract["required_layer_rows"]), 5)
        self.assertIn("historical_dataset_snapshot_ref", feature_contract["required_fields"])
        self.assertEqual(
            decision_contract["contract_type"],
            "execution_model_decision_input_snapshot_contract",
        )
        self.assertEqual(len(decision_contract["required_component_inputs"]), 5)
        self.assertIn("component_01_intake", decision_contract["required_component_inputs"])
        self.assertIn("component_06_execution_gate", decision_contract["required_component_inputs"])
        self.assertIn("model_activation", decision_contract["forbidden_actions"])

    def test_broker_catalog_accepts_okx_but_defers_firstrade(self) -> None:
        brokers = {broker.broker_id: broker for broker in broker_interfaces()}

        self.assertEqual(brokers["okx"].official_api_status, "official_api_available")
        self.assertFalse(brokers["okx"].order_mutation_enabled)
        self.assertIn("trade_risk_cap_valid", brokers["okx"].required_pre_order_gates)
        self.assertEqual(brokers["firstrade"].official_api_status, "no_official_api_found")
        self.assertEqual(
            brokers["firstrade"].implementation_status,
            "deferred_do_not_automate_reverse_engineered_login_or_order_flow",
        )
        self.assertFalse(brokers["firstrade"].order_mutation_enabled)
        self.assertEqual(brokers["alpaca_paper"].official_api_status, "official_paper_trading_api_available")
        self.assertTrue(brokers["alpaca_paper"].order_mutation_enabled)

    def test_combined_catalog_has_no_side_effects_or_mutation_enabled(self) -> None:
        catalog = build_execution_capability_catalog()

        self.assertEqual(catalog["contract_type"], "execution_capability_catalog")
        self.assertEqual(len(catalog["realtime_input_coverage_matrix"]), 5)
        self.assertEqual(catalog["realtime_capture_contract"]["contract_type"], "realtime_capture_contract")
        self.assertEqual(
            catalog["realtime_feature_snapshot_contract"]["contract_type"],
            "realtime_feature_snapshot_contract",
        )
        self.assertEqual(
            catalog["model_decision_input_snapshot_contract"]["contract_type"],
            "execution_model_decision_input_snapshot_contract",
        )
        self.assertFalse(catalog["order_mutation_enabled"])
        self.assertTrue(catalog["paper_order_mutation_enabled"])
        self.assertEqual(catalog["provider_calls_performed"], 0)
        self.assertEqual(catalog["broker_calls_performed"], 0)

    def test_capability_cli_prints_catalog(self) -> None:
        result = subprocess.run(
            [sys.executable, "scripts/execution/list_execution_capabilities.py"],
            check=True,
            cwd="/root/projects/trading-execution",
            env={"PYTHONPATH": "src"},
            text=True,
            capture_output=True,
        )
        payload = json.loads(result.stdout)

        self.assertEqual(payload["contract_type"], "execution_capability_catalog")
        self.assertEqual(len(payload["realtime_input_coverage_matrix"]), 5)
        self.assertFalse(payload["order_mutation_enabled"])


if __name__ == "__main__":
    unittest.main()
