from __future__ import annotations

import json
import subprocess
import sys
import unittest

from trading_execution.broker import broker_interfaces, build_execution_capability_catalog
from trading_execution.market_data import realtime_data_interfaces


class ExecutionCapabilityCatalogTests(unittest.TestCase):
    def test_realtime_data_interfaces_distinguish_historical_source_from_realtime_route(self) -> None:
        interfaces = {interface.source_id: interface for interface in realtime_data_interfaces()}

        self.assertIn("okx", interfaces)
        self.assertIn("okx_public_websocket", interfaces["okx"].realtime_interfaces)
        self.assertEqual(interfaces["okx"].canonical_historical_source_id, "okx")
        self.assertIn("historical OKX data", interfaces["okx"].boundary_note)
        self.assertIn("alpaca", interfaces)
        self.assertIn("thetadata", interfaces)

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

    def test_combined_catalog_has_no_side_effects_or_mutation_enabled(self) -> None:
        catalog = build_execution_capability_catalog()

        self.assertEqual(catalog["contract_type"], "execution_capability_catalog_v1")
        self.assertFalse(catalog["order_mutation_enabled"])
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

        self.assertEqual(payload["contract_type"], "execution_capability_catalog_v1")
        self.assertFalse(payload["order_mutation_enabled"])


if __name__ == "__main__":
    unittest.main()
