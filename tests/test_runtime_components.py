import unittest

from trading_execution.runtime import (
    CRYPTO_CANDIDATE_SYMBOLS,
    CRYPTO_SPOT_ACCOUNT_SLEEVE,
    CRYPTO_SPOT_INSTRUMENT_REFS,
    EQUITY_OPTIONS_ACCOUNT_SLEEVE,
    build_runtime_component_graph,
    runtime_component_manifest,
    runtime_account_sleeves,
    runtime_components,
    validate_same_component_graph,
)


class RuntimeComponentGraphTests(unittest.TestCase):
    def test_live_and_replay_use_same_component_shape(self) -> None:
        live = build_runtime_component_graph(mode="live")
        replay = build_runtime_component_graph(mode="replay")

        self.assertEqual(live["component_graph_policy"], "same_use_case_components_live_and_replay_different_adapters")
        self.assertNotEqual(live["adapter_profile"], replay["adapter_profile"])
        validation = validate_same_component_graph(live, replay)
        self.assertEqual(validation["validation_status"], "passed")
        self.assertEqual(live["component_order"], replay["component_order"])
        self.assertEqual(live["component_sequence"], replay["component_sequence"])
        self.assertEqual(live["use_case_graphs"], replay["use_case_graphs"])
        self.assertEqual(live["account_sleeves"], replay["account_sleeves"])

    def test_components_have_intraday_step_numbers_and_short_names(self) -> None:
        graph = build_runtime_component_graph(mode="replay")

        self.assertEqual(
            graph["component_sequence"],
            [
                {
                    "component_step": "C01",
                    "component_name": "Intake",
                    "component_id": "component_01_intake",
                },
                {
                    "component_step": "C02",
                    "component_name": "Entry",
                    "component_id": "component_02_entry",
                },
                {
                    "component_step": "C03",
                    "component_name": "Lifecycle",
                    "component_id": "component_03_lifecycle",
                },
                {
                    "component_step": "C04",
                    "component_name": "Expression Review",
                    "component_id": "component_04_expression_review",
                },
                {
                    "component_step": "C05",
                    "component_name": "Order Intent",
                    "component_id": "component_05_order_intent",
                },
                {
                    "component_step": "C06",
                    "component_name": "Execution Gate",
                    "component_id": "component_06_execution_gate",
                },
                {
                    "component_step": "C07",
                    "component_name": "Failure Review",
                    "component_id": "component_07_failure_review",
                },
            ],
        )

    def test_c01_branches_entry_candidates_and_open_positions(self) -> None:
        graph = build_runtime_component_graph(mode="replay")

        paths = {row["use_case_id"]: row for row in graph["use_case_graphs"]}
        self.assertEqual(
            paths["candidate_entry_execution"]["component_ids"],
            [
                "component_01_intake",
                "component_02_entry",
                "component_04_expression_review",
                "component_05_order_intent",
                "component_06_execution_gate",
            ],
        )
        self.assertEqual(paths["candidate_entry_execution"]["source_pool"], "candidate_entry_pool")
        self.assertEqual(
            paths["open_position_lifecycle_execution"]["component_ids"],
            [
                "component_01_intake",
                "component_03_lifecycle",
                "component_04_expression_review",
                "component_05_order_intent",
                "component_06_execution_gate",
            ],
        )
        self.assertEqual(paths["open_position_lifecycle_execution"]["source_pool"], "open_position_pool")

    def test_components_declare_model_surfaces_not_retired_layers(self) -> None:
        rows = {component.component_id: component for component in runtime_components()}

        entry = rows["component_02_entry"]
        self.assertEqual(entry.required_model_surfaces, ("model_03_event_state", "model_04_unified_decision"))
        self.assertEqual(entry.optional_model_surfaces, ())
        self.assertIn("thesis_distribution_surface", entry.input_contracts)
        self.assertIn("direct_underlying_intent", entry.input_contracts)
        self.assertNotIn("unified_decision_vector", entry.input_contracts)

        expression = rows["component_04_expression_review"]
        self.assertIn("expression_probability_surface", expression.input_contracts)
        self.assertIn("option_expression_plan", expression.input_contracts)

        failure = rows["component_07_failure_review"]
        self.assertEqual(failure.required_model_surfaces, ())
        self.assertEqual(failure.optional_model_surfaces, ())

        manifest = runtime_component_manifest()
        manifest_text = repr(manifest)
        self.assertNotIn("called_model_layers", manifest_text)
        self.assertIn("model_03_event_state", manifest_text)

    def test_order_intent_component_has_no_model_calls_or_mutation(self) -> None:
        rows = {component.component_id: component for component in runtime_components()}
        order_builder = rows["component_05_order_intent"]

        self.assertEqual(order_builder.required_model_surfaces, ())
        self.assertEqual(order_builder.optional_model_surfaces, ())
        self.assertEqual(order_builder.output_contracts, ("execution_order_intent",))
        self.assertFalse(order_builder.broker_mutation_allowed)
        self.assertFalse(order_builder.account_mutation_allowed)

        execution_gate = rows["component_06_execution_gate"]
        self.assertIn("execution_gate_result", execution_gate.output_contracts)
        self.assertIn("agent_final_review", execution_gate.input_contracts)
        self.assertFalse(execution_gate.broker_mutation_allowed)
        self.assertFalse(execution_gate.account_mutation_allowed)

    def test_first_batch_contracts_are_declared(self) -> None:
        graph = build_runtime_component_graph(mode="replay")

        self.assertEqual(
            graph["required_first_batch_contracts"],
            [
                "execution_intake_snapshot",
                "entry_decision",
                "position_lifecycle_decision",
                "execution_order_intent",
                "execution_gate_result",
            ],
        )
        self.assertIn("expression_decision", graph["required_second_batch_contracts"])
        self.assertIn("failure_explanation_packet", graph["required_second_batch_contracts"])
        self.assertIn("simulated_fill_event", graph["required_second_batch_contracts"])

    def test_account_sleeves_keep_crypto_and_equity_options_separate(self) -> None:
        sleeves = {sleeve.sleeve_id: sleeve for sleeve in runtime_account_sleeves()}

        crypto = sleeves[CRYPTO_SPOT_ACCOUNT_SLEEVE]
        self.assertEqual(crypto.candidate_symbols, CRYPTO_CANDIDATE_SYMBOLS)
        self.assertEqual(CRYPTO_CANDIDATE_SYMBOLS, ("BTC", "ETH", "SOL"))
        self.assertEqual(crypto.candidate_instrument_refs, CRYPTO_SPOT_INSTRUMENT_REFS)
        self.assertEqual(CRYPTO_SPOT_INSTRUMENT_REFS, ("BTC-USDT", "ETH-USDT", "SOL-USDT"))
        self.assertEqual(crypto.allowed_asset_classes, ("crypto_spot",))
        self.assertFalse(crypto.expression_review_enabled)

        equity_options = sleeves[EQUITY_OPTIONS_ACCOUNT_SLEEVE]
        self.assertEqual(equity_options.allowed_asset_classes, ("us_equity", "us_etf", "us_option"))
        self.assertEqual(equity_options.candidate_symbols, ())
        self.assertIn("reviewed_equity_watchlist", equity_options.candidate_pool_policy)
        self.assertTrue(equity_options.expression_review_enabled)

    def test_components_use_account_sleeve_contracts(self) -> None:
        graph = build_runtime_component_graph(mode="replay")
        self.assertEqual(
            graph["account_sleeve_policy"],
            "separate_crypto_and_equity_options_accounts_no_cross_account_netting",
        )
        self.assertFalse(graph["side_effect_policy"]["cross_account_collateral_or_position_netting_allowed"])
        self.assertFalse(graph["side_effect_policy"]["replay_broker_mutation_allowed"])
        self.assertFalse(graph["side_effect_policy"]["replay_account_mutation_allowed"])
        self.assertFalse(graph["side_effect_policy"]["replay_order_state_mutation_allowed"])
        self.assertFalse(graph["side_effect_policy"]["replay_position_state_mutation_allowed"])

        rows = {component.component_id: component for component in runtime_components()}
        for component_id in (
            "component_01_intake",
            "component_03_lifecycle",
            "component_05_order_intent",
        ):
            self.assertIn("account_sleeve_state_snapshot", rows[component_id].input_contracts)
        self.assertNotIn("account_sleeve_state_snapshot", rows["component_02_entry"].input_contracts)

        expression_review = rows["component_04_expression_review"]
        self.assertEqual(expression_review.account_sleeves, (EQUITY_OPTIONS_ACCOUNT_SLEEVE,))

    def test_runtime_manifest_declares_use_case_graphs(self) -> None:
        manifest = runtime_component_manifest()

        self.assertEqual(manifest["contract_type"], "execution_runtime_component_manifest")
        self.assertEqual(manifest["component_order"][3], "component_04_expression_review")
        use_cases = {row["use_case_id"]: row for row in manifest["use_case_graphs"]}
        self.assertIn("candidate_entry_execution", use_cases)
        self.assertIn("open_position_lifecycle_execution", use_cases)
        self.assertIn("direct_underlying_execution", use_cases)
        self.assertIn("option_expression_execution", use_cases)
        self.assertIn("failure_diagnosis", use_cases)
        self.assertTrue(manifest["manifest_checksum"])


if __name__ == "__main__":
    unittest.main()
