import unittest

from trading_execution.runtime import (
    CRYPTO_CANDIDATE_SYMBOLS,
    CRYPTO_SPOT_ACCOUNT_SLEEVE,
    CRYPTO_SPOT_INSTRUMENT_REFS,
    EQUITY_OPTIONS_ACCOUNT_SLEEVE,
    build_runtime_component_graph,
    runtime_account_sleeves,
    runtime_components,
    validate_same_component_graph,
)


class RuntimeComponentGraphTests(unittest.TestCase):
    def test_live_and_replay_use_same_component_shape(self) -> None:
        live = build_runtime_component_graph(mode="live")
        replay = build_runtime_component_graph(mode="replay")

        self.assertEqual(live["component_graph_policy"], "same_components_live_and_replay_different_adapters")
        self.assertNotEqual(live["adapter_profile"], replay["adapter_profile"])
        validation = validate_same_component_graph(live, replay)
        self.assertEqual(validation["validation_status"], "passed")
        self.assertEqual(live["component_order"], replay["component_order"])
        self.assertEqual(live["account_sleeves"], replay["account_sleeves"])

    def test_entry_component_does_not_call_layer_10(self) -> None:
        rows = {component.component_id: component for component in runtime_components()}

        entry = rows["entry_decision_engine"]
        self.assertNotIn("layer_10_event_risk_governor", entry.called_model_layers)
        self.assertIn("layer_04_event_failure_risk", entry.called_model_layers)
        self.assertIn("not_called", entry.layer_10_policy)

        failure = rows["failure_explanation_component"]
        self.assertEqual(failure.called_model_layers, ("layer_10_event_risk_governor",))
        self.assertIn("after_observed_model_or_trade_failure", failure.layer_10_policy)

    def test_order_intent_builder_has_no_model_calls_or_mutation(self) -> None:
        rows = {component.component_id: component for component in runtime_components()}
        order_builder = rows["order_intent_builder"]

        self.assertEqual(order_builder.called_model_layers, ())
        self.assertEqual(order_builder.output_contracts, ("execution_order_intent",))
        self.assertFalse(order_builder.broker_mutation_allowed)
        self.assertFalse(order_builder.account_mutation_allowed)

    def test_first_batch_contracts_are_declared(self) -> None:
        graph = build_runtime_component_graph(mode="replay")

        self.assertEqual(
            graph["required_first_batch_contracts"],
            [
                "target_allocation_snapshot",
                "entry_decision",
                "position_lifecycle_decision",
                "execution_order_intent",
            ],
        )
        self.assertIn("option_reexpression_decision", graph["required_second_batch_contracts"])
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
        self.assertFalse(crypto.option_reexpression_enabled)

        equity_options = sleeves[EQUITY_OPTIONS_ACCOUNT_SLEEVE]
        self.assertEqual(equity_options.allowed_asset_classes, ("us_equity", "us_etf", "us_option"))
        self.assertEqual(equity_options.candidate_symbols, ())
        self.assertIn("reviewed_equity_watchlist", equity_options.candidate_pool_policy)
        self.assertTrue(equity_options.option_reexpression_enabled)

    def test_components_use_account_sleeve_contracts(self) -> None:
        graph = build_runtime_component_graph(mode="replay")
        self.assertEqual(
            graph["account_sleeve_policy"],
            "separate_crypto_and_equity_options_accounts_no_cross_account_netting",
        )
        self.assertFalse(graph["side_effect_policy"]["cross_account_collateral_or_position_netting_allowed"])

        rows = {component.component_id: component for component in runtime_components()}
        for component_id in (
            "opportunity_risk_allocation_engine",
            "entry_decision_engine",
            "position_lifecycle_controller",
            "order_intent_builder",
        ):
            self.assertIn("account_sleeve_state_snapshot", rows[component_id].input_contracts)

        option_review = rows["option_reexpression_review"]
        self.assertEqual(option_review.account_sleeves, (EQUITY_OPTIONS_ACCOUNT_SLEEVE,))


if __name__ == "__main__":
    unittest.main()
