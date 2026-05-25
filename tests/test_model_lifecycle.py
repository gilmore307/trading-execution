import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from trading_execution.model_lifecycle import (
    build_active_model_config_write,
    build_shadow_cycle_selection,
    shadow_runtime_component,
    simulate_c08_capacity,
    validate_active_model_config_write,
    validate_shadow_cycle_selection,
)


class ModelLifecycleTests(unittest.TestCase):
    def test_c08_capacity_simulation_admits_only_safe_parallel_groups(self) -> None:
        simulation = simulate_c08_capacity(
            requested_model_group_count=8,
            cpu_count=16,
            available_memory_mb=32768,
            live_reserved_cpu_count=4,
            per_group_worker_count=2,
            reserved_memory_mb=4096,
            per_group_memory_mb=2048,
            active_path_p95_ms=250,
            per_group_p95_ms=120,
            orchestration_overhead_ms=40,
            realtime_tick_budget_ms=1000,
        ).to_dict()

        self.assertEqual(simulation["contract_type"], "execution_c08_capacity_simulation")
        self.assertEqual(simulation["admitted_model_group_count"], 6)
        self.assertEqual(simulation["throttled_model_group_count"], 2)
        self.assertEqual(simulation["decision_status"], "capacity_limited")
        self.assertIn("cpu_capacity_limited", simulation["reason_codes"])
        self.assertEqual(simulation["historical_model_tasks_policy"], "pause_historical_model_tasks_during_live_runtime")

    def test_c08_capacity_simulation_blocks_when_live_path_uses_host(self) -> None:
        simulation = simulate_c08_capacity(
            requested_model_group_count=3,
            cpu_count=4,
            available_memory_mb=2048,
            live_reserved_cpu_count=4,
            reserved_memory_mb=2048,
        ).to_dict()

        self.assertEqual(simulation["admitted_model_group_count"], 0)
        self.assertEqual(simulation["decision_status"], "blocked")
        self.assertIn("no_shadow_model_group_capacity_available", simulation["reason_codes"])

    def test_shadow_runtime_component_is_intraday_and_not_replay(self) -> None:
        component = shadow_runtime_component().to_dict()

        self.assertEqual(component["contract_type"], "execution_shadow_runtime_component")
        self.assertEqual(component["component_step"], "C08")
        self.assertEqual(component["component_id"], "component_08_model_group_shadow_comparison")
        self.assertEqual(component["runtime_data_mode"], "realtime_market_hours_only")
        self.assertFalse(component["replay_allowed"])
        self.assertIn("promotion_readiness_record", component["input_contracts"])
        self.assertIn("runtime_capacity_snapshot", component["input_contracts"])
        self.assertIn("execution_shadow_model_runtime_evidence", component["output_contracts"])
        self.assertIn("execution_shadow_cycle_selection", component["output_contracts"])
        self.assertIn("Only the current active model", component["active_trading_authority_policy"])
        self.assertIn("without degrading C01-C06 latency", component["hardware_capacity_policy"])
        self.assertFalse(component["broker_mutation_allowed"])
        self.assertFalse(component["account_mutation_allowed"])
        self.assertFalse(component["active_pointer_write_allowed"])

    def test_shadow_cycle_selection_assigns_active_realtime_and_eliminate(self) -> None:
        selection = build_shadow_cycle_selection(
            cycle_ref="cycle://2026-06",
            current_active_model_ref="model://incumbent",
            candidate_reviews=[
                {
                    "candidate_model_ref": "model://winner",
                    "promotion_readiness_ref": "ready://winner",
                    "overall_rank": 1,
                    "review_status": "active_candidate",
                },
                {
                    "candidate_model_ref": "model://rank2",
                    "promotion_readiness_ref": "ready://rank2",
                    "overall_rank": 2,
                    "review_status": "realtime_candidate",
                },
                {
                    "candidate_model_ref": "model://rank3",
                    "promotion_readiness_ref": "ready://rank3",
                    "overall_rank": 3,
                    "review_status": "realtime_candidate",
                },
                {
                    "candidate_model_ref": "model://rank4",
                    "promotion_readiness_ref": "ready://rank4",
                    "overall_rank": 4,
                    "review_status": "shadow_continue",
                },
                {
                    "candidate_model_ref": "model://bad",
                    "promotion_readiness_ref": "ready://bad",
                    "overall_rank": 9,
                    "review_status": "eliminate_candidate",
                    "elimination_reason": "three consecutive shadow cycles with unstable tail loss",
                },
            ],
        )

        self.assertEqual(selection["contract_type"], "execution_shadow_cycle_selection")
        self.assertEqual(selection["selected_active_model_ref"], "model://winner")
        self.assertTrue(selection["active_model_switch_recommended"])
        self.assertEqual(selection["realtime_candidate_refs"], ["model://rank2", "model://rank3", "model://rank4"])
        self.assertEqual(selection["stable_wingman_refs"], ["model://rank2", "model://rank3", "model://rank4"])
        self.assertEqual(selection["probation_wingman_refs"], [])
        self.assertEqual(selection["rotating_challenger_refs"], [])
        self.assertEqual(selection["active_runtime_slots"], 4)
        self.assertEqual(selection["rerank_cadence"], "weekly")
        self.assertEqual(selection["eliminate_candidate_refs"], ["model://bad"])
        self.assertFalse(selection["active_model_config_write_performed"])
        self.assertFalse(selection["broker_order_construction_performed"])
        self.assertFalse(selection["broker_execution_performed"])
        self.assertFalse(selection["account_mutation_performed"])
        self.assertEqual(validate_shadow_cycle_selection(selection).validation_status, "passed")

    def test_shadow_cycle_selection_uses_one_wingman_slot_for_probation(self) -> None:
        selection = build_shadow_cycle_selection(
            cycle_ref="cycle://2026-07",
            current_active_model_ref="model://incumbent",
            candidate_reviews=[
                {
                    "candidate_model_ref": "model://winner",
                    "promotion_readiness_ref": "ready://winner",
                    "overall_rank": 1,
                    "review_status": "active_candidate",
                },
                {
                    "candidate_model_ref": "model://rank2",
                    "promotion_readiness_ref": "ready://rank2",
                    "overall_rank": 2,
                    "review_status": "stable_wingman",
                },
                {
                    "candidate_model_ref": "model://rank3",
                    "promotion_readiness_ref": "ready://rank3",
                    "overall_rank": 3,
                    "review_status": "stable_wingman",
                },
                {
                    "candidate_model_ref": "model://rank4",
                    "promotion_readiness_ref": "ready://rank4",
                    "overall_rank": 4,
                    "review_status": "rotating_challenger",
                },
                {
                    "candidate_model_ref": "model://rank5",
                    "promotion_readiness_ref": "ready://rank5",
                    "overall_rank": 5,
                    "review_status": "rotating_challenger",
                },
                {
                    "candidate_model_ref": "model://probation",
                    "promotion_readiness_ref": "ready://probation",
                    "overall_rank": 9,
                    "review_status": "elimination_probation",
                    "probation_reason": "candidate is under final realtime review before expedited elimination",
                },
                {
                    "candidate_model_ref": "model://shadow",
                    "promotion_readiness_ref": "ready://shadow",
                    "overall_rank": 10,
                    "review_status": "shadow_continue",
                },
            ],
        )

        self.assertEqual(selection["stable_wingman_refs"], ["model://rank2", "model://rank3"])
        self.assertEqual(selection["probation_wingman_refs"], ["model://probation"])
        self.assertEqual(selection["rotating_challenger_refs"], ["model://rank4", "model://rank5"])
        self.assertEqual(
            selection["realtime_candidate_refs"],
            ["model://rank2", "model://rank3", "model://probation", "model://rank4", "model://rank5"],
        )
        self.assertEqual(selection["shadow_only_candidate_refs"], ["model://shadow"])
        self.assertEqual(selection["active_runtime_slots"], 6)
        self.assertIn("probation_failed_enters_expedited_elimination_review", selection["probation_exit_policy"])
        self.assertEqual(validate_shadow_cycle_selection(selection).validation_status, "passed")

    def test_probation_candidate_requires_reason_and_is_single_slot(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires sufficient reason"):
            build_shadow_cycle_selection(
                cycle_ref="cycle://2026-07",
                current_active_model_ref="model://incumbent",
                candidate_reviews=[
                    {
                        "candidate_model_ref": "model://winner",
                        "promotion_readiness_ref": "ready://winner",
                        "overall_rank": 1,
                        "review_status": "active_candidate",
                    },
                    {
                        "candidate_model_ref": "model://probation",
                        "promotion_readiness_ref": "ready://probation",
                        "overall_rank": 9,
                        "review_status": "elimination_probation",
                    },
                ],
            )
        with self.assertRaisesRegex(ValueError, "only one elimination probation candidate"):
            build_shadow_cycle_selection(
                cycle_ref="cycle://2026-07",
                current_active_model_ref="model://incumbent",
                candidate_reviews=[
                    {
                        "candidate_model_ref": "model://winner",
                        "promotion_readiness_ref": "ready://winner",
                        "overall_rank": 1,
                        "review_status": "active_candidate",
                    },
                    {
                        "candidate_model_ref": "model://probation1",
                        "promotion_readiness_ref": "ready://probation1",
                        "overall_rank": 8,
                        "review_status": "elimination_probation",
                        "probation_reason": "first final review",
                    },
                    {
                        "candidate_model_ref": "model://probation2",
                        "promotion_readiness_ref": "ready://probation2",
                        "overall_rank": 9,
                        "review_status": "elimination_probation",
                        "probation_reason": "second final review",
                    },
                ],
            )

    def test_eliminate_candidate_requires_reason(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires sufficient reason"):
            build_shadow_cycle_selection(
                cycle_ref="cycle://2026-06",
                current_active_model_ref="model://incumbent",
                candidate_reviews=[
                    {
                        "candidate_model_ref": "model://bad",
                        "promotion_readiness_ref": "ready://bad",
                        "overall_rank": 9,
                        "review_status": "eliminate_candidate",
                    }
                ],
            )

    def test_active_model_config_write_requires_matching_previous_active(self) -> None:
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

        record = build_active_model_config_write(
            shadow_cycle_selection=selection,
            expected_previous_active_model_ref="model://incumbent",
            new_active_config_ref="storage://active/winner",
            rollback_ref="storage://active/incumbent",
            write_window_ref="window://closed-market",
        )

        self.assertEqual(record["contract_type"], "execution_active_model_config_write")
        self.assertEqual(record["selected_active_model_ref"], "model://winner")
        self.assertEqual(record["shadow_cycle_selection_ref"], selection["selection_id"])
        self.assertEqual(record["shadow_cycle_selection"], selection)
        self.assertTrue(record["active_pointer_write_performed"])
        self.assertTrue(record["rollback_available"])
        self.assertFalse(record["broker_order_construction_performed"])
        self.assertFalse(record["broker_execution_performed"])
        self.assertFalse(record["account_mutation_performed"])
        self.assertEqual(validate_active_model_config_write(record).validation_status, "passed")

        with self.assertRaisesRegex(ValueError, "must match"):
            build_active_model_config_write(
                shadow_cycle_selection=selection,
                expected_previous_active_model_ref="model://other",
                new_active_config_ref="storage://active/winner",
                rollback_ref="storage://active/incumbent",
                write_window_ref="window://closed-market",
            )

    def test_shadow_cycle_selection_rejects_forged_empty_review_payload(self) -> None:
        result = validate_shadow_cycle_selection(
            {
                "contract_type": "execution_shadow_cycle_selection",
                "selection_id": "selection://forged",
                "cycle_ref": "cycle://2026-06",
                "cycle_duration_days": 30,
                "generated_at_utc": "2026-06-30T21:00:00Z",
                "previous_active_model_ref": "model://incumbent",
                "selected_active_model_ref": "model://not-reviewed",
                "realtime_candidate_refs": [],
                "shadow_only_candidate_refs": [],
                "eliminate_candidate_refs": [],
                "candidate_review_rows": [],
                "active_model_config_write_performed": False,
                "broker_order_construction_performed": False,
                "broker_execution_performed": False,
                "account_mutation_performed": False,
            }
        )

        self.assertEqual(result.validation_status, "failed")
        self.assertIn("candidate_review_rows must be non-empty", result.errors)

    def test_shadow_cycle_selection_rejects_invalid_cycle_duration_without_raising(self) -> None:
        result = validate_shadow_cycle_selection(
            {
                "contract_type": "execution_shadow_cycle_selection",
                "selection_id": "selection://forged",
                "cycle_ref": "cycle://2026-06",
                "cycle_duration_days": "not-int",
                "generated_at_utc": "2026-06-30T21:00:00Z",
                "previous_active_model_ref": "model://incumbent",
                "selected_active_model_ref": "model://not-reviewed",
                "realtime_candidate_refs": [],
                "shadow_only_candidate_refs": [],
                "eliminate_candidate_refs": [],
                "candidate_review_rows": [],
                "active_model_config_write_performed": False,
                "broker_order_construction_performed": False,
                "broker_execution_performed": False,
                "account_mutation_performed": False,
            }
        )

        self.assertEqual(result.validation_status, "failed")
        self.assertIn("cycle_duration_days must be positive", result.errors)

    def test_lifecycle_validators_reject_non_object_payloads_without_raising(self) -> None:
        for payload in (None, [], "bad"):
            with self.subTest(payload=type(payload).__name__):
                selection_result = validate_shadow_cycle_selection(payload)  # type: ignore[arg-type]
                active_result = validate_active_model_config_write(payload)  # type: ignore[arg-type]

            self.assertEqual(selection_result.validation_status, "failed")
            self.assertEqual(selection_result.errors, ("payload must be an object",))
            self.assertEqual(active_result.validation_status, "failed")
            self.assertEqual(active_result.errors, ("payload must be an object",))

    def test_active_model_config_write_rejects_missing_embedded_selection(self) -> None:
        result = validate_active_model_config_write(
            {
                "contract_type": "execution_active_model_config_write",
                "active_model_config_write_id": "activewrite_forged",
                "shadow_cycle_selection_ref": "selection://missing",
                "shadow_cycle_selection_digest": "0" * 64,
                "previous_active_model_ref": "model://incumbent",
                "selected_active_model_ref": "model://not-reviewed",
                "expected_previous_active_model_ref": "model://incumbent",
                "new_active_config_ref": "storage://active/forged",
                "rollback_ref": "storage://active/incumbent",
                "write_window_ref": "window://closed-market",
                "written_at_utc": "2026-06-30T21:00:00Z",
                "active_pointer_write_performed": True,
                "rollback_available": True,
                "broker_order_construction_performed": False,
                "broker_execution_performed": False,
                "account_mutation_performed": False,
            }
        )

        self.assertEqual(result.validation_status, "failed")
        self.assertIn("shadow_cycle_selection must be embedded for pointer validation", result.errors)

    def test_cli_builds_shadow_cycle_selection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reviews.jsonl"
            path.write_text(
                "\n".join(
                    json.dumps(row)
                    for row in [
                        {
                            "candidate_model_ref": "model://incumbent",
                            "promotion_readiness_ref": "ready://incumbent",
                            "overall_rank": 1,
                            "review_status": "incumbent_active",
                        },
                        {
                            "candidate_model_ref": "model://rank2",
                            "promotion_readiness_ref": "ready://rank2",
                            "overall_rank": 2,
                            "review_status": "realtime_candidate",
                        },
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    "scripts/execution/build_shadow_cycle_selection.py",
                    "--cycle-ref",
                    "cycle://2026-06",
                    "--current-active-model-ref",
                    "model://incumbent",
                    "--candidate-reviews-jsonl",
                    str(path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["selected_active_model_ref"], "model://incumbent")
        self.assertFalse(payload["active_model_switch_recommended"])

    def test_cli_builds_active_model_config_write(self) -> None:
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
        with tempfile.TemporaryDirectory() as directory:
            selection_path = Path(directory) / "selection.json"
            selection_path.write_text(json.dumps(selection), encoding="utf-8")
            completed = subprocess.run(
                [
                    sys.executable,
                    "scripts/execution/build_active_model_config_write.py",
                    "--shadow-cycle-selection-json",
                    str(selection_path),
                    "--expected-previous-active-model-ref",
                    "model://incumbent",
                    "--new-active-config-ref",
                    "storage://active/winner",
                    "--rollback-ref",
                    "storage://active/incumbent",
                    "--write-window-ref",
                    "window://closed-market",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["contract_type"], "execution_active_model_config_write")
        self.assertTrue(payload["active_pointer_write_performed"])


if __name__ == "__main__":
    unittest.main()
