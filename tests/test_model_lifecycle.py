import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from trading_execution.model_lifecycle import build_shadow_cycle_selection, validate_shadow_cycle_selection


class ModelLifecycleTests(unittest.TestCase):
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
        self.assertEqual(selection["eliminate_candidate_refs"], ["model://bad"])
        self.assertFalse(selection["active_model_config_write_performed"])
        self.assertFalse(selection["broker_order_construction_performed"])
        self.assertFalse(selection["broker_execution_performed"])
        self.assertFalse(selection["account_mutation_performed"])
        self.assertEqual(validate_shadow_cycle_selection(selection).validation_status, "passed")

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


if __name__ == "__main__":
    unittest.main()
