"""Column-level schema reference evidence contract."""

import unittest

from graph.schema_queries import _filter_column_reference_records


def _record(
    text: str,
    *,
    evidence_line: int = 108,
    start_line: int = 100,
) -> dict:
    return {
        "source_name": "checkout_process",
        "access_type": "WRITES",
        "evidence_line": evidence_line,
        "_reference_text": text,
        "_reference_start_line": start_line,
    }


class ColumnReferenceEvidenceTests(unittest.TestCase):
    def test_table_reference_keeps_all_records_without_internal_evidence_fields(self):
        records = [_record("SELECT member_id FROM orders")]

        self.assertEqual(
            _filter_column_reference_records(records, None),
            [
                {
                    "source_name": "checkout_process",
                    "access_type": "WRITES",
                    "evidence_line": 108,
                }
            ],
        )

    def test_column_reference_requires_exact_token_near_relation_evidence(self):
        lines = ["irrelevant"] * 20
        lines[8] = (
            "UPDATE orders SET order_status_cd = :status "
            "WHERE member_id = :member_id"
        )
        records = [_record("\n".join(lines))]

        self.assertEqual(
            len(_filter_column_reference_records(records, "member_id")),
            1,
        )
        self.assertEqual(
            _filter_column_reference_records(records, "member"),
            [],
        )

    def test_column_elsewhere_in_large_function_is_not_attributed_to_relation(self):
        lines = ["irrelevant"] * 40
        lines[0] = "SELECT member_id FROM members"
        lines[28] = "UPDATE orders SET order_status_cd = :status"
        records = [
            _record(
                "\n".join(lines),
                evidence_line=128,
                start_line=100,
            )
        ]

        self.assertEqual(
            _filter_column_reference_records(records, "member_id"),
            [],
        )

    def test_column_reference_without_line_anchored_evidence_is_not_guessed(self):
        record = _record("SELECT member_id FROM orders")
        record["_reference_start_line"] = None

        self.assertEqual(
            _filter_column_reference_records([record], "member_id"),
            [],
        )


if __name__ == "__main__":
    unittest.main()
