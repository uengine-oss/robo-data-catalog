"""Column filtering for published source text."""

import unittest

from graph.schema_queries import _filter_column_references


def _record(text: str) -> dict:
    return {
        "source_name": "checkout_process",
        "access_type": "WRITES",
        "code_text": text,
    }


class ColumnReferenceEvidenceTests(unittest.TestCase):
    def test_table_reference_is_not_filtered(self):
        records = [_record("SELECT member_id FROM orders")]
        self.assertIs(_filter_column_references(records, None), records)

    def test_column_reference_requires_an_exact_token(self):
        records = [_record(
            "UPDATE orders SET order_status_cd = :status "
            "WHERE member_id = :member_id"
        )]

        self.assertEqual(len(_filter_column_references(records, "member_id")), 1)
        self.assertEqual(_filter_column_references(records, "member"), [])

    def test_column_can_appear_anywhere_in_published_source(self):
        source = "SELECT member_id FROM members\n" + ("irrelevant\n" * 40)
        records = [_record(source)]
        self.assertEqual(_filter_column_references(records, "member_id"), records)

    def test_missing_source_text_does_not_invent_evidence(self):
        self.assertEqual(
            _filter_column_references([_record("")], "member_id"),
            [],
        )


if __name__ == "__main__":
    unittest.main()
