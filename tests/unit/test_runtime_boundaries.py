from __future__ import annotations

import csv
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SPEC = REPO / "specs" / "014-runtime-boundaries"


class RuntimeBoundaryTest(unittest.TestCase):
    def test_audited_folders_and_declared_move_resolve(self) -> None:
        with (SPEC / "folder-audit.tsv").open(encoding="utf-8", newline="") as stream:
            rows = list(csv.DictReader(stream, delimiter="\t"))
        for row in rows:
            self.assertTrue((REPO / row["path"]).is_dir(), row["path"])
        with (SPEC / "move-map.tsv").open(encoding="utf-8", newline="") as stream:
            move = next(csv.DictReader(stream, delimiter="\t"))
        self.assertEqual([], list((REPO / move["old_path"]).glob("*.py")))
        self.assertTrue((REPO / move["new_path"]).is_dir())


if __name__ == "__main__":
    unittest.main()
