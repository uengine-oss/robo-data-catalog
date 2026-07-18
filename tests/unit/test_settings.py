import os
import unittest
from unittest.mock import patch

from settings import CatalogGraphDatabaseSettings, _bounded_int, _strict_bool


class CatalogSettingsTest(unittest.TestCase):
    def test_boolean_parser_is_fail_closed(self):
        with patch.dict(os.environ, {"ROBO_CATALOG_TEST_BOOL": "yes"}):
            with self.assertRaises(ValueError):
                _strict_bool("ROBO_CATALOG_TEST_BOOL", False)

    def test_integer_parser_enforces_bounds(self):
        with patch.dict(os.environ, {"ROBO_CATALOG_TEST_PORT": "70000"}):
            with self.assertRaises(ValueError):
                _bounded_int("ROBO_CATALOG_TEST_PORT", 15503, 1, 65535)

    def test_system_database_is_forbidden(self):
        with self.assertRaises(ValueError):
            CatalogGraphDatabaseSettings(database="system")


if __name__ == "__main__":
    unittest.main()
