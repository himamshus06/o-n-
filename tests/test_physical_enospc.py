import json
import tempfile
import unittest
from pathlib import Path

from physical_enospc import (
    record_assertion,
    require_assertion,
    write_report,
)


class PhysicalEnospcResultTests(unittest.TestCase):
    def test_successful_assertion_is_recorded(self):
        report = {}
        require_assertion(report, "check", True, "")
        self.assertEqual(report["assertions"], [{"name": "check", "passed": True, "detail": ""}])
        self.assertEqual(report.get("failures", []), [])

    def test_failed_assertion_raises_and_is_recorded(self):
        report = {}
        with self.assertRaises(AssertionError):
            require_assertion(report, "batch_keys_absent", False, "batch key visible")
        self.assertEqual(report["assertions"][0]["passed"], False)
        self.assertEqual(report["failures"][0]["assertion"], "batch_keys_absent")

    def test_report_persists_failed_result(self):
        report = {"result": "FAIL", "assertions": [], "failures": [{"assertion": "check"}]}
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "result.json"
            write_report(report, path)
            loaded = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(loaded["result"], "FAIL")
        self.assertEqual(loaded["failures"], [{"assertion": "check"}])

    def test_record_assertion_can_capture_nonfatal_status(self):
        report = {}
        record_assertion(report, "optional", False, "not tested")
        self.assertFalse(report["assertions"][0]["passed"])
        self.assertEqual(report["failures"][0]["detail"], "not tested")


if __name__ == "__main__":
    unittest.main()
