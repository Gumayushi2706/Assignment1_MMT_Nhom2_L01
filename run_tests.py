"""
CO3093 - Automated Unit & Integration Test Suite.
Usage:
    python run_tests.py
"""
import unittest
import sys
import os

if __name__ == "__main__":
    test_dir = os.path.join(os.path.dirname(__file__), "tests")
    suite = unittest.defaultTestLoader.discover(test_dir, pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
