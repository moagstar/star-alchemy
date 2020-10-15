import os
import subprocess
import unittest
from pathlib import Path


class TestPyCodeStyle(unittest.TestCase):
    def test_pycodestyle(self):
        for dir in os.scandir(Path(__file__).parent.parent):
            if os.path.isdir(dir.path) and not dir.name.startswith('.'):
                with self.subTest(dir.name):
                    p = subprocess.run(['pycodestyle', dir.path])
                    self.assertEqual(p.returncode, 0)
