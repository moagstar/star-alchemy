import subprocess
import unittest
from pathlib import Path


class TestMyPy(unittest.TestCase):
    def test_mypy(self):
        p = subprocess.run(['mypy', Path(__file__).parent.parent/'star_alchemy'])
        self.assertEqual(p.returncode, 0)
