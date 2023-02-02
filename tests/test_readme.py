import doctest
from pathlib import Path
from unittest import TestCase

import mistletoe
from bs4 import BeautifulSoup

from tests.util import DocTestMixin


def readme():
    pass


with open(Path(__file__).parent.parent / "README.md") as f:
    md = mistletoe.markdown(f)
    bs = BeautifulSoup(md, features="html5lib")
    readme.__doc__ = "\n\n".join(example.text for example in bs.find_all("code"))


class ReadMeTestCase(DocTestMixin(readme), TestCase):
    pass
