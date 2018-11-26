"""Test persistent classes."""

import os
import tempfile
import unittest
from shutil import rmtree


class BaseTestDataSuper(unittest.TestCase):
    """Test persistent classes."""

    def setUp(self):
        self.root_dir = os.getcwd()
        tdir = tempfile.mkdtemp()
        os.chdir(tdir)
        self.tdir = tdir

    def tearDown(self):
        rmtree(self.tdir)
        os.chdir(self.root_dir)
