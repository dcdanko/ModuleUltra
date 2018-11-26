"""Test API."""

import unittest
from datasuper import Repo

from .base_test import BaseTestDataSuper


class TestDataSuperAPI(BaseTestDataSuper):
    """Test API."""

    def test_init(self):
        """Ensure repo can be created."""
        Repo.initRepo()


if __name__ == '__main__':
    unittest.main()
