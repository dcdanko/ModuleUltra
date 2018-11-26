"""Test persistent classes."""

import os

import unittest
from datasuper import PersistentDict, PersistentSet

from .base_test import BaseTestDataSuper


class TestPresistence(BaseTestDataSuper):
    """Test persistent classes."""

    def test_persistent_dict_add(self):
        """Ensure items can be added to persistent dict."""
        fpath = os.path.join(self.tdir, 'test.yml')
        persistent_dict = PersistentDict(fpath)
        persistent_dict['1'] = 'a'
        del persistent_dict
        persistent_dict = PersistentDict(fpath)
        assert persistent_dict['1'] == 'a'

    def test_persistent_dict_keys(self):
        """Ensure keys in persistent dict work as expected."""
        fpath = os.path.join(self.tdir, 'test.yml')
        persistent_dict = PersistentDict(fpath)
        persistent_dict['1'] = 'a'
        assert '1' in persistent_dict.keys()

    def test_persistent_set_add(self):
        """Ensure items can be added to persistent set."""
        fpath = os.path.join(self.tdir, 'test.yml')
        persistent_set = PersistentSet(fpath)
        persistent_set.add('1')
        del persistent_set
        persistent_set = PersistentSet(fpath)
        assert '1' in persistent_set


if __name__ == '__main__':
    unittest.main()
