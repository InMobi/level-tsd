''' file name does not being with test_ as we do not want this module to be
autodiscovered. We do not want this modeule to be autodiscovered as it
requires carbon to be available on the python path'''

from pyleveltsd.cstore import LevelTsdCarbon
from time import time
import unittest


class CP(unittest.TestCase):
    def test(self):
        tsd = LevelTsdCarbon({'LOCAL_DATA_DIR': '/tmp'})
        tsd.write("foo.bar", ((1, 2.4),))
        tsd.write("foo.bien", ((int(time()), 12.12),))
        self.assertTrue(tsd.exists('foo.bien'))

        tsd.create('ailaaa')


if __name__ == '__main__':
    unittest.main()
