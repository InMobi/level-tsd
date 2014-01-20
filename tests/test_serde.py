import random
from pyleveltsd.base import LevelShard

import unittest


class Serde(unittest.TestCase):
    def test_pack_unpack_val(self):
        for i in xrange(1, 100 * 1000):
            x = random.random() * random.randint(0, 1000 ** 5)
            y = LevelShard._unpack_number(LevelShard._pack_number(x))
            self.assertEqual(x, y)

if __name__ == '__main__':
    unittest.main()
