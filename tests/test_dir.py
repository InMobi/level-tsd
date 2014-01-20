from pyleveltsd.writer import LevelTsdWriter
from pyleveltsd.reader import LevelTsdReader
from pyleveltsd.base import LevelTsdBase
from time import time
import unittest

class DirTest(unittest.TestCase):
    def test_dir_metric(self):
        tsd = LevelTsdBase('/tmp')
        ddb = tsd.dir_db
        ddb.upsert("a.b.c.d.january")
        ddb.upsert("a.b.c.d.january.monday")
        ddb.upsert("a.b.c.d.feburary")
        ddb.upsert("a.b.c.d.feburary.tuesday")
        ddb.upsert("a.b.c.d.feburary.wednesday")
        ddb.upsert("a.b.c.d.march.sunday.one")
        ddb.upsert("a.b.c.d.march.sunday.two")

        feb_child = ddb.get_children("a.b.c.d.feburary")
        self.assertEqual(set(feb_child), set(('tuesday', 'wednesday')))

if __name__ == '__main__':
    unittest.main()
