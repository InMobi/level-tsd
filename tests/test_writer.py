from pyleveltsd.writer import LevelTsdWriter
from pyleveltsd.reader import LevelTsdReader
from pyleveltsd.base import LevelTsdBase
from time import time
import unittest

class WriterTest(unittest.TestCase):
    def test_create_metric(self):
        tsd = LevelTsdBase('/tmp')
        writer = LevelTsdWriter(tsd)
        ddb = tsd.dir_db
        mdb = tsd.indexer

        self.assertTrue(writer.write("foo.bar", 1, 2.4))
        self.assertTrue(writer.write("foo.baz", int(time()), 12.1))
        self.assertTrue(writer.write("foo.baz", int(time()) - 90000, -1))
        self.assertTrue(writer.write("foo.bazigar", 112, 3.14))
        self.assertTrue(writer.write("foo.bazigar", 90000, 3.14))
        self.assertTrue(writer.write("foo.bazigar", 90000000, 2.18))

        tsd.flush(True)

        self.assertTrue(writer.purge_db_data("foo.bazigar"))
        self.assertTrue(writer.purge_db_data("foo"))



if __name__ == '__main__':
    unittest.main()
