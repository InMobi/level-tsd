from pyleveltsd.writer import LevelTsdWriter
from pyleveltsd.reader import LevelTsdReader
from pyleveltsd.base import LevelTsdBase
from time import time
import unittest

class ReaderTest(unittest.TestCase):
    def test_read_metric(self):
        tsd = LevelTsdBase('/tmp')
        reader = LevelTsdReader(tsd)
        writer = LevelTsdWriter(tsd)

        self.assertTrue(writer.write("just.purge", 1, 2.4))
        self.assertTrue(writer.write("just.delete", 1, 2.4))
        self.assertTrue(writer.write("fubar.zigar", 112, 3.14))
        self.assertTrue(writer.write("fubar.zigar", 90000, 3.14))
        self.assertTrue(writer.write("fubar.zigar", 90000000, 2.18))

        tsd.flush(True)

        just_children = reader.get_child_nodes('just')
        self.assertTrue('purge' in just_children)
        self.assertTrue('delete' in just_children)
        self.assertTrue(reader.is_node_leaf('just.purge'))

        dp = list(reader.get_range_data("just.purge", 1, 100))
        print dp
        self.assertEqual(len(dp), 1)

        dp = list(reader.get_range_data("fubar.zigar", 1, 100000))
        print dp
        self.assertEqual(len(dp), 2)

if __name__ == '__main__':
    unittest.main()
