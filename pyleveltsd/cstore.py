import re
from carbon.database import TimeSeriesDatabase
from pyleveltsd.writer import LevelTsdWriter
from pyleveltsd.reader import LevelTsdReader
from pyleveltsd.base import LevelTsdBase
from txjsonrpc.web import jsonrpc


class LevelTsdCarbon(TimeSeriesDatabase):
    plugin_name = 'level-tsd'
    _pesudo_singleton = None

    @staticmethod
    def _scrub_metric(s):
        s = s.translate(None, r'?*[]/')  # smash wildcard charaters
        s = re.sub(r'\.+', r'.', s)  # squash multiple dots
        return s

    def __init__(self, settings):
        self.db = LevelTsdBase(settings['LOCAL_DATA_DIR'])
        self.writer = LevelTsdWriter(self.db)
        LevelTsdCarbon._pesudo_singleton = self

    def exists(self, metric):
        sm = LevelTsdCarbon._scrub_metric(metric)
        return self.db.indexer.get_metric_shortcut(sm)

    def create(self, metric, **options):
        sm = LevelTsdCarbon._scrub_metric(metric)
        self.db.indexer.make_metric_shortcut(sm, self.db.dir_db)

    def write(self, metric, datapoints):
        sm = LevelTsdCarbon._scrub_metric(metric)
        for point in datapoints:
            self.writer.write(sm, point[0], point[1])

    def close(self):
        self.db = None
        self.writer = None


class LevelTsdRpc(jsonrpc.JSONRPC):
    def __init__(self):
        self.reader = LevelTsdReader(LevelTsdCarbon._pesudo_singleton.db)
        self.writer = LevelTsdWriter(LevelTsdCarbon._pesudo_singleton.db)

    def jsonrpc_get_child_nodes(self, parent_path):
        return self.reader.get_child_nodes(parent_path)

    def jsonrpc_is_node_leaf(self, path):
        return self.reader.is_node_leaf(path)

    def jsonrpc_get_range_data(self, metric, start_time, end_time):
        return list(self.reader.get_range_data(metric, start_time, end_time))
