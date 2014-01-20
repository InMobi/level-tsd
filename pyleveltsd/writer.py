

class LevelTsdWriter(object):
    def __init__(self, cluster):
        self._ltb = cluster

    def write(self, metric_name, timestamp, value):
        db = self._ltb.get_shard(metric_name, timestamp, True)
        k = self._ltb._get_effective_key(metric_name, timestamp, True)
        return db.write_number(k, value)

    def purge_db_data(self, metric_prefix):
        return self._ltb.purge_tree(metric_prefix)
