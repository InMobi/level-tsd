

class LevelTsdReader(object):
    def __init__(self, cluster):
        self._ltb = cluster

    def get_child_nodes(self, parent_path):
        return self._ltb.dir_db.get_children(parent_path)

    def is_node_leaf(self, path):
        try:
            children = self._ltb.dir_db.get_children(path)
            return not children
        except KeyError:
            return True

    def get_range_data(self, metric, start_time, end_time):
        for segment_queries in self._ltb.get_range_scanner(metric, start_time, end_time):
            for item in segment_queries.query():
                yield item
