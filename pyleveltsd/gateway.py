import fnmatch
from django.conf import settings

from graphite.node import BranchNode, LeafNode
from graphite.intervals import Interval, IntervalSet

from time import time
import jsonrpclib


class LevelRpcFinder(object):
    def __init__(self, server_path=None, go_mode=None):
        self.server = server_path or settings.LEVEL_RPC_PATH
        self.go_mode = go_mode or getattr(settings, 'LEVEL_GO_MODE', False)

    def find_nodes(self, query):
        #TODO: not ignore time components

        return self._find_nodes(query.pattern.split('.'), 0, '')

    def _find_nodes(self, parts, current_level, parent_path):
        client = _get_rpc_client(self.server, self.go_mode)

        if len(parts) == current_level:  # we have fully eval'ed the path
            if client.is_node_leaf(parent_path):
                reader = LevelRpcReader(parent_path, self.server, self.go_mode)
                yield LeafNode(parent_path, reader)
            else:
                yield BranchNode(parent_path)
        else:  # we are still expanding a regex'ed path
            component = parts[current_level]
            new_path = '%s.%s' % (parent_path, component) if parent_path else component

            if '*' in component:  # does this segment need globbing?
                candidates = []
                for f in client.get_child_nodes(parent_path):
                    partial_path = '%s.%s' % (parent_path, f) if parent_path else f
                    candidates.append(partial_path)

                for y in fnmatch.filter(candidates, new_path):
                    for z in self._find_nodes(parts, current_level + 1, y):
                        yield z
            else:
                for x in self._find_nodes(parts, current_level + 1, new_path):
                    yield x


class LevelRpcReader(object):
    # TODO: fix the step
    _HARDCODED_STEP_IN_SECONDS = 60

    def __init__(self, metric_name, server_url, go_mode):
        self.metric = metric_name
        self.server = server_url
        self.step_in_seconds = self._HARDCODED_STEP_IN_SECONDS
        self.go_mode = go_mode

    def get_intervals(self):
        # pretend we support entire range for now
        return IntervalSet([Interval(1, int(time())), ])

    def fetch(self, startTime, endTime):
        client = _get_rpc_client(self.server, self.go_mode)
        values = client.get_range_data(self.metric, startTime, endTime)
        real_start = self._rounder(startTime)
        real_end = self._rounder(endTime)
        value_map = self._round_base_data(values)
        if value_map:
            ts = []

            for curr in xrange(real_start, real_end, self.step_in_seconds):
                ts.append(value_map.get(curr, None))

            time_info = (real_start, real_end, self.step_in_seconds)
        else:
            time_info = (real_start, real_end, self.step_in_seconds)
            ts = [None for i in xrange((real_end - real_start)/self.step_in_seconds + 1)]
        return (time_info, ts)

    def __repr__(self):
        return '<LevelRpcReader[%x]: %s>' % (id(self), self.metric)

    def _rounder(self, x):
        return int(x / self.step_in_seconds) * self.step_in_seconds

    def _round_base_data(self, b):
        retval = {}
        for kv in b:
            z = self._rounder(kv[0])
            retval[z] = kv[1]
        return retval


def _get_rpc_client(server, go_mode):
    retval = jsonrpclib.Server(server)
    return GoAdapter(retval) if go_mode else retval


class GoAdapter(object):
    def __init__(self, goClient):
        self.client = goClient

    def is_node_leaf(self, parent_path):
        request = {'Node': parent_path}
        response = self.client.ReaderService.IsNodeLeaf(request)
        return response.get("Yes", False)

    def get_child_nodes(self, parent_path):
        print 'serching in "%s"' % parent_path
        request = {'Node': parent_path}
        response = self.client.ReaderService.GetChildNodes(request)
        print response
        return response.get("Nodes", [])

    def get_range_data(self, metric, startTime, endTime):
        request = {'Node': metric, 'Start': startTime, 'End': endTime}
        response = self.client.ReaderService.GetRangeData(request)
        kv = response.get("Data", [])
        retval = [[x["Timestamp"], x["Value"]] for x in kv]
        return retval
