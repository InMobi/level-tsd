from datetime import datetime, timedelta
from time import clock
from calendar import timegm
from copy import deepcopy
from os.path import isdir
import hashlib
import struct
import json
from threading import Lock
import leveldb


class LevelTsdBase(object):
    _partition_cache = {}

    def __init__(self, basedir, **kwargs):
        self._base_path = basedir
        self._shards = {}
        self.indexer = MetricNormalizer('%s/tsd-map.db' % (self._base_path))
        self.dir_db = LevelDir('%s/tsd-dir.db' % (self._base_path))

    def get_shard(self, metric, epoch_seconds, create_if_absent=False):
        partition = self._get_partition(metric, epoch_seconds)
        retval = self._shards.get(partition)
        if retval is None: #  not in cache
            path = '%s/tsd-data-%s.db' % (self._base_path, partition)
            if isdir(path) or create_if_absent:
                # an uncached, prev handle needs to be addressed
                retval = LevelShard(path)
                if len(self._shards) > 15:
                    self._shards = {}
                self._shards[partition] = retval
        return retval

    def _get_partition(self, metric_name, epoch_seconds):
        # cache epoch - > partition mapping
        retval = self._partition_cache.get(epoch_seconds)
        if retval is None:
            retval = datetime.utcfromtimestamp(epoch_seconds).strftime('%Y%m%d')
            self._partition_cache[epoch_seconds] = retval

        # stoopid cache nuking to keep things under control
        if len(self._partition_cache) > 10000:
            self._partition_cache = {}

        return retval

    def _get_effective_key(self, metric_name, timestamp, create_if_absent=False):
        ms = self.indexer.get_metric_shortcut(metric_name)
        if create_if_absent and ms is None:
            ms = self.indexer.make_metric_shortcut(metric_name, self.dir_db)
        if ms is not None:
            return struct.pack('>16sq', ms, int(timestamp))
        else:
            return None

    def get_range_scanner(self, metric, start_time, end_time):
        print 'scan %s from %d to %d' % (metric, start_time, end_time)
        chunk_start = datetime.utcfromtimestamp(start_time)
        final_end = datetime.utcfromtimestamp(end_time)
        time_range = final_end - chunk_start
        if(time_range.days == 0):
            return (LevelShardQuery(self, metric, start_time, end_time), )
        else:
            parts = []

            first_day_end = deepcopy(chunk_start)
            first_day_end = first_day_end.replace(hour=23, minute=59, second=59)
            first_day_end = timegm(first_day_end.utctimetuple())
            parts.append(LevelShardQuery(self, metric, start_time, first_day_end))

            chunk_start = chunk_start + timedelta(days=1)
            chunk_start = chunk_start.replace(hour=0, minute=0, second=0, microsecond=0)

            while( _get_total_seconds(final_end - chunk_start) > 0):
                new_end = deepcopy(chunk_start)
                new_end = new_end.replace(hour=23, minute=59, second=59)
                start_ts = timegm(chunk_start.utctimetuple())
                end_ts = timegm(new_end.utctimetuple())
                parts.append(LevelShardQuery(self, metric, start_ts, end_ts))
                chunk_start = chunk_start + timedelta(days=1)

            last_day_begin = timegm(chunk_start.utctimetuple())
            parts.append(LevelShardQuery(self, metric, last_day_begin, end_time))
            return (x for x in parts)

    def flush(self, sync=False):
        for x in self._shards.itervalues():
            x.flush(sync)

    def purge_tree(self, metric_prefix):
        range = {}
        metric_prefix = metric_prefix.encode('utf-8')
        range['indexer'] = self.indexer.delete_metric_tree(metric_prefix)
        range['dir'] = self.dir_db.delete_dir_tree(metric_prefix)
        return range

class MetricNormalizer(object):
    _metric_map = {}

    def __init__(self, dbpath):
        self._map_db = leveldb.LevelDB(dbpath)

    def get_metric_shortcut(self, metric_name):
        retval = self._metric_map.get(metric_name)
        if retval is None:
            try:
                retval = self._map_db.Get(metric_name)
                self._metric_map[metric_name] = retval
                return retval
            except KeyError:
                    return None
        else:
            return retval

    def make_metric_shortcut(self, metric_name, dir_db):
        retval = self.get_metric_shortcut(metric_name)
        if retval is None:
            retval = hashlib.md5(metric_name).digest()
            self._map_db.Put(metric_name, retval)
            self._metric_map[metric_name] = retval
            dir_db.upsert(metric_name)
        return retval

    def delete_metric_tree(self, metric_prefix):
        count = 0
        for i in self._map_db.RangeIter(key_from = metric_prefix, key_to = metric_prefix + '\xff'):
            self._map_db.Delete(i[0])
            self._metric_map.pop(i[0],None)
            count = count + 1
        return count


class LevelDir(object):
    """ Handles operations related to storing hierarchical directory like paths

    The key is the name of the directory & the value is a json array of its
    immediate childern
    """

    def __init__(self, dbpath):
        self._dir_db = leveldb.LevelDB(dbpath)
        self.l = Lock()

    def upsert(self, metric):
        self.l.acquire()
        try:
            self._upsert(metric)
        finally:
            self.l.release()

    def get_children(self, parent):
        try:
            jser = self._dir_db.Get(parent)
            return json.loads(jser)
        except KeyError:
            return []

    def _upsert(self, metric, depth=1):
        try:
            jser = self._dir_db.Get(metric)
            return json.loads(jser)
        except KeyError:
            if(metric == ''):
                self._dir_db.Put('', '[]')
            else:
                parts = metric.rsplit('.', 1)
                if len(parts) == 1:
                    self._uppend('', parts[0], depth)
                else:
                    self._uppend(parts[0], parts[1], depth)
            return []

    def _uppend(self, parent, part, depth):
        siblings = self._upsert(parent, depth+1)
        siblings.append(part)
        retval = list(set(siblings))
        updated_kids = json.dumps(retval)
        self._dir_db.Put(parent, updated_kids, sync=True)

    def delete_dir_tree(self, metric_prefix):
        count = 0
        for i in self._dir_db.RangeIter(key_from = metric_prefix, key_to = metric_prefix + '\xff'):
            self._dir_db.Delete(i[0])
            count = count + 1
        return count

class LevelShard(object):
    _SERIALIZATION_SCHEME_KEY = '__l3xedfRCTNUI7EFuFIw2CyffG7ggL7h8RE1VtBOrCvVvpdCORvCIRfSc49Zr'
    _SERIALIZATION_TECHNIQUE = 'struct pack <d'

    def __init__(self, path):
        self._path = path
        self._db = leveldb.LevelDB(path)
        self._batch = leveldb.WriteBatch()
        self._init_db()
        self.x = 0
        self.lflush = clock()

    def write_number(self, key, value):
        if key is None:
            raise TypeError('key cannot be None')
        self._batch.Put(key, LevelShard._pack_number(value))
        self.x = self.x + 1
        now = clock()
        if(self.x > 1000 or (now - self.lflush) > 30):  # configurable size
            self.flush(False)
        return True

    def __del__(self):
        self._db.Write(self._batch, sync=True)

    def flush(self, sync_mode=False):
        self._db.Write(self._batch, sync=sync_mode)
        self.x = 0
        self._batch = leveldb.WriteBatch()
        self.lflush = clock()

    def _init_db(self):
        try:
            scheme = self._db.Get(LevelShard._SERIALIZATION_SCHEME_KEY)
            if scheme != LevelShard._SERIALIZATION_TECHNIQUE:
                raise Exception('format mismatch')
        except KeyError:
            self._db.Put(LevelShard._SERIALIZATION_SCHEME_KEY,
                         LevelShard._SERIALIZATION_TECHNIQUE)

    def get_serialization_technique(self):
        return self._db.Get(LevelShard._SERIALIZATION_SCHEME_KEY)

    @staticmethod
    def _pack_number(x):
        return struct.pack('<d', x)

    @staticmethod
    def _unpack_number(b):
        x, =  struct.unpack('<d', b)
        return x

    def __str__(self):
        return 'leveldb path is %s' % self._path


class LevelShardQuery(object):
    def __init__(self, ltsd, metric, start_time, end_time):
        self.start_key = ltsd._get_effective_key(metric, start_time)
        self.end_key = ltsd._get_effective_key(metric, end_time)
        self.shard = ltsd.get_shard(metric, start_time)

    def query(self):
        if self.start_key is None or self.end_key is None or self.shard is None:
            return
        else:
            for i in self.shard._db.RangeIter(self.start_key, self.end_key):
                (k, v) = struct.unpack('>16sq', i[0])
                yield (v, LevelShard._unpack_number(i[1]))


def _get_total_seconds(td):
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 1e6) / 1e6
