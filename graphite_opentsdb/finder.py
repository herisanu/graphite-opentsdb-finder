from __future__ import division

from cacheback.decorators import cacheback
from django.conf import settings
from graphite.intervals import Interval, IntervalSet
from graphite.node import BranchNode, LeafNode
from graphite.logger import log
from graphite.finders.utils import BaseFinder
from graphite.readers.utils import BaseReader
import re
import requests
import time
import threading

from . import app_settings

class OpenTSDBNodeMixin(object):
    def __init__(self, name, *args):
        super(OpenTSDBNodeMixin, self).__init__(*args)
        self.name = name


class OpenTSDBLeafNode(OpenTSDBNodeMixin, LeafNode):
    def fetch(self, startTime, endTime, now=None, requestContext=None):
        try:
            result = self.reader.fetch(startTime, endTime, now, requestContext)
        except TypeError:
            # Support for legacy 3rd party, readers.
            result = self.reader.fetch(startTime, endTime)
        
        log.info('$$$$$$$$$$$$$ {0}'.format(result))
        return result


class OpenTSDBBranchNode(OpenTSDBNodeMixin, BranchNode):
    pass


def find_nodes_from_pattern(opentsdb_uri, opentsdb_tree, pattern):
    log.info(">> find_nodes_from_pattern({0}, {1}, {2})".format(opentsdb_uri, opentsdb_tree, pattern))
    query_parts = []
    for part in pattern.split('.'):
        part = part.replace('*', '.*')
        part = re.sub(
            r'{([^{]*)}',
            lambda x: "(%s)" % x.groups()[0].replace(',', '|'),
            part,
        )
        query_parts.append(part)

    shared_reader = SharedReader()
    nodes = list(find_opentsdb_nodes(opentsdb_uri, query_parts, "%04X" % opentsdb_tree, shared_reader=shared_reader))
    if nodes:
        log.info("DD {0}".format(nodes))
    shared_reader.node_count = len(nodes)
    #for node in nodes:
    #    yield node
    return nodes


#@cacheback(app_settings.OPENTSDB_CACHE_TIME)
def get_opentsdb_url(opentsdb_uri, url):
    full_url = "%s/%s" % (opentsdb_uri, url)
    return requests.get(full_url).json()


def find_opentsdb_nodes(opentsdb_uri, query_parts, current_branch, shared_reader, path=''):
    log.info(">> find_opentsdb_nodes({0}, {1}, {2}, {3}, {4})".format(opentsdb_uri, query_parts, current_branch, shared_reader, path))
    query_regex = re.compile(query_parts[0])
    for node, node_data in get_branch_nodes(opentsdb_uri, current_branch, shared_reader, path):
        node_name = node_data['displayName']
        dot_count = node_name.count('.')

        if dot_count:
            node_query_regex = re.compile(r'\.'.join(query_parts[:dot_count+1]))
        else:
            node_query_regex = query_regex

        if node_query_regex.match(node_name):
            if len(query_parts) == 1:
                yield node
            elif node.is_leaf:
                yield node
            elif not node.is_leaf:
                # We might need to split into two branches here
                # if using dotted nodes, as we can't tell if the UI
                # wanted all nodes with a single * (from advanced mode)
                # or if a node like a.b is supposed to be matched by *.*
                if query_parts[dot_count+1:]:
                    for inner_node in find_opentsdb_nodes(
                        opentsdb_uri,
                        query_parts[dot_count+1:],
                        node_data['branchId'],
                        shared_reader,
                        node.path,
                    ):
                        yield inner_node
                if dot_count and query_parts[0] == '.*':
                    for inner_node in find_opentsdb_nodes(
                        opentsdb_uri,
                        query_parts[1:],
                        node_data['branchId'],
                        shared_reader,
                        node.path,
                    ):
                        yield inner_node


def get_branch_nodes(opentsdb_uri, current_branch, shared_reader, path):
    log.info(">> get_branch_nodes({0}, {1}, {2}, {3})".format(opentsdb_uri, current_branch, shared_reader, path))
    results = get_opentsdb_url(opentsdb_uri, "tree/branch?branch=%s" % current_branch)
    if results:
        if path:
            path += '.'
        if results['branches']:
            for branch in results['branches']:
                yield OpenTSDBBranchNode(branch['displayName'], path + branch['displayName']), branch
        if results['leaves']:
            for leaf in results['leaves']:
                reader = OpenTSDBReader(
                    opentsdb_uri,
                    leaf,
                    shared_reader,
                )
                yield OpenTSDBLeafNode(leaf['displayName'], path + leaf['displayName'], reader), leaf


class OpenTSDBFinder(BaseFinder):
    def __init__(self, opentsdb_uri=None, opentsdb_tree=None):
        self.opentsdb_uri = (opentsdb_uri or app_settings.OPENTSDB_URI).rstrip('/')
        self.opentsdb_tree = opentsdb_tree or app_settings.OPENTSDB_TREE

    def find_nodes(self, query):
        log.info(query)
        for node in find_nodes_from_pattern(self.opentsdb_uri, self.opentsdb_tree, query.pattern):
            log.info('******************* {0}'.format(node))
            yield node


class SharedReader(object):
    def __init__(self):
        self.worker = threading.Semaphore(1)
        self.config_lock = threading.Lock()
        self.workers = {}
        self.results = {}
        self.result_events = {}

    def get(self, opentsdb_uri, aggregation_interval, leaf_data, start, end):
        key = (opentsdb_uri, aggregation_interval, leaf_data['metric'], start, end)
        with self.config_lock:
            if key not in self.workers:
                self.workers[key] = threading.Semaphore(1)
                self.result_events[key] = threading.Event()

        if self.workers[key].acquire(False):
            # we are the worker, do the work
            data = requests.get("%s/query?m=sum:%ds-avg:%s{%s}&start=%d&end=%d&show_tsuids=true" % (
                opentsdb_uri,
                aggregation_interval,
                leaf_data['metric'],
                ','.join(["%s=*" % t for t in leaf_data['tags']]),
                start,
                end,
            )).json()

            self.results[key] = {}
            for metric in data:
                assert len(metric['tsuids']) == 1
                self.results[key][metric['tsuids'][0]] = [metric]
            self.result_events[key].set()

        self.result_events[key].wait()
        tsuid = leaf_data['tsuid']
        if tsuid in self.results[key]:
            return self.results[key][tsuid]
        else:
            return []


class OpenTSDBReader(BaseReader):
    __slots__ = ('opentsdb_uri', 'leaf_data', 'shared_reader',)
    supported = True
    step = app_settings.OPENTSDB_DEFAULT_AGGREGATION_INTERVAL

    def __init__(self, opentsdb_uri, leaf_data, shared_reader):
        self.opentsdb_uri = opentsdb_uri
        self.leaf_data = leaf_data
        self.shared_reader = shared_reader
        #log.info("## OpenTSDBReader({0}, {1}, {2})".format(opentsdb_uri, leaf_data, shared_reader))

    def get_intervals(self):
        log.info("------->> get_intervals()")
        return IntervalSet([Interval(0, time.time())])


    def fetch(self, patterns, start_time, end_time, now=None, requestContext=None):
        log.info(">~> fetch({0}, {1}, {2}, {3}, {4}, {5})".format(patterns, start_time, end_time, now, requestContext))
        return []

    def fetch(self, startTime, endTime):
        log.info("------->> fetch({0}, {1})".format(startTime, endTime))
        data = requests.get("%s/query?tsuid=sum:%ds-avg:%s&start=%d&end=%d" % (
                    self.opentsdb_uri,
                    app_settings.OPENTSDB_DEFAULT_AGGREGATION_INTERVAL,
                    self.leaf_data['tsuid'],
                    int(startTime),
                    int(endTime),
                )).json()
        log.info("<< {0}".format(data))
        time_info = (startTime, endTime, self.step)
        number_points = int((endTime-startTime)//self.step)
        datapoints = [None for i in range(number_points)]

        for series in data:
            for timestamp, value in series['dps'].items():
                timestamp = int(timestamp)
                interval = timestamp - (timestamp % app_settings.OPENTSDB_DEFAULT_AGGREGATION_INTERVAL)
                index = (interval - int(startTime)) // self.step
                datapoints[index] = value

        return (time_info, datapoints)
