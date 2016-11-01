#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function

import argparse
import csv
import sys

from collections import namedtuple

from work_types import WorkTypes
from lsi_search import Schedule, LsiSearch

OpenShift = namedtuple('OpenShift', 'work_day work_shift work_type')


class Node():
    ID_UNKNOWN = '__IDUNK__'

    def __init__(self, node_id, capacity=0):
        self.edges = {}
        if node_id:
            self.node_id = node_id
        else:
            self.node_id = self.ID_UNKNOWN
        self.capacity = capacity
        # self.flow = 0

    def connect_to(self, node_id, edge_weight=1, add_weight=False):
        if not add_weight:
            if node_id in self.edges:
                print("INFO: Node.connect_to -- updating weight from %s to %s for edge %s to %s" %
                      (self.edges[node_id], edge_weight, self.node_id, node_id))
            self.edges[node_id] = edge_weight
        else:
            if node_id not in self.edges:
                self.edges[node_id] = 0
            self.edges[node_id] += edge_weight


class Graph():
    ID_SOURCE = '__IDSRC__'
    ID_SINK = '__IDSNK__'

    def __init__(self):
        self.nodes = {}
        self.nodes[self.ID_SOURCE] = Node(self.ID_SOURCE)
        self.nodes[self.ID_SINK] = Node(self.ID_SINK)

    def add_node(self, node_id):
        if node_id not in self.nodes:
            self.nodes[node_id] = Node(node_id)

    def add_edge(self, source_node, sink_node, capacity, add_capacity=False):
        if source_node not in self.nodes or sink_node not in self.nodes:
            print("ERROR: Graph.add_edge -- invalid source (%s) or sink (%s)" % (source_node, sink_node))
            return False  # TODO: raise an error here instead?
        s = self.nodes[source_node]
        s.connect_to(sink_node, capacity, add_capacity)
        return True  # TODO: not needed if raising errors

    def add_leading_edge(self, sink_node, capacity):
        self.add_edge(self.ID_SOURCE, sink_node, capacity)

    def add_trailing_edge(self, source_node, capacity):
        self.add_edge(source_node, self.ID_SINK, capacity)

    def dump(self):
        for node in self.nodes:
            print("%s: " % node, end="")
            edges = self.nodes[node].edges
            for edge in edges:
                print("%s (%s), " % (edge, edges[edge]), end="")
            print()
        print()


class MaxFlowMatch():
    OPEN_SHIFT_COL_MAP = {
        'work_day': 'work_day',
        'work_shift': 'work_shift',
        'work_type': 'work_type'
    }
    WORK_TYPE_MAP = WorkTypes().map

    def __init__(self):
        self.schedule_graph = None

    def read_shift_csv(self, open_shift_stream):
        """ Read a CSV file and return a list of lists

        :open_shift_stream: CSV stream to read from
        :returns:           ???
        """
        shift_list = []
        for row in csv.DictReader(open_shift_stream, delimiter=','):
            try:
                work_day = int(row[self.OPEN_SHIFT_COL_MAP['work_day']])
            except:
                work_day = 0
            try:
                work_shift = int(row[self.OPEN_SHIFT_COL_MAP['work_shift']])
            except:
                work_shift = 0
            work_type = self.WORK_TYPE_MAP[row[self.OPEN_SHIFT_COL_MAP['work_type']]]

            shift_list.append(OpenShift(
                work_day=work_day,
                work_shift=work_shift,
                work_type=work_type
            ))
        return shift_list

    def initialize_graph(self):
        if self.schedule_graph is None:
            self.schedule_graph = Graph()

    def add_to_graph(self, shift_key, shift_candidates):
        HOURS_SHIFT = 8
        HOURS_WEEK = 40
        self.schedule_graph.add_node(shift_key)
        self.schedule_graph.add_leading_edge(shift_key, HOURS_SHIFT)
        for shift_candidate in shift_candidates:
            candidate_key = '{}'.format(shift_candidate.employee)
            self.schedule_graph.add_node(candidate_key)
            self.schedule_graph.add_edge(shift_key, candidate_key, HOURS_SHIFT)
            self.schedule_graph.add_trailing_edge(candidate_key, HOURS_WEEK)

    def find_and_print(self, hist_data_stream, open_shift_stream):
        lsi = LsiSearch()
        self.initialize_graph()
        shift_list = self.read_shift_csv(open_shift_stream)
        # schedule_size = len(shift_list)
        for shift in shift_list:
            print
            search_schedule = Schedule(
                work_day=shift.work_day,
                work_shift=shift.work_shift,
                work_type=shift.work_type,
                worked=1,
                employee_id=0
            )
            # TODO: pull out into a method?
            shift_key = '{}-{}-{}'.format(shift.work_day, shift.work_shift, shift.work_type)
            # print shift_key
            # TODO: go fix lsi, there are duplicate employee ids coming through
            shift_candidates = lsi.find_in_csv(hist_data_stream, search_schedule, len(shift_list))
            # print shift_candidates
            self.add_to_graph(shift_key, shift_candidates)

        print()
        self.schedule_graph.dump()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find the best matches for open shifts given historical data')
    parser.add_argument("open_shift_file",
                        type=argparse.FileType('r'),
                        help="CSV file with open shifts")
    parser.add_argument("historical_data_file",
                        nargs='?',
                        type=argparse.FileType('r'),
                        default=sys.stdin,
                        help="CSV file with historical data (or stdin)")
    args = parser.parse_args()
    MaxFlowMatch().find_and_print(args.historical_data_file, args.open_shift_file)
