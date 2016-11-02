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
    """ Directed Graph Node
    """
    ID_ANONYMOUS = '__IDANON__'

    def __init__(self, node_id):
        """ Initialize a node

        :node_id:   the id for the new node
                    if no id is given, the node becomes anonymous
                    anonymous nodes cannot have edges added to them
                    and thus are useless
        """
        self.edges = {}
        if node_id:
            self.node_id = node_id
        else:
            self.node_id = self.ID_ANONYMOUS
        # self.capacity = capacity
        # self.flow = 0

    def connect_to(self, node_id, edge_weight=1, add_weight=False):
        """ Connect this node to another

        Add the other node to this node's edges index

        :node_id:       the node to connect to
        :edge_weight:   the capacipty of the new edge
        :add_weight:    add weight to any existing edges when true
                        otherwise existing edges have their weight updated
        """
        if node_id == self.ID_ANONYMOUS:
            print("WARNING: Node.connect_to -- not adding edge to anonymous node")
            return
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
    """ Directed Graph
    """
    ID_SOURCE = '__IDSRC__'
    ID_SINK = '__IDSNK__'

    def __init__(self):
        self.nodes = {}
        self.nodes[self.ID_SOURCE] = Node(self.ID_SOURCE)
        self.nodes[self.ID_SINK] = Node(self.ID_SINK)

    def add_node(self, node_id):
        """ Add a node to the graph

        :node_id:   the id for the new node
        """
        if node_id not in self.nodes:
            self.nodes[node_id] = Node(node_id)

    def add_edge(self, source_node, sink_node, capacity, add_capacity=False):
        """ Add an edge between two nodes

        :source_node:   where the edge begins
        :sink_node:     where the edge ends
        :capacity:      the flow across the edge
        :add_capacity:  if edge exists add to existing capacity
                        otherwise existing capacity is updated
        """
        if source_node not in self.nodes or sink_node not in self.nodes:
            print("ERROR: Graph.add_edge -- invalid source (%s) or sink (%s)" % (source_node, sink_node))
            return
        s = self.nodes[source_node]
        s.connect_to(sink_node, capacity, add_capacity)

    def add_leading_edge(self, sink_node, capacity):
        """ Add an edge from the graph source to this node

        :sink_node:     where the edge ends
        :capacity:      the flow across the edge
        """
        self.add_edge(self.ID_SOURCE, sink_node, capacity)

    def add_trailing_edge(self, source_node, capacity):
        """ Add and edge from this node to the graph sink

        :source_node:   where the edge ends
        :capacity:      the flow across the edge
        """
        self.add_edge(source_node, self.ID_SINK, capacity)

    def breadth_first_search(self, source_node, target_node):
        """ Perform a breadth first search, obviously

        :source_node:   starting point for the BFS
        :target_node:   stopping point for the BFS
        """
        pass

    def depth_first_search(self, source_node, target_node):
        """ Perform a breadth first search, obviously

        :source_node:   starting point for the BFS
        :target_node:   stopping point for the BFS
        """
        if source_node not in self.nodes:
            print("ERROR: Graph.depth_first_search -- invalid source (%s)" % source_node)
            return
        elif target_node not in self.nodes:
            print("ERROR: Graph.depth_first_search -- invalid target (%s)" % target_node)
            return

        # starting max capacity (should be restricted as graph is traveresed)
        capacity = sys.maxint
        # list of found nodes (which we should attempt to visit latter)
        node_list = []
        node_list.append(source_node)
        # list of capacities for the found nodes
        capacity_list = []
        capacity_list.append(capacity)
        # used to get the search path
        parent_list = []
        parent_list.append(0)
        # list of already visited nodes
        visited = {}

        while len(node_list):
            current_node = node_list.pop()
            current_capacity = capacity_list.pop()
            parent_list.pop()
            capacity = min(capacity, current_capacity)

            # # when the target node is found
            # if current_node == target_node:
            #     print("> %s (%s)" % (current_node, current_capacity))
            #     break

            # we have not previously seen this node
            if current_node not in visited:
                # print("> %s (%s)" % (current_node, current_capacity))
                visited[current_node] = 1
                # get the outbound edges from the current node
                edges = self.nodes[current_node].edges
                for i, edge in enumerate(edges):
                    if edge == target_node:
                        # add final items to the search path
                        parent_list.append(current_node)
                        parent_list.append(edge)
                        print(". %s (%s)" % (edge, current_capacity))
                        print()
                        # prepare to exit the outer while loop
                        node_list = []
                        # exit the inner for loop
                        break
                    # check that the edge has capacity
                    if edges[edge] > 0:
                        print(">%s> %s (%s)" % (i, edge, current_capacity))
                        # add to the list the while loop is pulling from
                        node_list.append(edge)
                        capacity_list.append(edges[edge])
                        parent_list.append(current_node)

        node_last = 0
        search_path = []
        for i in range((len(parent_list) - 1), 0, -1):
            if parent_list[i] != node_last:
                search_path.append(parent_list[i])
            node_last = parent_list[i]
        search_path.reverse()

        for sp in search_path:
            print("-> %s" % sp)

        print("  %s" % capacity)
        print()
        return capacity, search_path

    def edmonds_karp(self):
        """ Perform max-flow calculation using Ddmonds-Karp implementation
        """
        pass

    def dump(self):
        """ Print the graph
        """
        for node in self.nodes:
            print("%s: " % node, end="")
            edges = self.nodes[node].edges
            for edge in edges:
                print("%s (%s), " % (edge, edges[edge]), end="")
            print()
        print()
        self.depth_first_search(self.ID_SOURCE, self.ID_SINK)
        self.depth_first_search(self.ID_SOURCE, 'B0023')


class MaxFlowMatch():
    """ Match workers to shifts using a graph-based max-flow technique
    """
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
        :returns:           a list of OpenShift
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
        """ Initialize the graph object
        """
        if self.schedule_graph is None:
            self.schedule_graph = Graph()

    def add_to_graph(self, shift_key, shift_candidates):
        """ Add node (and associated edges) to graph

        :shift_key:         the shift node to add
        :shift_candidates:  list of OpenShift candidates to add/update nodes for
        """
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
        """ Find the best candidates for a set of open shifts
            Given historical data and desired shifts to fill

        :hist_data_stream:  file stream containing the historical data
        :open_shift_stream: file stream containing the open shift data
        """
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
