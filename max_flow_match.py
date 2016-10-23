#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import csv
import sys

from collections import namedtuple

from work_types import WorkTypes
from lsi_search import Schedule, LsiSearch

OpenShift = namedtuple('OpenShift', 'work_day work_shift work_type')


class MaxFlowMatch():
    OPEN_SHIFT_COL_MAP = {
        'work_day': 'work_day',
        'work_shift': 'work_shift',
        'work_type': 'work_type'
    }
    WORK_TYPE_MAP = WorkTypes().map

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

    def find_and_print(self, hist_data_stream, open_shift_stream):
        lsi = LsiSearch()
        shift_list = self.read_shift_csv(open_shift_stream)
        for shift in shift_list:
            print
            search_schedule = Schedule(
                work_day=shift.work_day,
                work_shift=shift.work_shift,
                work_type=shift.work_type,
                worked=1,
                employee_id=0
            )
            print lsi.find_in_csv(hist_data_stream, search_schedule, len(shift_list))

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
