#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
# import csv
import sys


class HopcroftKarpMatch():

    def find_and_print(self):
        pass

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
    HopcroftKarpMatch().find_and_print(args.historical_data_file, args.open_shift_file)
