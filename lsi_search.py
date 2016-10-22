#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import csv
import numpy
import math
import sys

from operator import attrgetter
from collections import namedtuple

Schedule = namedtuple('Schedule', 'work_day work_shift work_type worked employee_id')
SearchResult = namedtuple('SearchResult', 'score index employee work_day work_shift work_type worked worked_count')


class LsiSearch():
    COLS_MAP = {'work_day': 0, 'work_shift': 1, 'work_type': 2, 'worked': 3, 'employee_id': 4}
    # these need to be grouped by similarity, maybe higher values can cover lower values
    # (is it cheaper to schedule a more qualified employee than to pay overtime to the correct qualified?)
    WORK_MAP = {'CNA': 0, 'LPN': 1, 'ZZZ': 2}
    BOOL_MAP = {'False': 0, 'True': 1}

    WORK_COUNT_FACTOR = .01

    def read_csv(self, filehandle):
        """ Read a CSV file and return a list of lists

        :filehandle:    CSV stream to read from
        :returns:       a list (of CSV rows) of lists (of CSV columns),
                        a list of employees
        """
        # csv_reader = csv.DictReader(filehandle, delimiter=',')
        # for row in csv_reader:
        #     print row["work_type"]
        csv_reader = csv.reader(filehandle, delimiter=',')
        csv_list = list(csv_reader)
        schedule_list = []
        employee_ids = []
        csv_index = {}
        for i, line in enumerate(csv_list):
            # skip the first (header) line
            if i:
                work_day = int(line[self.COLS_MAP['work_day']])
                work_shift = int(line[self.COLS_MAP['work_shift']])
                work_type = self.WORK_MAP[line[self.COLS_MAP['work_type']]]
                worked = self.BOOL_MAP[line[self.COLS_MAP['worked']]]
                employee_id = line[self.COLS_MAP['employee_id']]

                csv_key = '{}-{}-{}-{}-{}'.format(work_day, work_shift, work_type, worked, employee_id)
                if csv_key not in csv_index:
                    csv_item_detail = {}
                    csv_item_detail['id'] = len(schedule_list)
                    csv_item_detail['count'] = 1
                    csv_index[csv_key] = csv_item_detail
                else:
                    csv_item_detail = csv_index[csv_key]
                    csv_item_detail['count'] += 1
                    csv_index[csv_key] = csv_item_detail

                if csv_index[csv_key]['count'] == 1:
                    employee_ids.append(employee_id)
                    schedule_list.append([
                        work_day,
                        work_shift,
                        work_type,
                        worked,
                        self.WORK_COUNT_FACTOR,
                        0, 0, 0, 0
                    ])
                else:
                    schedule_list[csv_index[csv_key]['id']][4] = csv_index[csv_key]['count'] * self.WORK_COUNT_FACTOR
        return schedule_list, employee_ids

    def center_matrix(self, source_matrix):
        """ Take a 2D matrix and return the centered matrix and the mean values
        (A centered matrix is one which has been adjusted by the means)

        :source_matrix: the matrix to center
        :returns:       the centered matrix
                        a list of the mean values
        """
        row_count = len(source_matrix)
        row_width = len(source_matrix[0])

        # Aready have (Φ), source_matrix
        mean_values = [0 for i in xrange(row_width)]

        # Get the mean row (Ψ), step 1 sum
        for row in source_matrix:
            for index, item in enumerate(row):
                # TODO: overflow prevention?
                mean_values[index] += item

        # Get the mean row (Ψ), step 2 divide
        for i in xrange(len(mean_values)):
            mean_values[i] = mean_values[i] / row_count

        target_matrix = []
        for i in xrange(row_count):
            new_row = []
            for j in xrange(row_width):
                new_row.append(source_matrix[i][j])
            target_matrix.append(new_row)

        # Get the difference from the mean (Φ) = Φ=Γ−Ψ
        for i in xrange(row_count):
            for j in xrange(row_width):
                target_matrix[i][j] = target_matrix[i][j] - mean_values[j]

        return target_matrix, mean_values

    def get_k_limit(self, sigma, klimit_min=1, klimit_max=1024):
        """ Take a list of singular values (eigen values) and return the number of signifigant values

        TODO: rather than looking for the first precipitous drop in values, find all the deltas
                and return the number of eigen values up to the location of the maximum delta

        :sigma:         list of singular values
        :klimit_min:    the minimum number of signifigant values
        :klimit_max:    the maximum number of signifigant values
        :returns:       the number of signifigant singular values
        """
        # if current value times this value is less than the last value then we found the last signifigant value
        DROP_FACTOR = 100

        klimit = klimit_max
        last_eigenvalue = 0
        eigenvalues = numpy.nditer(sigma, flags=['f_index'])
        while not eigenvalues.finished:
            if eigenvalues.index > klimit_min:
                if last_eigenvalue:
                    if (eigenvalues[0] * DROP_FACTOR) < last_eigenvalue:
                        klimit = eigenvalues.index - 1
                        break
            last_eigenvalue = eigenvalues[0]
            eigenvalues.iternext()

        return klimit

    def create_eigenspace(self, source_matrix):
        """ Take a matrix and return the eigenspace and eigen values

        :source_matrix: the original matrix
        :returns:       the eigenspace matrix
                        the eigenvalue list
        """
        A = numpy.asmatrix(source_matrix)
        At = A.transpose()

        # covariance matrix
        C = A * At

        # get eigen vectors
        U, s, Vt = numpy.linalg.svd(C, full_matrices=True)

        # project original matrix into the eigenspace
        eigenspace = numpy.dot(At, U)
        eigenspace = eigenspace.transpose()

        # TODO: FIXUP this area
        #       because of the math.sqrt below, the vector length must be a perfect square
        # normalize the eigenspace
        img_width = int(math.sqrt(len(source_matrix[0])))
        for i in xrange(len(source_matrix)):
            ui = eigenspace[i]
            ui.shape = (img_width, img_width)
            norm = numpy.trace(numpy.dot(ui.transpose(), ui))
            eigenspace[i] = eigenspace[i] / norm

        klimit = self.get_k_limit(s)
        eigenspace = eigenspace[:klimit]
        # s_len = s.shape[0]
        # for k in xrange(0, s_len):
        #   if k >= klimit:
        #       s[k] = 0
        s = s[:klimit]

        return eigenspace, s

    def generate_row_weights(self, k_limit, eigen_space, row):
        """ Take a list of know values and calculate the projected weights

        :k_limit:       the number of eigen values used
        :eigen_space:   the target eigenspace
        :row:           the source row
        :returns:       a list of the row's wieghts
        """
        test_array = numpy.array(row)
        weights = []
        for x in xrange(0, k_limit):
            eigen_vector = eigen_space[x].transpose()
            new_weight = numpy.dot(test_array, eigen_vector)[0, 0]
            weights.append(new_weight)
        return weights

    def generate_weights(self, k_limit, eigen_space, matrix):
        """ Take a matrix of know values and calculate the projected weights

        :k_limit:       the number of eigen values used
        :eigen_space:   the target eigenspace
        :matrix:        the source matrix
        :returns:       a matrix of weights
        """
        weights = []
        for row in matrix:
            new_weight = self.generate_row_weights(k_limit, eigen_space, row)
            weights.append(new_weight)
        return weights

    def perform_search(self, search_schedule, result_count, csv_list, employee_ids, eigen_space, k_limit, weights):
        """ Take an eigenspace, the pre-calculated example weights, a search query and return search results

        :search_schedule:   the search query
        :result_count:      limit results to this (may be less than or equal to this)
        :csv_list:          the original historical examples
        :employee_ids:      employee ids for the original historical examples
        :eigen_space:       the eigenspace
        :k_limit:           the number of eigen values
        :weights:           the known cases projected onto the eigenspace
        :returns:           the search results
        """
        test_array = numpy.array([
            search_schedule.work_day,
            search_schedule.work_shift,
            search_schedule.work_type,
            1,  # worked = true
            1,  # worked count = 100
            0, 0, 0, 0
        ])
        search_weights = []
        for x in xrange(0, k_limit):
            eigen_vector = eigen_space[x].transpose()
            new_weight = numpy.dot(test_array, eigen_vector)[0, 0]
            search_weights.append(new_weight)

        # approach: cosine similarity
        max_score = -999999
        answer = ''
        answer_index = 0
        results = []
        for idx, weight_vector in enumerate(weights):
            numerator = 0
            denominatorA = 0
            denominatorB = 0
            for index in xrange(0, k_limit):
                numerator += search_weights[index] * weight_vector[index]
                denominatorA += search_weights[index] * search_weights[index]
                denominatorB += weight_vector[index] * weight_vector[index]
            if denominatorA and denominatorB:
                total_score = numerator / (math.sqrt(denominatorA) * math.sqrt(denominatorB))
            else:
                total_score = 0

            # print idx, characters[idx], weight_vector[index], total_score, max_score
            if total_score >= (max_score - 0.02):
                max_score = total_score
                answer = employee_ids[idx]
                answer_index = idx
                result = SearchResult(
                    score=total_score,
                    index=(answer_index + 2),
                    employee=answer,
                    work_day=csv_list[answer_index][0],
                    work_shift=csv_list[answer_index][1],
                    work_type=csv_list[answer_index][2],
                    worked=csv_list[answer_index][3],
                    worked_count=csv_list[answer_index][4]
                )
                results.append(result)

        results = sorted(results, key=attrgetter('score'), reverse=True)[:result_count]
        return results
        #
        # TODO: compare results against a simple cartesian distance of the search terms to each of the known results
        #

    def find_in_csv(self, filehandle, search_schedule, result_count):
        """ Take a csv file with historical data and a schedule search
            and calculate the employee most suited for the schedule

        TODO: cache the eigenspace and other data from intermediate steps
                move cached data (or to be cached data) to class namespace

        :filehandle:        the csv containing historical shift data
        :search_schedule:   the schedule to find an ideal employee for
        :result_count:      the maximum number of results to return
        :returns:           the top n matches for the query
        """
        csv_list, employee_ids = self.read_csv(filehandle)
        csv_list_centered, csv_list_means = self.center_matrix(csv_list)
        eigen_space, eigen_values = self.create_eigenspace(csv_list_centered)
        k_limit = len(eigen_values)
        weights = self.generate_weights(k_limit, eigen_space, csv_list)
        results = self.perform_search(
            search_schedule, result_count, csv_list, employee_ids, eigen_space, k_limit, weights)
        return results

    def find_in_csv_and_print(self, filehandle, search_schedule, result_count):
        """ Take a csv file with historical data and a schedule search,
            calculate the employee most suited for the schedule,
            and print the results

        TODO: switch everything from here back to use generators (with yield)

        :filehandle:        the csv containing historical shift data
        :search_schedule:   the schedule to find an ideal employee for
        :result_count:      the maximum number of results to return
        :returns:           none, prints the top n matches for the query
        """
        results = self.find_in_csv(filehandle, search_schedule, result_count)
        for result in results:
            print result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find the best matches for one shift given historical data')
    parser.add_argument("work_day",
                        help='Work day (e.g. 1-7)')
    parser.add_argument("work_shift",
                        help='Work shift (e.g. 1-3')
    parser.add_argument("work_type",
                        help='Code for the type of work')
    parser.add_argument("result_count",
                        help='Limit of matches to return')
    parser.add_argument("historical_data_file",
                        nargs='?',
                        type=argparse.FileType('r'),
                        default=sys.stdin,
                        help="CSV file with historical data (or stdin)")
    args = parser.parse_args()
    search_schedule = Schedule(
        work_day=int(args.work_day),
        work_shift=int(args.work_shift),
        work_type=int(args.work_type),
        worked=1,
        employee_id=0
    )
    LsiSearch().find_in_csv_and_print(args.historical_data_file, search_schedule, int(args.result_count))
