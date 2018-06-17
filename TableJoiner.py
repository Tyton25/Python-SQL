#!/usr/bin/python
import os
from string import ascii_lowercase
from itertools import chain
import copy

TABLE_COLUMN_JOIN = {"TABLE_1": ["COLUMN_1A",
                                 "COLUMN_1B",
                                 "COLUMN_1C"],
                     "TABLE_2": ["COLUMN_2A",
                                 "COLUMN_2B"],
                     "TABLE_3": "COLUMN_3",
                     "TABLE_4": ["COLUMN_4A",
                                 "COLUMN_4B"]}

TABLE_COLUMN_MAP = {
                    "TABLE_1": {"COLUMN_NAME_1A": "column_name_1a",
                                "COLUMN_NAME_1B": "column_name_1b",
                                "COLUMN_NAME_1C": "column_name_1c"},
                    "TABLE_2": {"COLUMN_NAME_2A": "column_name_2a",
                                "COLUMN_NAME_2B": "column_name_2b",
                                "COLUMN_NAME_2C": "column_name_2c"},
                    "TABLE_3": {"COLUMN_NAME_3A": "column_name_3a",
                                "COLUMN_NAME_3B": "column_name_3b"},
                    "TABLE_4": {"COLUMN_NAME_4": "column_name_4"},
                    "TABLE_5": {"COLUMN_NAME_5": "column_name_5"},
                    "TABLE_6": {"COLUMN_NAME_6": "column_name_6"},
                    "TABLE_7": {"COLUMN_NAME_7": "column_name_7"}
}

TABLE_JOIN_MAP = {"TABLE_1": "TABLE_2",
                  "TABLE_2": "TABLE_3",
                  "TABLE_3": "TABLE_4",
                  "TABLE_4": "TABLE_5"
                  }

DATE_CONVERSION_LIST = ['column_name_1', 'column_name_2', 'column_name_3', 'column_name_4']


class TableJoiner:

    #def main(self):

    @staticmethod
    def make_alphas():
        alphas = []
        for c in chain(ascii_lowercase, '_'):
            if c != '_':
                alphas.append(c)
        else:
            for d in chain(ascii_lowercase):
                alphas.append(d + d)
        return alphas

    def get_compare_table(self, compare_value, table_column_map, table_names):
        for table, value in table_column_map.items():
            if table == compare_value:
                for col, name in value.items():
                    if name == 'column_name':
                        compare_table = table
                        compare_col = col
        for table, letter in table_names.itemns():
            if table == compare_table:
                letter_column = letter + '.' + compare_col
        return letter_column

    def inner_join_builder(self, table_list, table_join_map, table_keys_dict, table_lett_column):
        count = 1
        nums = []
        join_statement = []

        for i in range(len(table_list)):
            nums.append(0)

        table_use_count = dict(zip(table_list, nums))

        for table in table_join_map:
            join_vals = self.get_join_data(table[0], table_lett_column)
            join_table = join_vals[0]
            join_letter = join_vals[1]
            join_columns = join_vals[2]

            for table2, val in table_use_count.items():
                if table2 == table:
                    join_table_index = val

            join_column_temp = ''.join(map(str, join_columns[join_table_index]))
            join_column = ''.join(map(str, join_column_temp))

            if count == 0:
                join_statement.append('"FROM {} AS {}'.format(join_table, join_letter))

            self.table_reference_counter(join_table, table_keys_dict, table_use_count)

            on_vals = self.get_join_data(table[1], table_lett_column)
            join_on_table = on_vals[0]
            join_on_letter = on_vals[1]
            join_on_columns = on_vals[2]

            for table2, val in table_use_count.items():
                if table2 == table:
                    on_table_index = val

            join_on_column_temp = ''.join(map(str, join_columns[on_table_index]))
            join_on_column = ''.join(map(str, join_on_column_temp))

            join_statement.append("INNER JOIN " + join_on_table + " AS " + join_on_letter + " ON " + \
                                  join_letter + "." + join_column + "=" + join_on_letter + "." + join_on_column)

            self.table_reference_counter(join_table, table_keys_dict, table_use_count)

            count += 1

            return join_statement

    def table_reference_counter(self, compare_table, table_keys, table_ref_count):
        for table, val in table_ref_count.items():
            for table2, cols in table_keys.items():
                if table == table2:
                    if len(cols) == 1 or len(cols) == val + 1:
                        pass
                    else:
                        table_ref_count[val] += 1
                    return table_ref_count

    def column_header_builder(self, table_dict, table_column_map, date_convert_list):
        header = []
        column_letters = []
        for table, letter in table_dict.items():
            for table2, value in table_column_map.items():
                if table == table2:
                    for column, name in value.items():
                        column_letters.append(letter + '.' + column + " AS " + name)
                        header.append(name)
        return header, column_letters

    def table_column_letter_mapper(self, table_keys_dict, table_join_map):
        table_letter_columns = []
        for table, letter in table_keys_dict.items():
            for table2, value in table_join_map.items():
                if table == table2:
                    for col, name in value.items():
                        table_letter_columns.append(table, letter, col)
        return table_letter_columns

    def get_join_data(self, compare_table, table_letter_column):
        for table in table_letter_column:
            temp_table = ''.join(map(str, table[0]))
            if temp_table == compare_table:
                return table

    def make_table_list(self, table_column_map):
        my_alphas = self.make_alphas()
        table_list = table_column_map.keys()
        table_list = sorted(table_list)
        table_letter_dict = dict(zip(table_list, my_alphas))
        return table_letter_dict

    def time_boundary_formatter(self, table_column_map, table_letter_column, query_start, query_stop):
        for table, value in table_column_map.items():
            for col, name in value.items():
                if name == 'string':
                    temp_table = table
                    temp_col = col
            for tab in table_letter_column:
                if tab == table:
                    time_table = tab[1] + '.' + col
                    break
        time_boundary_clause = "{}::timestamp WITH TIMEZONE AT TIMEZONE 'GMT' BETWEEN '{}' AND '{}'".format(time_table, query_start, query_stop)
        return time_boundary_clause


    startTime = '2018/04/25 00:01:00'
    stopTime = '2018/04/25 00:11:00'
    myAlphas = make_alphas()
    tableNames = make_table_list(TABLE_COLUMN_MAP)
    tableColumnMap = copy.deepcopy(TABLE_COLUMN_MAP)
    tableLetterColumn = table_column_letter_mapper(tableNames, TABLE_JOIN_MAP)
    timeBoundClause = time_boundary_formatter(TABLE_COLUMN_MAP, DATE_CONVERSION_LIST, startTime, stopTime)

if 'name' == '__main__':
    main()
