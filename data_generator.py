import datetime
import random
import sys
from typing import Dict

import psycopg2
from psycopg2 import sql

import postgres
import utils

START_DATE = datetime.date(year=1950, month=1, day=1)
END_DATE = datetime.date.today()


class DataGenerator:
    table_information: Dict = {}

    def generate(self, args):
        try:
            connection = psycopg2.connect(dbname=args.DBNAMEGEN,
                                          user=args.user,
                                          host=args.hostname,
                                          port=args.port,
                                          password=args.password)
        except psycopg2.DatabaseError:
            sys.exit(f'Could not connect to the \"{args.DBNAMEGEN}\" database.')

        cursor = connection.cursor()

        postgres.truncate_tables(connection, cursor)

        try:
            connection = psycopg2.connect(dbname=args.DBNAMEIN,
                                          user=args.user,
                                          host=args.hostname,
                                          port=args.port,
                                          password=args.password)
        except psycopg2.DatabaseError:
            sys.exit(f'Could not connect to the \"{args.DBNAMEIN}" database.')

        cursor = connection.cursor()

        print(f'Generating synthetic data into the "{args.DBNAMEGEN}" database...')

        table_results = postgres.get_tables(cursor)

        tables_list = None
        if args.tables is not None:
            tables_list = args.tables.split(",")
            tables_list = [table.strip(' ') for table in tables_list]

        insert_dict = dict()
        for table_entry in table_results:
            table_name = table_entry[1]

            if tables_list:
                if table_name not in tables_list:
                    continue

            primary_column_result = postgres.get_table_primary_key(cursor, table_name)
            primary_column = None

            if primary_column_result:
                primary_column = primary_column_result[0]

            column_results = postgres.get_column_information(cursor, table_name)

            self.table_information[table_name] = {}
            self.table_information[table_name]["column_information"] = {}
            self.table_information[table_name]["pg_stats"] = {}

            self.fill_columns_dict(table_name, column_results, primary_column)

            table_stats = postgres.get_table_stats(cursor, table_name)
            self.fill_stats_dict(table_name, table_stats)

            column_names = list()

            for column_name in self.table_information.get(table_name)["column_information"].keys():
                column_names.append(column_name)

            self.create_insert_query(cursor, args.mf, table_name, column_names, insert_dict)

        connection = psycopg2.connect(dbname=args.DBNAMEGEN,
                                      user=args.user,
                                      host=args.hostname,
                                      port=args.port,
                                      password=args.password)

        cursor = connection.cursor()

        for table_name, insert_query in insert_dict.items():
            try:
                if insert_query:
                    cursor.execute(
                        sql.SQL(insert_query).format(
                            table_name=sql.Identifier(table_name)
                        )
                    )
                    connection.commit()
            except psycopg2.DatabaseError as db_error:
                sys.stdout.write(
                    f"An error occurred while inserting data into the \"{table_name}\" table. Error description: {db_error}.\n")
                connection.rollback()

        sys.stdout.write(f"Successfully generated the synthetic data into the \"{args.DBNAMEGEN}\" database.")

    def fill_columns_dict(self, table_name, column_results, primary_column):
        for column_entry in column_results:
            if column_entry[0] != primary_column:
                columns_dict = dict()
                columns_dict["column_name"] = column_entry[0]
                columns_dict["data_type"] = column_entry[1]
                columns_dict["max_length"] = column_entry[2]
                self.table_information[table_name]["column_information"][column_entry[0]] = columns_dict

    def fill_stats_dict(self, table_name, table_stats):
        for stats_entry in table_stats:
            stats_dict = dict()
            stats_dict["column_name"] = stats_entry[0]
            stats_dict["null_frac"] = stats_entry[1]
            stats_dict["avg_width"] = stats_entry[2]
            stats_dict["n_distinct"] = stats_entry[3]
            stats_dict["most_common_vals"] = stats_entry[4]
            stats_dict["most_common_freqs"] = stats_entry[5]
            stats_dict["histogram_bounds"] = stats_entry[6]
            stats_dict["correlation"] = stats_entry[7]
            self.table_information[table_name]["pg_stats"][stats_entry[0]] = stats_dict

    def create_insert_query(self, cursor, multiplication_factor, table_name, column_names, insert_dict):
        insert_query = ""
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        number_of_rows = cursor.fetchone()[0]
        if number_of_rows is None:
            number_of_rows = 100

        for _ in range(int(number_of_rows * multiplication_factor)):
            column_values = list()
            insert_query += "INSERT INTO {table_name}("
            insert_query += '{0}{1}'.format(', '.join(column_names), ') VALUES (')

            for column_info in self.table_information[table_name]["column_information"].values():
                column_name = column_info.get("column_name")
                data_type = column_info.get("data_type")
                max_length = column_info.get("max_length")

                column_stats = None
                if column_name in self.table_information[table_name]["pg_stats"]:
                    column_stats = self.table_information[table_name]["pg_stats"][column_name]

                most_common_values = None
                histogram_bounds = None
                if column_stats:
                    if column_stats["most_common_vals"]:
                        most_common_values = column_stats["most_common_vals"].strip("{}").split(",")
                        most_common_values = [value.strip('"').replace("'", "''") for value in most_common_values]

                    if column_stats["histogram_bounds"]:
                        histogram_bounds = column_stats["histogram_bounds"].strip("{}").split(",")
                        histogram_bounds = [bound.strip('"').replace("'", "''") for bound in histogram_bounds]

                if data_type in postgres.DataTypes.NUMERIC_TYPES:
                    if most_common_values:
                        column_values.append("{0}".format(utils.random_choice(most_common_values)))
                    elif histogram_bounds:
                        column_values.append("{0}".format(utils.random_choice(histogram_bounds)))
                    else:
                        column_values.append("{0}".format(utils.random_number(1, 1000)))
                elif data_type in postgres.DataTypes.DATE_TYPES:
                    if most_common_values:
                        column_values.append("'{0}'".format(utils.random_choice(most_common_values)))
                    elif histogram_bounds:
                        column_values.append("'{0}'".format(utils.random_choice(histogram_bounds)))
                    else:
                        column_values.append("'{0}'".format(utils.random_date(START_DATE, END_DATE)))
                elif data_type in postgres.DataTypes.BOOLEAN_TYPES:
                    column_values.append("{0}".format(utils.random_boolean()))
                elif data_type in postgres.DataTypes.VARCHAR_TYPES:
                    if most_common_values:
                        column_values.append("'{0}'".format(utils.random_choice(most_common_values)))
                    elif histogram_bounds:
                        column_values.append("'{0}'".format(utils.random_choice(histogram_bounds)))
                    else:
                        random_length = random.randrange(50 if max_length is None else (max_length + 1))
                        column_values.append("'{0}'".format(utils.random_word(random_length)))
                else:
                    sys.stdout.write(f"The \"{data_type}\" data type is not supported.")

            insert_query += '{0}{1}'.format(', '.join(column_values), ');')

        insert_dict[table_name] = insert_query
