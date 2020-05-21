import datetime
import random
import sys
from typing import Dict

import psycopg2
from psycopg2 import sql

import postgres
import utils

DEFAULT_NUMBER_OF_ROWS = 100
RANDOM_WORD_LENGTH = 15

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
            sys.exit(f'Could not connect to the "{args.DBNAMEIN}" database.')

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

            primary_columns = postgres.get_table_primary_keys(cursor, table_name)

            column_results = postgres.get_column_information(cursor, table_name)

            self.table_information[table_name] = {}
            self.table_information[table_name]["column_information"] = {}
            self.table_information[table_name]["pg_stats"] = {}

            self.fill_columns_dict(table_name, column_results, primary_columns)

            table_stats = postgres.get_table_stats(cursor, table_name)
            self.fill_stats_dict(table_name, table_stats)

            column_names = list()

            for column_name, column_info in self.table_information.get(table_name)["column_information"].items():
                if not column_info.get("column_default"):
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
                    f'An error occurred while inserting data into the "{table_name}" table. Error description: {db_error}.\n')
                connection.rollback()

        sys.stdout.write(f'Successfully generated the synthetic data into the "{args.DBNAMEGEN}" database.')

    def fill_columns_dict(self, table_name, column_results, primary_columns):
        for column_entry in column_results:
            if not primary_columns or column_entry[0] not in primary_columns:
                columns_dict = dict()
                columns_dict["column_name"] = column_entry[0]
                columns_dict["data_type"] = column_entry[1]
                columns_dict["max_length"] = column_entry[2]
                if column_entry[3]:
                    columns_dict["column_default"] = column_entry[3]
                if column_entry[4]:
                    columns_dict["numeric_precision"] = column_entry[4]
                if column_entry[5]:
                    columns_dict["numeric_precision_radix"] = column_entry[5]
                if column_entry[6]:
                    columns_dict["numeric_scale"] = column_entry[6]

                self.table_information[table_name]["column_information"][column_entry[0]] = columns_dict

    def fill_stats_dict(self, table_name, table_stats):
        for stats_entry in table_stats:
            stats_dict = dict()
            stats_dict["column_name"] = stats_entry[0]
            stats_dict["null_frac"] = stats_entry[1]
            stats_dict["avg_width"] = stats_entry[2]
            stats_dict["n_distinct"] = stats_entry[3]

            most_common_values = stats_entry[4]
            if most_common_values is not None:
                most_common_values = most_common_values.strip("{}").split(",")
                most_common_values = [value.strip('"').replace("'", "''") for value in most_common_values]
                most_common_values = [value for value in most_common_values if value.strip()]

            stats_dict["most_common_vals"] = most_common_values
            stats_dict["most_common_freqs"] = stats_entry[5]

            histogram_bounds = stats_entry[6]
            if histogram_bounds is not None:
                histogram_bounds = histogram_bounds.strip("{}").split(",")
                histogram_bounds = [bound.strip('"').replace("'", "''") for bound in histogram_bounds]
                histogram_bounds = [bound for bound in histogram_bounds if bound.strip()]

            stats_dict["histogram_bounds"] = histogram_bounds
            stats_dict["correlation"] = stats_entry[7]
            self.table_information[table_name]["pg_stats"][stats_entry[0]] = stats_dict

    def create_insert_query(self, cursor, multiplication_factor, table_name, column_names, insert_dict):
        insert_query = ""
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        number_of_rows = cursor.fetchone()[0]
        if number_of_rows is None:
            number_of_rows = DEFAULT_NUMBER_OF_ROWS

        column_information = Dict = {}
        for column_info in self.table_information[table_name]["column_information"].values():
            column_name = column_info.get("column_name")
            data_type = column_info.get("data_type")
            max_length = column_info.get("max_length")
            numeric_precision = column_info.get("numeric_precision")
            numeric_precision_radix = column_info.get("numeric_precision_radix")
            numeric_scale = column_info.get("numeric_scale")

            column_information[column_name] = {}

            if column_name in self.table_information[table_name]["pg_stats"]:
                column_stats = self.table_information[table_name]["pg_stats"][column_name]

                if column_stats:
                    if column_stats["most_common_vals"] and column_stats["most_common_freqs"]:
                        most_common_values = column_stats["most_common_vals"]
                        most_common_freqs = column_stats["most_common_freqs"]

                        if most_common_values and most_common_freqs:
                            generated_vals = list()
                            generated_freqs = list()

                            index = 0
                            if data_type in postgres.DataTypes.NUMERIC_TYPES:
                                for _ in most_common_values:
                                    generated_vals.append(
                                        random_number(numeric_precision, numeric_precision_radix, numeric_scale))
                                    generated_freqs.append(most_common_freqs[index])
                                    index += 1

                            elif data_type in postgres.DataTypes.DATE_TYPES:
                                for _ in most_common_values:
                                    generated_vals.append(utils.random_date(START_DATE, END_DATE))
                                    generated_freqs.append(most_common_freqs[index])
                                    index += 1

                            elif data_type in postgres.DataTypes.VARCHAR_TYPES:
                                average_length = sum(map(len, most_common_values)) / len(most_common_values)
                                for value in most_common_values:
                                    generated_vals.append(random_word(value, average_length, max_length))
                                    generated_freqs.append(most_common_freqs[index])
                                    index += 1

                            column_information[column_name]["generated_vals"] = generated_vals
                            column_information[column_name]["generated_freqs"] = generated_freqs

            if data_type not in postgres.DataTypes.SUPPORTED_TYPES:
                print(
                    f'The "{data_type}" data type is not supported. Skipping the table\'s "{table_name}" data generation...')
                return

        if not column_names:
            print(f'No columns found to generate data into. Skipping the table\'s "{table_name}" data generation...')
            return

        for _ in range(int(number_of_rows * multiplication_factor)):
            column_values = list()
            insert_query += "INSERT INTO {table_name}("
            insert_query += '{0}{1}'.format(', '.join(column_names), ') VALUES (')

            for column_info in self.table_information[table_name]["column_information"].values():
                column_name = column_info.get("column_name")
                data_type = column_info.get("data_type")
                max_length = column_info.get("max_length")
                numeric_precision = column_info.get("numeric_precision")
                numeric_precision_radix = column_info.get("numeric_precision_radix")
                numeric_scale = column_info.get("numeric_scale")

                if column_info.get("column_default"):
                    continue

                # histogram_bounds = None

                generated_vals = None
                generated_freqs = None

                column_stats = None
                if column_name in self.table_information[table_name]["pg_stats"]:
                    column_stats = self.table_information[table_name]["pg_stats"][column_name]

                if column_stats:
                    if "generated_vals" in column_information[column_name] \
                            and "generated_freqs" in column_information[column_name] \
                            and column_information[column_name]["generated_vals"] \
                            and column_information[column_name]["generated_freqs"]:
                        generated_vals = column_information[column_name]["generated_vals"]
                        generated_freqs = column_information[column_name]["generated_freqs"]

                    # if "histogram_bounds" in column_stats:
                    #    histogram_bounds = column_stats["histogram_bounds"]

                if data_type in postgres.DataTypes.NUMERIC_TYPES:
                    if generated_vals and generated_freqs:
                        column_values.append("{0}".format(utils.random_choices(generated_vals, generated_freqs)))
                    # elif histogram_bounds:
                    #    column_values.append("{0}".format(utils.random_choice(histogram_bounds)))
                    else:
                        column_values.append(
                            "{0}".format(random_number(numeric_precision, numeric_precision_radix, numeric_scale)))
                elif data_type in postgres.DataTypes.DATE_TYPES:
                    if generated_vals and generated_freqs:
                        column_values.append(
                            "'{0}'".format(utils.random_choices(generated_vals, generated_freqs)))
                    # elif histogram_bounds:
                    #    column_values.append("'{0}'".format(utils.random_choice(histogram_bounds)))
                    else:
                        column_values.append("'{0}'".format(utils.random_date(START_DATE, END_DATE)))
                elif data_type in postgres.DataTypes.BOOLEAN_TYPES:
                    column_values.append("{0}".format(utils.random_boolean()))
                elif data_type in postgres.DataTypes.VARCHAR_TYPES:
                    if generated_vals and generated_freqs:
                        column_values.append(
                            "'{0}'".format(utils.random_choices(generated_vals, generated_freqs)))
                    # elif histogram_bounds:
                    #    column_values.append("'{0}'".format(utils.random_choice(histogram_bounds)))
                    else:
                        column_values.append("'{0}'".format(random_word(
                            utils.random_word(RANDOM_WORD_LENGTH), max_length / 2.5, max_length
                        )))

            insert_query += '{0}{1}'.format(', '.join(column_values), ');')

        insert_dict[table_name] = insert_query


def random_word(value, average_length, max_length):
    random_length = random.randrange(int(average_length), stop=int(average_length * 2))
    if max_length < random_length:
        random_length = max_length

    if str(value).isupper():
        word = utils.random_word(random_length).upper()
    elif str(value) and str(value)[0].isupper():
        word = utils.random_word(random_length).capitalize()
    else:
        word = utils.random_word(random_length)

    return word


def random_number(numeric_precision, numeric_precision_radix, numeric_scale):
    if numeric_precision:
        if numeric_scale and numeric_scale != 0:
            number = round(utils.random_number(
                0, (numeric_precision_radix ** (numeric_precision - numeric_scale - 1)) / 1.5,
                uniform=True),
                numeric_scale)
        else:
            number = utils.random_number(0, (numeric_precision_radix ** (numeric_precision - 1)) / 1.5)
    else:
        number = utils.random_number(0, 50000)

    return number
